/*
========================================================================
Project: OpenRFStack
Author:  Brendan Michaud
Year:    2026
Part of OpenRFStack (https://github.com/OpenRFStack)

Licensed under the Personal Use License.
Do not use for commercial, organizational, or military purposes.
Contact author for permission: https://github.com/OpenRFStack
========================================================================
*/
#include "RadioDevice.hpp"

#include <SoapySDR/Errors.hpp>
#include <SoapySDR/Formats.hpp>
#include <SoapySDR/Types.hpp>
#include <spdlog/spdlog.h>

#include <cmath>
#include <limits>
#include <stdexcept>

namespace sdr
{

RadioDevice::RadioDevice(const DeviceConfig& cfg)
    : cfg_(cfg)
{
}

RadioDevice::~RadioDevice()
{
    close();
}

bool RadioDevice::open()
{
    std::lock_guard lock(mu_);
    if (online_)
        return true;
    try
    {
        SoapySDR::Kwargs args;
        args["driver"] = cfg_.driver;
        // SoapyRemote uses key "remote" for the host:port; all other drivers
        // use "uri" (libiio convention: "ip:192.168.1.x", "usb:X.Y.Z", etc.)
        if (cfg_.driver == "remote")
        {
            args["remote"] = cfg_.uri;
        }
        else if (!cfg_.uri.empty() && cfg_.uri.find('=') != std::string::npos)
        {
            // "serial=0000…,type=b200" / "addr=192.168.10.2": a SoapySDR
            // kwargs string, for drivers that identify a unit by something
            // other than a libiio uri (UHD, rtlsdr, hackrf, lime …). It used
            // to be passed whole as uri=…, which those drivers ignore — every
            // entry of the same driver then opened the first unit (RM-14).
            for (auto& [k, v] : SoapySDR::KwargsFromString(cfg_.uri))
                args[k] = v;
            args["driver"] = cfg_.driver;
        }
        else if (!cfg_.uri.empty())
        {
            args["uri"] = cfg_.uri;
        }
        else
        {
            // No uri configured — ask the driver to find itself instead of us
            // having to hand-maintain a device's address in devices.xml.
            // SoapyPlutoSDR's registered find function (and any other driver's)
            // tries USB scan, then zeroconf, then a PLUTO_IP env var fallback
            // before giving up; Device::make() alone skips all of that and
            // goes straight to iio_create_default_context(), which only finds
            // a USB-attached device.
            auto found = SoapySDR::Device::enumerate(args);
            // enumerate() can return multiple matches of the same driver (e.g.
            // two Plutos) -- pick the first one no other RadioDevice instance
            // has already claimed, so two devices.xml entries with empty uri
            // don't both grab the same physical unit.
            std::lock_guard<std::mutex> claim_lock(claimMutex());
            for (auto& candidate : found)
            {
                std::string key = candidate.count("uri")        ? candidate.at("uri")
                                  : candidate.count("hostname") ? candidate.at("hostname")
                                  : candidate.count("serial")   ? candidate.at("serial")
                                  : candidate.count("label")    ? candidate.at("label")
                                                                : std::string();
                if (key.empty() || claimedUris().count(key))
                    continue;
                args         = candidate;
                claimed_key_ = key;
                claimedUris().insert(key);
                spdlog::info("[{}] Discovered via enumerate(): {}", cfg_.id, key);
                break;
            }
            if (claimed_key_.empty() && !found.empty())
                spdlog::warn("[{}] enumerate() found {} candidate(s) but all are "
                             "already claimed by other configured devices",
                             cfg_.id,
                             found.size());
        }
        spdlog::info("[{}] Opening: driver={} uri={}",
                     cfg_.id,
                     cfg_.driver,
                     args.count("uri") ? args.at("uri") : cfg_.uri);
        dev_ = SoapySDR::Device::make(args);
        if (!dev_)
        {
            spdlog::error("[{}] make() returned nullptr", cfg_.id);
            return false;
        }

        // Clamp configured channel count to what the hardware actually exports.
        // Prevents accepting tasks that request more channels than exist, which
        // would crash the worker thread when readStream fails on the invalid channel.
        int hw_rx = (int)dev_->getNumChannels(SOAPY_SDR_RX);
        int hw_tx = (int)dev_->getNumChannels(SOAPY_SDR_TX);
        if (cfg_.caps.rx_channels > hw_rx)
        {
            spdlog::warn("[{}] config rx_channels={} > hw={}, clamping",
                         cfg_.id,
                         cfg_.caps.rx_channels,
                         hw_rx);
            cfg_.caps.rx_channels = hw_rx;
        }
        if (cfg_.caps.tx_channels > hw_tx)
        {
            spdlog::warn("[{}] config tx_channels={} > hw={}, clamping",
                         cfg_.id,
                         cfg_.caps.tx_channels,
                         hw_tx);
            cfg_.caps.tx_channels = hw_tx;
        }

        spdlog::info("[{}] OK hw={} drv={} rx_ch={} tx_ch={}",
                     cfg_.id,
                     dev_->getHardwareKey(),
                     dev_->getDriverKey(),
                     cfg_.caps.rx_channels,
                     cfg_.caps.tx_channels);
        online_.store(true, std::memory_order_release);
        return true;
    }
    catch (const std::exception& ex)
    {
        spdlog::error("[{}] open() exception: {}", cfg_.id, ex.what());
        dev_ = nullptr;
        return false;
    }
}

void RadioDevice::close()
{
    std::lock_guard lock(mu_);
    if (!online_)
        return;
    if (dev_)
    {
        SoapySDR::Device::unmake(dev_);
        dev_ = nullptr;
    }
    online_.store(false, std::memory_order_release);
    // A re-made device starts at the driver's defaults: forget what we set,
    // or tune() skips setSampleRate() after a reopen and the RM advertises a
    // rate the hardware is not running at (RM-70).
    current_cf_   = 0.0;
    current_rate_ = 0.0;
    ch_rate_.clear();
    open_streams_ = 0;
    if (!claimed_key_.empty())
    {
        std::lock_guard<std::mutex> claim_lock(claimMutex());
        claimedUris().erase(claimed_key_);
        claimed_key_.clear();
    }
    spdlog::info("[{}] Closed", cfg_.id);
}

std::mutex& RadioDevice::claimMutex()
{
    static std::mutex m;
    return m;
}

std::set<std::string>& RadioDevice::claimedUris()
{
    static std::set<std::string> s;
    return s;
}

bool RadioDevice::tune(double cf_hz, double sr_sps)
{
    std::lock_guard lock(mu_);
    if (!online_ || !dev_)
        return false;
    try
    {
        // If fixed_sample_rate_hz is configured, use it unconditionally and
        // only call setSampleRate once (on first tune).  This avoids the 3-4 s
        // AD9361 BB-filter recalibration that fires on every rate change.
        const double target_rate =
            (cfg_.fixed_sample_rate_hz > 0.0) ? cfg_.fixed_sample_rate_hz : sr_sps;

        if (std::abs(target_rate - current_rate_.load()) > 1.0)
        {
            dev_->setSampleRate(SOAPY_SDR_RX, 0, target_rate);
            // The TX rate follows (rare: rate changes only). AD9361-class
            // parts share one baseband clock chain between RX and TX.
            if (cfg_.caps.tx_channels > 0)
                dev_->setSampleRate(SOAPY_SDR_TX, 0, target_rate);
            current_rate_ = target_rate;
            spdlog::info("[{}] Sample rate set to {:.3f} MSPS", cfg_.id, target_rate / 1e6);
        }

        // Pure LO hop — single IIO write, settles in ~25 µs on AD9361.
        // RX only (RM-13): TX is never streamed (tx_count > 0 is rejected,
        // RM-58), so moving the TX PLL with every hop cost a second PLL write
        // (an extra RPC on SoapyRemote) per scan entry and parked the idle TX
        // LO on the RX frequency, where its leakage lands in the RX band.
        dev_->setFrequency(SOAPY_SDR_RX, 0, cf_hz);
        current_cf_ = cf_hz;
        // debug: a scan logs this once per entry (hundreds per second).
        spdlog::debug(
            "[{}] Tuned {:.3f} MHz {:.3f} MSPS", cfg_.id, cf_hz / 1e6, current_rate_.load() / 1e6);
        return true;
    }
    catch (const std::exception& ex)
    {
        spdlog::error("[{}] tune() failed: {}", cfg_.id, ex.what());
        return false;
    }
}

bool RadioDevice::tuneChannel(int ch, double cf_hz, double sr_sps)
{
    std::lock_guard lock(mu_);
    if (!online_ || !dev_)
        return false;
    try
    {
        size_t c = static_cast<size_t>(ch);
        dev_->setFrequency(SOAPY_SDR_RX, c, cf_hz);
        // Only when it changes (RM-73): this runs on every scan hop.
        auto it = ch_rate_.find(ch);
        if (it == ch_rate_.end() || std::abs(it->second - sr_sps) > 1.0)
        {
            dev_->setSampleRate(SOAPY_SDR_RX, c, sr_sps);
            ch_rate_[ch] = sr_sps;
        }
        spdlog::debug(
            "[{}] ch{} tuned {:.3f}MHz {:.3f}MSPS", cfg_.id, ch, cf_hz / 1e6, sr_sps / 1e6);
        return true;
    }
    catch (const std::exception& ex)
    {
        spdlog::error("[{}] tuneChannel({}) failed: {}", cfg_.id, ch, ex.what());
        return false;
    }
}

bool RadioDevice::setRxGain(int ch, double gain_db, bool agc)
{
    std::lock_guard lock(mu_);
    if (!online_ || !dev_)
        return false;
    try
    {
        size_t c = static_cast<size_t>(ch);
        dev_->setGainMode(SOAPY_SDR_RX, c, agc);
        if (!agc)
            dev_->setGain(SOAPY_SDR_RX, c, gain_db);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

bool RadioDevice::setTxAtten(int ch, double atten_db)
{
    std::lock_guard lock(mu_);
    if (!online_ || !dev_)
        return false;
    try
    {
        dev_->setGain(SOAPY_SDR_TX, static_cast<size_t>(ch), -atten_db);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

bool RadioDevice::reopen()
{
    // mu_ must NOT be held by the caller — open()/close() acquire it internally.
    spdlog::warn("[{}] reopen: closing stale device connection", cfg_.id);
    close();
    if (!open())
    {
        spdlog::error("[{}] reopen: failed to reopen device", cfg_.id);
        return false;
    }
    spdlog::info("[{}] reopen: device reconnected", cfg_.id);
    return true;
}

SoapySDR::Stream* RadioDevice::openRxStream(const std::vector<int>& channels)
{
    {
        std::lock_guard lock(mu_);
        if (!online_ || !dev_)
            return nullptr;
        try
        {
            std::vector<size_t> chans;
            for (int c : channels)
                chans.push_back(static_cast<size_t>(c));
            SoapySDR::Stream* s          = dev_->setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, chans);
            consecutive_stream_failures_ = 0;
            if (s)
                ++open_streams_;
            return s;
        }
        catch (const std::exception& ex)
        {
            spdlog::error("[{}] openRxStream: {}", cfg_.id, ex.what());
        }
    }
    // On failure: if the driver is network-backed (PlutoSDR/IIO) a stale or
    // half-open TCP connection to iiod causes the NEXT connect() to block for
    // the OS TCP connect timeout (up to 2 min), not the 5 s iio_context timeout.
    // Reopen the device after consecutive failures to get a fresh connection.
    int live_streams = 0, fails = 0;
    {
        std::lock_guard lock(mu_);
        fails        = ++consecutive_stream_failures_;
        live_streams = open_streams_;
    }
    if (fails >= 2 && live_streams > 0)
    {
        // Other tasks are streaming from this device: the connection works,
        // and unmaking it would free the device under their worker threads.
        spdlog::warn(
            "[{}] openRxStream: {} consecutive failures, not reopening — {} stream(s) live",
            cfg_.id,
            fails,
            live_streams);
    }
    else if (fails >= 2)
    {
        spdlog::warn(
            "[{}] openRxStream: {} consecutive failures — reopening device", cfg_.id, fails);
        {
            std::lock_guard lock(mu_);
            consecutive_stream_failures_ = 0;
        }
        reopen();
    }
    return nullptr;
}

bool RadioDevice::activateStream(SoapySDR::Stream* s)
{
    std::lock_guard lock(mu_);
    if (!s || !dev_)
        return false;
    try
    {
        return dev_->activateStream(s) == 0;
    }
    catch (...)
    {
        return false;
    }
}

void RadioDevice::deactivateStream(SoapySDR::Stream* s)
{
    std::lock_guard lock(mu_);
    if (!s || !dev_)
        return;
    try
    {
        dev_->deactivateStream(s);
    }
    catch (...)
    {
    }
}

void RadioDevice::closeStream(SoapySDR::Stream* s)
{
    std::lock_guard lock(mu_);
    if (!s || !dev_)
        return;
    try
    {
        dev_->closeStream(s);
    }
    catch (...)
    {
    }
    if (open_streams_ > 0)
        --open_streams_;
}

int RadioDevice::readStream(SoapySDR::Stream* s,
                            void**            buffs,
                            size_t            numElems,
                            int&              flags,
                            long long&        timeNs,
                            long              timeoutUs)
{
    if (!s || !dev_)
        return SOAPY_SDR_NOT_SUPPORTED;
    // NOTE: readStream must NOT hold mu_ — it blocks for up to timeoutUs.
    // dev_ only changes in reopen(), which openRxStream() no longer triggers
    // while any stream is open.
    return dev_->readStream(s, buffs, numElems, flags, timeNs, timeoutUs);
}

double RadioDevice::getTemperature() const
{
    std::lock_guard lock(mu_);
    if (!online_ || !dev_)
        return std::numeric_limits<double>::quiet_NaN();
    try
    {
        return std::stod(dev_->readSensor("temp0"));
    }
    catch (...)
    {
        return std::numeric_limits<double>::quiet_NaN();
    }
}

void RadioDevice::refreshSensors()
{
    auto            temps = listTemperatures();  // driver I/O, outside sensor_mu_
    const double    t0    = getTemperature();
    std::lock_guard lk(sensor_mu_);
    cached_temps_ = std::move(temps);
    cached_temp0_ = t0;
}

std::vector<RadioDevice::TempSensor> RadioDevice::cachedTemperatures() const
{
    std::lock_guard lk(sensor_mu_);
    return cached_temps_;
}

double RadioDevice::cachedTemperature() const
{
    std::lock_guard lk(sensor_mu_);
    return cached_temp0_;
}

std::vector<RadioDevice::TempSensor> RadioDevice::listTemperatures() const
{
    std::lock_guard lock(mu_);
    if (!online_ || !dev_)
        return {};
    std::vector<TempSensor> out;
    try
    {
        for (auto& name : dev_->listSensors())
        {
            if (name.find("temp") == std::string::npos && name.find("Temp") == std::string::npos)
                continue;
            double val = std::numeric_limits<double>::quiet_NaN();
            try
            {
                val = std::stod(dev_->readSensor(name));
            }
            catch (...)
            {
            }
            out.push_back({name, val});
        }
    }
    catch (...)
    {
        // listSensors() not supported — fall back to well-known "temp0"
        double val = std::numeric_limits<double>::quiet_NaN();
        try
        {
            val = std::stod(dev_->readSensor("temp0"));
        }
        catch (...)
        {
        }
        if (!std::isnan(val))
            out.push_back({"temp0", val});
    }
    return out;
}

double RadioDevice::currentCF() const
{
    return current_cf_.load();
}

double RadioDevice::currentRate() const
{
    return current_rate_.load();
}

RadioDevice::Status RadioDevice::getStatus() const
{
    Status s;
    s.device_id       = cfg_.id;
    s.driver          = cfg_.driver;
    s.uri             = cfg_.uri;
    s.coherency_group = cfg_.coherency_group;
    s.online          = online_.load(std::memory_order_acquire);
    s.center_freq_hz  = current_cf_.load();
    s.sample_rate     = current_rate_.load();
    s.temperature_c   = cachedTemperature();  // was a live readSensor() (RM-17)
    return s;
}

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
