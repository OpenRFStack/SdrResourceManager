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
#pragma once
#include "FakeSoapyControl.hpp"

#include <SoapySDR/Constants.h>
#include <SoapySDR/Device.hpp>
#include <SoapySDR/Errors.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// In-process SoapySDR device for unit tests.
// Registered under driver="fake" — no .so required.
// Tests control behaviour via FakeSoapy:: atomics.
class FakeSoapyDevice : public SoapySDR::Device
{
public:
    explicit FakeSoapyDevice(const SoapySDR::Kwargs& = {})
    {
    }

    std::string getDriverKey() const override
    {
        return "fake";
    }

    std::string getHardwareKey() const override
    {
        return "FakeSoapyDevice";
    }

    size_t getNumChannels(const int) const override
    {
        return 2;
    }

    // ── Frequency ────────────────────────────────────────────────────────
    void
    setFrequency(const int dir, const size_t ch, const double f, const SoapySDR::Kwargs&) override
    {
        cf_[dir][ch] = f;
        if (dir == SOAPY_SDR_TX)
            FakeSoapy::tx_freq_set_count.fetch_add(1);
    }

    double getFrequency(const int dir, const size_t ch, const std::string&) const override
    {
        return cf_[dir][ch];
    }

    std::vector<std::string> listFrequencies(const int, const size_t) const override
    {
        return {"RF"};
    }

    // ── Sample rate ───────────────────────────────────────────────────────
    void setSampleRate(const int dir, const size_t ch, const double r) override
    {
        rate_[dir][ch] = r;
        if (dir == SOAPY_SDR_RX)
            FakeSoapy::last_rx_rate.store(r);
    }

    double getSampleRate(const int dir, const size_t ch) const override
    {
        return rate_[dir][ch];
    }

    // ── Gain ──────────────────────────────────────────────────────────────
    void setGainMode(const int, const size_t, const bool) override
    {
    }

    bool getGainMode(const int, const size_t) const override
    {
        return false;
    }

    void setGain(const int, const size_t, const double g) override
    {
        gain_ = g;
    }

    double getGain(const int, const size_t) const override
    {
        return gain_;
    }

    // ── Stream ────────────────────────────────────────────────────────────
    SoapySDR::Stream* setupStream(const int,
                                  const std::string&,
                                  const std::vector<size_t>& channels,
                                  const SoapySDR::Kwargs&) override
    {
        const int call = FakeSoapy::setup_stream_calls.fetch_add(1);
        if (FakeSoapy::fail_setup_stream_count.load() > 0
            && FakeSoapy::fail_setup_stream_count.fetch_sub(1) > 0)
            throw std::runtime_error("FakeSoapy: forced setupStream failure (count)");
        if (call == FakeSoapy::fail_setup_stream_at.load())
            throw std::runtime_error("FakeSoapy: forced setupStream failure");
        // Per-stream state, like a real driver: a device can carry several
        // streams with different channel counts at once.
        auto st   = std::make_unique<StreamState>();
        st->n_ch  = channels.empty() ? 1 : channels.size();
        auto* raw = st.get();
        {  // the device owns its streams (freed with it, like a real driver)
            std::lock_guard lk(streams_mu_);
            streams_.push_back(std::move(st));
        }
        FakeSoapy::open_streams.fetch_add(1);
        return reinterpret_cast<SoapySDR::Stream*>(raw);
    }

    int activateStream(SoapySDR::Stream* s, const int, const long long, const size_t) override
    {
        int d = FakeSoapy::activate_delay_us.load();
        if (d > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(d));
        reinterpret_cast<StreamState*>(s)->samples_since_activate = 0;
        return 0;
    }

    int deactivateStream(SoapySDR::Stream*, const int, const long long) override
    {
        return 0;
    }

    void closeStream(SoapySDR::Stream* s) override
    {
        std::lock_guard lk(streams_mu_);
        auto*           st = reinterpret_cast<StreamState*>(s);
        for (auto it = streams_.begin(); it != streams_.end(); ++it)
            if (it->get() == st)
            {
                streams_.erase(it);
                break;
            }
        FakeSoapy::open_streams.fetch_sub(1);
    }

    size_t getStreamMTU(SoapySDR::Stream*) const override
    {
        return 1024;
    }

    int readStream(SoapySDR::Stream* stream,
                   void* const*      buffs,
                   const size_t      numElems,
                   int&              flags,
                   long long&        timeNs,
                   const long) override
    {
        auto* st = reinterpret_cast<StreamState*>(stream);
        // Concurrency probe: two threads inside readStream on the same
        // device is exactly what real drivers do not support.
        int now_in = in_read_.fetch_add(1) + 1;
        int prev   = FakeSoapy::max_concurrent_reads.load();
        while (now_in > prev
               && !FakeSoapy::max_concurrent_reads.compare_exchange_weak(prev, now_in))
        {
        }

        struct Leave
        {
            std::atomic<int>& c;

            ~Leave()
            {
                c.fetch_sub(1);
            }
        } leave{in_read_};

        int delay = FakeSoapy::read_delay_us.load();
        if (delay > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(delay));
        if (FakeSoapy::timeout_count.fetch_sub(1) > 0)
            return SOAPY_SDR_TIMEOUT;
        if (FakeSoapy::overflow_next.exchange(false))
            return SOAPY_SDR_OVERFLOW;
        if (FakeSoapy::fail_read.load())
            return SOAPY_SDR_NOT_SUPPORTED;

        size_t n = std::min(numElems, static_cast<size_t>(FakeSoapy::samples_per_read.load()));
        const double r0     = rate_[SOAPY_SDR_RX][0].load();
        const double fs     = r0 > 0.0 ? r0 : 1e6;
        const double tone   = FakeSoapy::tone_hz.load();
        const float  amp    = FakeSoapy::tone_amplitude.load();
        const float  v      = FakeSoapy::sample_value.load();
        const float  step   = FakeSoapy::channel_step.load();
        const double dph    = 2.0 * M_PI * tone / fs;
        const int    lag    = FakeSoapy::pipeline_reads.load();
        float        lo_tag = 0.f;
        if (lag > 0)
        {
            st->lo_hist.push_back(cf_[SOAPY_SDR_RX][0].load());
            while ((int)st->lo_hist.size() > lag + 1)
                st->lo_hist.erase(st->lo_hist.begin());
            lo_tag = (float)(st->lo_hist.front() / 1e9);
        }
        double ph = st->tone_phase;
        for (size_t c = 0; c < st->n_ch; ++c)
        {
            float* buf = static_cast<float*>(buffs[c]);
            if (!buf)
                continue;
            ph = st->tone_phase;
            for (size_t i = 0; i < n; ++i)
            {
                float re = v + (float)c * step + lo_tag, im = re;
                if (tone != 0.0)
                {
                    re += amp * (float)std::cos(ph);
                    im += amp * (float)std::sin(ph);
                    ph += dph;
                }
                buf[2 * i]     = re;
                buf[2 * i + 1] = im;
            }
        }
        st->tone_phase = std::remainder(ph, 2.0 * M_PI);

        flags  = 0;
        timeNs = 0;
        if (FakeSoapy::hw_time.load())
        {
            flags |= SOAPY_SDR_HAS_TIME;
            timeNs = (long long)std::llround(st->samples_since_activate * 1e9 / fs);
        }
        st->samples_since_activate += (long long)n;
        return static_cast<int>(n);
    }

    // ── Sensor ────────────────────────────────────────────────────────────
    std::string readSensor(const std::string&) const override
    {
        return std::to_string(FakeSoapy::reported_temp.load());
    }

    std::vector<std::string> listSensors() const override
    {
        return {"temp0"};
    }

private:
    // Atomic: the controller retunes (scan / combined window) from one thread
    // while an IQStreamer is inside readStream() on another, as real drivers allow.
    std::atomic<double> cf_[2][4]   = {};
    std::atomic<double> rate_[2][4] = {};
    std::atomic<double> gain_{30.0};

    struct StreamState
    {  // what SoapySDR::Stream* points to
        size_t              n_ch                   = 1;
        double              tone_phase             = 0.0;
        long long           samples_since_activate = 0;
        std::vector<double> lo_hist;  // pipeline_reads model
    };

    std::mutex                                streams_mu_;
    std::vector<std::unique_ptr<StreamState>> streams_;
    std::atomic<int>                          in_read_{0};
};

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
