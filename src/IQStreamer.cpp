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
#include "IQStreamer.hpp"

#include <SoapySDR/Constants.h>
#include <SoapySDR/Errors.hpp>
#include <SoapySDR/Formats.hpp>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <stdexcept>

namespace sdr
{
using namespace std::chrono;

namespace
{
constexpr int kMaxReadChunk  = 65'536;  // samples per readStream() call (upper bound)
constexpr int kMaxUdpSamples = (65'507 - (int)IQ_PACKET_HEADER_SIZE) / 8;

inline int64_t nsBetween(IQStreamer::Clock::time_point from, IQStreamer::Clock::time_point to)
{
    return duration_cast<nanoseconds>(to - from).count();
}
}  // namespace

IQStreamer::IQStreamer(const Config&     cfg,
                       SoapySDR::Device* dev,
                       SoapySDR::Stream* stream,
                       TaskErrorCb       on_error)
    : cfg_(cfg)
    , dev_(dev)
    , stream_(stream)
    , on_error_(std::move(on_error))
{
    if (cfg_.packet_samples < 1)
        cfg_.packet_samples = 1;
    if (cfg_.packet_samples > kMaxUdpSamples)
    {
        spdlog::warn("IQStreamer [{}] packet_samples={} exceeds the UDP datagram limit; using {}",
                     cfg_.stream_id,
                     cfg_.packet_samples,
                     kMaxUdpSamples);
        cfg_.packet_samples = kMaxUdpSamples;
    }
    metrics_.stream_id     = cfg_.stream_id;
    metrics_.channel_type  = "RX";
    metrics_.channel_index = cfg_.channel_index;
    metrics_.udp_port      = cfg_.dest_port;
    current_cf_.store(0.0);
    current_sr_hz_.store((uint32_t)cfg_.sample_rate);
}

IQStreamer::~IQStreamer()
{
    stop();
    // Consumers added but never started (e.g. failed activation) still own sockets.
    std::lock_guard lock(dests_mu_);
    for (auto& d : dests_)
        if (d.fd >= 0)
            ::close(d.fd);
    for (auto& sb : subbands_)
        if (sb.dest_fd >= 0)
            ::close(sb.dest_fd);
    dests_.clear();
    subbands_.clear();
}

// ── Helpers ──────────────────────────────────────────────────────────────────

int IQStreamer::bufIndexFor(int channel_index) const
{
    if (channel_index == cfg_.channel_index)
        return 0;
    for (size_t i = 0; i < cfg_.extra_channels.size(); ++i)
        if (cfg_.extra_channels[i].channel_index == channel_index)
            return (int)i + 1;
    return -1;
}

int IQStreamer::openConnectedUdp(const std::string& ip, int port, const char* who) const
{
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
    {
        spdlog::error("IQStreamer::{}: socket() failed for {}:{}", who, ip, port);
        return -1;
    }
    int sndbuf = 8 * 1024 * 1024;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    int       eff = 0;
    socklen_t len = sizeof(eff);
    if (::getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &eff, &len) == 0 && eff < 2 * 1024 * 1024)
    {
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true))
            spdlog::warn("IQStreamer: SO_SNDBUF clamped to {} bytes (asked 8 MiB) — raise "
                         "net.core.wmem_max for high-rate streams; drops are counted in "
                         "metrics",
                         eff);
    }
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port   = htons((uint16_t)port);
    if (::inet_pton(AF_INET, ip.c_str(), &dst.sin_addr) != 1
        || ::connect(fd, (sockaddr*)&dst, sizeof(dst)) < 0)
    {
        ::close(fd);
        spdlog::error(
            "IQStreamer::{}: cannot stream to {}:{} (needs a dotted IPv4 address)", who, ip, port);
        return -1;
    }
    return fd;
}

// ── Multi-destination management ─────────────────────────────────────────────

void IQStreamer::addDest(const std::string& task_id,
                         const std::string& stream_id,
                         const std::string& ip,
                         int                port,
                         Clock::time_point  epoch,
                         int                channel_index)
{
    Dest d;
    d.task_id       = task_id;
    d.stream_id     = stream_id;
    d.channel_index = channel_index < 0 ? cfg_.channel_index : channel_index;
    d.buf_index     = bufIndexFor(d.channel_index);
    d.epoch         = epoch;
    if (d.buf_index < 0)
    {
        spdlog::error("IQStreamer [{}] dest {} wants channel {} which this stream does not carry",
                      cfg_.stream_id,
                      task_id,
                      d.channel_index);
        return;
    }
    d.fd = openConnectedUdp(ip, port, "addDest");
    if (d.fd < 0)
        return;
    std::lock_guard lock(dests_mu_);
    dests_.push_back(std::move(d));
    spdlog::debug("IQStreamer [{}] +dest {} → {}:{}", cfg_.stream_id, task_id, ip, port);
}

int IQStreamer::removeDest(const std::string& task_id)
{
    std::lock_guard lock(dests_mu_);
    // All channels of the task — an owner's extra-channel dests used to keep
    // streaming to its released ports while fan-out subscribers kept us alive.
    for (auto it = dests_.begin(); it != dests_.end();)
    {
        if (it->task_id == task_id)
        {
            if (it->fd >= 0)
                ::close(it->fd);
            it = dests_.erase(it);
            spdlog::debug("IQStreamer [{}] -dest {}", cfg_.stream_id, task_id);
        }
        else
            ++it;
    }
    int n = 0;
    for (auto& d : dests_)
        if (d.buf_index == 0)
            ++n;
    return n;
}

int IQStreamer::destCount() const
{
    std::lock_guard lock(dests_mu_);
    int             n = 0;
    for (auto& d : dests_)
        if (d.buf_index == 0)
            ++n;
    return n;
}

// ── DDC sub-band fan-out ──────────────────────────────────────────────────────

void IQStreamer::addSubBand(const std::string& task_id,
                            const std::string& stream_id,
                            int                channel_index,
                            const std::string& ip,
                            int                port,
                            double             cf_hz,
                            double             output_sr_hz,
                            double             wideband_sr_hz,
                            Clock::time_point  epoch)
{
    SubBand sb;
    sb.task_id        = task_id;
    sb.stream_id      = stream_id;
    sb.channel_index  = channel_index;
    sb.buf_index      = bufIndexFor(channel_index);
    sb.center_freq_hz = (uint64_t)std::llround(cf_hz);
    sb.output_sr      = output_sr_hz;
    sb.epoch          = epoch;
    if (sb.buf_index < 0)
    {
        spdlog::error("IQStreamer [{}] sub-band {} wants channel {} which this stream "
                      "does not carry",
                      cfg_.stream_id,
                      task_id,
                      channel_index);
        return;
    }

    sb.dest_fd = openConnectedUdp(ip, port, "addSubBand");
    if (sb.dest_fd < 0)
        return;

    int decim = (output_sr_hz > 0.0) ? (int)std::lround(wideband_sr_hz / output_sr_hz) : 1;
    if (decim < 1)
        decim = 1;
    // Advertise what is actually produced, not what was asked for (audit RM-09).
    sb.sample_rate_hz = (uint32_t)std::llround(wideband_sr_hz / decim);
    if (std::abs(wideband_sr_hz / decim - output_sr_hz) > 1.0)
        spdlog::warn("IQStreamer [{}] sub-band {}: {:.0f} S/s requested, {:.0f}/{} = {} S/s "
                     "delivered (non-integer ratio)",
                     cfg_.stream_id,
                     task_id,
                     output_sr_hz,
                     wideband_sr_hz,
                     decim,
                     sb.sample_rate_hz);
    double offset_hz = cf_hz - current_cf_.load(std::memory_order_relaxed);
    sb.ddc           = std::make_unique<Ddc>(offset_hz, wideband_sr_hz, decim);

    std::lock_guard lock(dests_mu_);
    subbands_.push_back(std::move(sb));
    spdlog::debug("IQStreamer [{}] +subband {} ch={} cf={:.3f}MHz decim={} → {}:{}",
                  cfg_.stream_id,
                  task_id,
                  channel_index,
                  cf_hz / 1e6,
                  decim,
                  ip,
                  port);
}

int IQStreamer::removeSubBand(const std::string& task_id)
{
    std::lock_guard lock(dests_mu_);
    // Every sub-band of the task — a multi-channel DDC consumer has one per
    // channel. Removing only the first kept the stream alive forever (the
    // device was later closed under the running worker: use-after-free).
    for (auto it = subbands_.begin(); it != subbands_.end();)
    {
        if (it->task_id == task_id)
        {
            if (it->dest_fd >= 0)
                ::close(it->dest_fd);
            it = subbands_.erase(it);
            spdlog::debug("IQStreamer [{}] -subband {}", cfg_.stream_id, task_id);
        }
        else
            ++it;
    }
    std::vector<const std::string*> tasks;
    for (auto& d : dests_)
        tasks.push_back(&d.task_id);
    for (auto& sb : subbands_)
        tasks.push_back(&sb.task_id);
    std::sort(tasks.begin(),
              tasks.end(),
              [](auto* a, auto* b)
              {
                  return *a < *b;
              });
    return (int)std::distance(tasks.begin(),
                              std::unique(tasks.begin(),
                                          tasks.end(),
                                          [](auto* a, auto* b)
                                          {
                                              return *a == *b;
                                          }));
}

int IQStreamer::subBandCount() const
{
    std::lock_guard lock(dests_mu_);
    return (int)subbands_.size();
}

int IQStreamer::totalConsumers() const
{
    std::lock_guard                 lock(dests_mu_);
    std::vector<const std::string*> tasks;
    for (auto& d : dests_)
        tasks.push_back(&d.task_id);
    for (auto& sb : subbands_)
        tasks.push_back(&sb.task_id);
    std::sort(tasks.begin(),
              tasks.end(),
              [](auto* a, auto* b)
              {
                  return *a < *b;
              });
    return (int)std::distance(tasks.begin(),
                              std::unique(tasks.begin(),
                                          tasks.end(),
                                          [](auto* a, auto* b)
                                          {
                                              return *a == *b;
                                          }));
}

bool IQStreamer::retuneSubBands(double new_cf_hz, double new_sr_hz)
{
    std::lock_guard lock(dests_mu_);
    bool            all_ok = true;
    for (auto& sb : subbands_)
    {
        const double ratio = sb.output_sr > 0.0 ? new_sr_hz / sb.output_sr : 1.0;
        const int    decim = std::max(1, (int)std::lround(ratio));
        if (std::abs(ratio - decim) / decim >= 1e-3)
        {
            spdlog::error("IQStreamer [{}] sub-band {}: {:.0f} S/s does not divide the new "
                          "window rate {:.0f} S/s — muting it",
                          cfg_.stream_id,
                          sb.task_id,
                          sb.output_sr,
                          new_sr_hz);
            sb.ddc.reset();
            all_ok = false;
            continue;
        }
        sb.ddc = std::make_unique<Ddc>((double)sb.center_freq_hz - new_cf_hz, new_sr_hz, decim);
        sb.sample_rate_hz = (uint32_t)std::llround(new_sr_hz / decim);
    }
    return all_ok;
}

// ── Lifecycle ────────────────────────────────────────────────────────────────

void IQStreamer::start()
{
    if (running_.exchange(true, std::memory_order_acq_rel))
        return;
    start_epoch_ = (cfg_.task_epoch != Clock::time_point{}) ? cfg_.task_epoch : Clock::now();
    if (current_sr_hz_.load() == 0)
        current_sr_hz_.store((uint32_t)cfg_.sample_rate, std::memory_order_release);
    // Auto-register the primary dest from Config (backward-compat single-dest path)
    if (!cfg_.dest_ip.empty() && cfg_.dest_port > 0)
        addDest(cfg_.task_id, cfg_.stream_id, cfg_.dest_ip, cfg_.dest_port, cfg_.task_epoch);
    // Extra channels of a multi-channel stream belong to the owning task.
    for (size_t i = 0; i < cfg_.extra_channels.size(); ++i)
    {
        const auto& ec = cfg_.extra_channels[i];
        if (ec.dest_ip.empty() || ec.dest_port <= 0)
            continue;  // read-only channel (DDC consumers)
        Dest d;
        d.task_id       = cfg_.task_id;
        d.stream_id     = ec.stream_id;
        d.channel_index = ec.channel_index;
        d.buf_index     = (int)i + 1;
        d.epoch         = cfg_.task_epoch;
        d.fd            = openConnectedUdp(ec.dest_ip, ec.dest_port, "extra-channel");
        if (d.fd < 0)
            continue;
        std::lock_guard lock(dests_mu_);
        dests_.push_back(std::move(d));
    }
    {  // every consumer starts a fresh packet sequence on (re)start
        std::lock_guard lock(dests_mu_);
        for (auto& d : dests_)
        {
            d.first = true;
        }
        for (auto& sb : subbands_)
        {
            sb.first = true;
        }
    }
    thread_ = std::thread(&IQStreamer::workerLoop, this);
    spdlog::info("IQStreamer [{}] started", cfg_.stream_id);
}

void IQStreamer::stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel))
        return;
    if (thread_.joinable())
        thread_.join();
    {
        std::lock_guard lock(dests_mu_);
        for (auto& d : dests_)
            if (d.fd >= 0)
            {
                ::close(d.fd);
                d.fd = -1;
            }
        dests_.clear();
        for (auto& sb : subbands_)
            if (sb.dest_fd >= 0)
            {
                ::close(sb.dest_fd);
                sb.dest_fd = -1;
            }
        subbands_.clear();
    }
    std::lock_guard lock(mu_);
    spdlog::info("IQStreamer [{}] stopped pkts={} oflw={} dropped={}",
                 cfg_.stream_id,
                 metrics_.packets_sent,
                 metrics_.overflows,
                 packets_dropped_);
}

// ── Tune helpers ─────────────────────────────────────────────────────────────

void IQStreamer::updateCenterFreq(double new_cf_hz)
{
    current_cf_.store(new_cf_hz, std::memory_order_release);
    dwell_changed_.store(true, std::memory_order_release);
}

void IQStreamer::updateSampleRate(double new_sr_sps)
{
    current_sr_hz_.store((uint32_t)std::llround(new_sr_sps), std::memory_order_release);
}

void IQStreamer::pauseForRetune(int settle_reads)
{
    drain_countdown_.store(settle_reads, std::memory_order_release);
}

// ── Batched send ─────────────────────────────────────────────────────────────

int IQStreamer::sendBatch(int          fd,
                          uint32_t&    seq,
                          bool&        first,
                          int          ch_idx,
                          uint64_t     cf_hz,
                          uint32_t     sr_hz,
                          int64_t      ts0_ns,
                          double       ns_per_sample,
                          const float* samples,
                          int          n,
                          uint8_t      flags)
{
    if (fd < 0)
        return 0;
    const int P      = cfg_.packet_samples;
    const int n_pkts = (n <= 0) ? 1 : (n + P - 1) / P;  // n == 0 → one flag-only packet
    thread_local std::vector<IqPacketHeader> hdrs;
    thread_local std::vector<iovec>          iovs;
    thread_local std::vector<mmsghdr>        msgs;
    hdrs.resize((size_t)n_pkts);
    iovs.resize((size_t)n_pkts * 2);
    msgs.resize((size_t)n_pkts);
    for (int k = 0; k < n_pkts; ++k)
    {
        const int       off = k * P;
        const int       cnt = (n <= 0) ? 0 : std::min(P, n - off);
        IqPacketHeader& h   = hdrs[(size_t)k];
        h                   = {};
        h.magic             = IQ_PACKET_MAGIC;
        h.sequence          = seq + (uint32_t)k;
        const double ts     = (double)ts0_ns + (double)off * ns_per_sample;
        h.timestamp_ns      = ts > 0.0 ? (uint64_t)std::llround(ts) : 0u;
        h.center_freq_hz    = cf_hz;
        h.sample_rate       = sr_hz;
        h.num_samples       = (uint16_t)cnt;
        h.channel_index     = (uint8_t)ch_idx;
        h.flags             = (k == 0) ? flags : 0;
        if (k == 0 && first)
            h.flags |= IQ_FLAG_FIRST_PACKET;
        iovs[2 * (size_t)k]                = {&h, sizeof(h)};
        iovs[2 * (size_t)k + 1]            = {const_cast<float*>(samples + 2 * (size_t)off),
                                              (size_t)cnt * 2 * sizeof(float)};
        msgs[(size_t)k]                    = {};
        msgs[(size_t)k].msg_hdr.msg_iov    = &iovs[2 * (size_t)k];
        msgs[(size_t)k].msg_hdr.msg_iovlen = cnt > 0 ? 2 : 1;
    }
    // MSG_DONTWAIT ensures a full send buffer never stalls the SDR worker thread.
    int sent = 0;
    while (sent < n_pkts)
    {
        int r = ::sendmmsg(fd, msgs.data() + sent, (unsigned)(n_pkts - sent), MSG_DONTWAIT);
        if (r <= 0)
            break;
        sent += r;
    }
    // Sequence numbers advance for every packet produced, delivered or not, so
    // the receiver can see exactly how many it lost (ICD §10.4).
    seq += (uint32_t)n_pkts;
    if (sent > 0)
        first = false;
    if (sent < n_pkts)
    {
        std::lock_guard lock(mu_);
        packets_dropped_ += (uint64_t)(n_pkts - sent);
    }
    return sent;
}

// ── Worker loop ──────────────────────────────────────────────────────────────

void IQStreamer::workerLoop()
{
    const int P    = cfg_.packet_samples;
    const int n_ch = 1 + (int)cfg_.extra_channels.size();

    int chunk = cfg_.read_chunk_samples;
    if (chunk <= 0)
    {
        size_t mtu = 0;
        try
        {
            mtu = dev_->getStreamMTU(stream_);
        }
        catch (...)
        {
        }
        chunk = (int)std::clamp<size_t>(mtu, (size_t)P, (size_t)kMaxReadChunk);
    }
    chunk = std::max(P, (chunk / P) * P);

    // Per-channel sample buffers — one per SoapySDR channel in the stream.
    std::vector<std::vector<float>> ch_bufs((size_t)n_ch, std::vector<float>((size_t)chunk * 2));
    std::vector<void*>              soapy_bufs((size_t)n_ch);
    for (int i = 0; i < n_ch; ++i)
        soapy_bufs[(size_t)i] = ch_bufs[(size_t)i].data();

    int       flags = 0;
    long long hw_ts = 0;
    int       errs  = 0;

    // Stream time base: ns since stream_anchor_ for the first sample of a read.
    Clock::time_point stream_anchor{};
    bool              anchored        = false;
    bool              reanchor        = false;
    bool              use_hw_time     = false;
    long long         hw_anchor       = 0;
    double            accum_ns        = 0.0;  // time accumulated at earlier sample rates
    long long         samples_at_rate = 0;    // samples since last rate change / anchor
    double            rate_used       = (double)current_sr_hz_.load(std::memory_order_relaxed);
    int64_t           next_ts         = 0;  // stream time of the next expected sample
    Clock::time_point tp_t0{};              // throughput window start
    double            tp_bytes = 0.0;       // bytes sent in the current window

    while (running_.load(std::memory_order_acquire))
    {
        int ret =
            dev_->readStream(stream_, soapy_bufs.data(), (size_t)chunk, flags, hw_ts, 500'000LL);
        const auto t_read = Clock::now();
        if (!running_.load(std::memory_order_acquire))
            break;
        if (ret == SOAPY_SDR_TIMEOUT)
            continue;

        uint8_t pkt_flags = 0;
        if (ret == SOAPY_SDR_OVERFLOW)
        {
            pkt_flags |= IQ_FLAG_OVERFLOW;
            ++errs;
            reanchor = !use_hw_time;  // unknown number of samples lost
        }
        else if (ret < 0)
        {
            ++errs;
            if (errs == 1 || errs % 10 == 0)
                spdlog::error("IQStreamer [{}] readStream error {} ({})",
                              cfg_.stream_id,
                              SoapySDR::errToStr(ret),
                              errs);
            if (errs >= 20)
            {
                if (on_error_)
                {
                    // Fire callback from a separate thread — calling it directly
                    // would deadlock: deactivateTask → stop() → join() cannot
                    // join the calling (worker) thread from itself.
                    auto cb  = on_error_;
                    auto tid = cfg_.task_id;
                    auto msg = std::string(SoapySDR::errToStr(ret));
                    std::thread(
                        [cb, tid, msg]()
                        {
                            cb(tid, msg);
                        })
                        .detach();
                }
                break;
            }
            continue;
        }
        else
        {
            errs = 0;
        }

        const int n = ret > 0 ? std::min(ret, chunk) : 0;

        // ── Time base ─────────────────────────────────────────────────────
        double sr = (double)current_sr_hz_.load(std::memory_order_relaxed);
        if (sr <= 0.0)
            sr = rate_used > 0.0 ? rate_used : 1.0;
        if (sr != rate_used)
        {  // scan / combined retune changed fs
            if (rate_used > 0.0)
                accum_ns += (double)samples_at_rate * 1e9 / rate_used;
            samples_at_rate = 0;
            rate_used       = sr;
        }
        const double ns_per_sample = 1e9 / sr;
        int64_t      ts_stream     = next_ts;  // ns since stream_anchor (first sample)
        if (n > 0)
        {
            const bool hw = (flags & SOAPY_SDR_HAS_TIME) != 0;
            if (!anchored)
            {
                // The read returns after its last sample arrived; date the first one.
                stream_anchor   = t_read - nanoseconds((int64_t)(n * ns_per_sample));
                anchored        = true;
                use_hw_time     = hw;
                hw_anchor       = hw_ts;
                accum_ns        = 0.0;
                samples_at_rate = 0;
            }
            else if (reanchor && !hw)
            {
                accum_ns        = (double)nsBetween(stream_anchor, t_read) - n * ns_per_sample;
                samples_at_rate = 0;
            }
            reanchor = false;
            ts_stream =
                (use_hw_time && hw)
                    ? (int64_t)(hw_ts - hw_anchor)
                    : (int64_t)std::llround(accum_ns + (double)samples_at_rate * ns_per_sample);
            samples_at_rate += n;
            next_ts = ts_stream + (int64_t)std::llround(n * ns_per_sample);
        }

        if (int rem = drain_countdown_.load(std::memory_order_acquire); rem > 0)
        {
            drain_countdown_.fetch_sub(1, std::memory_order_release);
            continue;
        }
        if (dwell_changed_.exchange(false, std::memory_order_acq_rel))
            pkt_flags |= IQ_FLAG_DWELL_CHANGE;
        if (n == 0 && pkt_flags == 0)
            continue;

        const uint64_t cf   = (uint64_t)std::llround(current_cf_.load(std::memory_order_relaxed));
        const uint32_t sr_u = current_sr_hz_.load(std::memory_order_relaxed);
        int            primary_ok = 0;
        {
            std::lock_guard lock(dests_mu_);
            // Raw fan-out (primary channel subscribers + extra channels)
            for (auto& d : dests_)
            {
                const float*            buf = ch_bufs[(size_t)d.buf_index].data();
                const Clock::time_point ep =
                    (d.epoch != Clock::time_point{}) ? d.epoch : start_epoch_;
                const int64_t ts0 = ts_stream + nsBetween(ep, stream_anchor);
                int           ok  = sendBatch(d.fd,
                                   d.seq,
                                   d.first,
                                   d.channel_index,
                                   cf,
                                   sr_u,
                                   ts0,
                                   ns_per_sample,
                                   buf,
                                   n,
                                   pkt_flags);
                if (d.buf_index == 0)
                    primary_ok = std::max(primary_ok, ok);
            }
            // DDC sub-band fan-out: mix + filter + decimate the sub-band's channel
            for (auto& sb : subbands_)
            {
                if (!sb.ddc || sb.dest_fd < 0 || sb.buf_index < 0)
                    continue;
                const int D = sb.ddc->decimRatio();
                sb.out_buf.resize(((size_t)n / (size_t)std::max(D, 1) + 2) * 2);
                int n_out = 0;
                if (n > 0)
                    n_out =
                        sb.ddc->process(ch_bufs[(size_t)sb.buf_index].data(), n, sb.out_buf.data());
                if (n_out == 0 && pkt_flags == 0)
                    continue;
                const Clock::time_point ep =
                    (sb.epoch != Clock::time_point{}) ? sb.epoch : start_epoch_;
                const int64_t ts0 = ts_stream + nsBetween(ep, stream_anchor);
                sendBatch(sb.dest_fd,
                          sb.seq,
                          sb.first,
                          sb.channel_index,
                          sb.center_freq_hz,
                          sb.sample_rate_hz,
                          ts0,
                          ns_per_sample * D,
                          sb.out_buf.data(),
                          n_out,
                          pkt_flags);
            }
        }

        std::lock_guard lock(mu_);
        if (primary_ok > 0)
        {
            metrics_.packets_sent += (uint64_t)primary_ok;
            metrics_.samples_total += (uint64_t)n;
            // Mbit/s over ~250 ms windows. It used to be an EMA of megabits
            // *per read* (≈0.5 at any rate for 8 k-sample reads) — RM-67.
            tp_bytes += (double)n * 8.0 + primary_ok * (double)IQ_PACKET_HEADER_SIZE;
            if (tp_t0 == Clock::time_point{})
                tp_t0 = t_read;
            const double dt = std::chrono::duration<double>(t_read - tp_t0).count();
            if (dt >= 0.25)
            {
                const double mbps        = tp_bytes * 8.0 / dt / 1e6;
                metrics_.throughput_mbps = metrics_.throughput_mbps > 0.0
                                               ? 0.5 * metrics_.throughput_mbps + 0.5 * mbps
                                               : mbps;
                tp_bytes                 = 0.0;
                tp_t0                    = t_read;
            }
        }
        if (pkt_flags & IQ_FLAG_OVERFLOW)
            ++metrics_.overflows;
        if (n >= 64)
        {
            float rms_sq = 0;
            for (int i = 0; i < 64 * 2; ++i)
                rms_sq += ch_bufs[0][static_cast<size_t>(i)] * ch_bufs[0][static_cast<size_t>(i)];
            float rdbfs        = 10.f * std::log10(rms_sq / 64.f + 1e-30f);
            metrics_.rssi_dbfs = 0.95f * metrics_.rssi_dbfs + 0.05f * rdbfs;
        }
    }
}

StreamMetrics IQStreamer::getMetrics() const
{
    std::lock_guard lock(mu_);
    StreamMetrics   m = metrics_;
    m.packets_dropped = packets_dropped_;
    return m;
}

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
