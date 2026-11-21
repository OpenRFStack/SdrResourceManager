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
#include "TriggerMonitor.hpp"

#include "IQStreamer.hpp"

#include <SoapySDR/Errors.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <thread>

namespace sdr
{
using namespace std::chrono;

TriggerMonitor::TriggerMonitor(const std::string&       id,
                               const TriggerParams&     p,
                               SoapySDR::Device*        dev,
                               SoapySDR::Stream*        stream,
                               IQStreamer*              s,
                               double                   rate,
                               DoneCb                   done,
                               std::vector<IQStreamer*> also_start)
    : task_id_(id)
    , params_(p)
    , dev_(dev)
    , stream_(stream)
    , streamer_(s)
    , also_(std::move(also_start))
    , rate_(rate)
    , done_(std::move(done))
{
}

TriggerMonitor::~TriggerMonitor()
{
    stop();
}

void TriggerMonitor::start()
{
    if (running_.exchange(true))
        return;
    thread_ = std::thread(&TriggerMonitor::loop, this);
    spdlog::info("TriggerMonitor [{}] started thr={:.1f}dBFS", task_id_, params_.threshold_dbfs);
}

void TriggerMonitor::stop()
{
    running_.store(false);
    if (!thread_.joinable())
        return;
    // done_() fires from loop() which runs on thread_ — joining from the same
    // thread is UB; detach so the thread cleans up on its own (same pattern
    // as ScanExecutor::stop()).
    if (thread_.get_id() == std::this_thread::get_id())
        thread_.detach();
    else
        thread_.join();
}

float TriggerMonitor::rmsDbfs(const float* b, int n)
{
    // 16 fixed lanes: a single running float sum is a serial dependency chain
    // the compiler may not vectorise without -ffast-math.
    float     lane[16]{};
    const int m = n * 2;
    int       i = 0;
    for (; i + 16 <= m; i += 16)
        for (int j = 0; j < 16; ++j)
            lane[j] += b[i + j] * b[i + j];
    float sq = 0;
    for (; i < m; ++i)
        sq += b[i] * b[i];
    for (float l : lane)
        sq += l;
    return 10.f * std::log10(sq / (float)n + 1e-30f);
}

void TriggerMonitor::Ring::init(int channels, size_t capacity)
{
    nch = std::max(1, channels);
    cap = capacity;
    buf.assign((size_t)nch, std::vector<float>(2 * cap));
    head = size = 0;
}

void TriggerMonitor::Ring::push(void* const* ch, size_t n)
{
    if (cap == 0 || n == 0)
        return;
    const size_t skip = n > cap ? n - cap : 0;  // only the newest cap samples matter
    for (int c = 0; c < nch; ++c)
    {
        const float* src = static_cast<const float*>(ch[c]) + 2 * skip;
        size_t       pos = head, left = n - skip;
        while (left > 0)
        {
            const size_t k = std::min(left, cap - pos);
            std::memcpy(buf[(size_t)c].data() + 2 * pos, src, 2 * k * sizeof(float));
            src += 2 * k;
            left -= k;
            pos = (pos + k) % cap;
        }
    }
    head = (head + (n - skip)) % cap;
    size = std::min(cap, size + (n - skip));
}

std::vector<std::vector<float>> TriggerMonitor::Ring::take()
{
    std::vector<std::vector<float>> out((size_t)nch, std::vector<float>(2 * size));
    const size_t                    start = (head + cap - size) % cap;
    for (int c = 0; c < nch; ++c)
    {
        const size_t first = std::min(size, cap - start);
        std::memcpy(
            out[(size_t)c].data(), buf[(size_t)c].data() + 2 * start, 2 * first * sizeof(float));
        std::memcpy(out[(size_t)c].data() + 2 * first,
                    buf[(size_t)c].data(),
                    2 * (size - first) * sizeof(float));
    }
    head = size = 0;
    return out;
}

// Read whatever the task's other streams have buffered (non-blocking) into
// their pre-trigger rings. Unread, they buffered the whole wait and each
// capture on the other boards began with stale samples (RM-78).
void TriggerMonitor::drainOthers(std::vector<Ring>& rings)
{
    static constexpr int                         kMaxCh = 16;
    thread_local std::vector<std::vector<float>> scratch(kMaxCh,
                                                         std::vector<float>((size_t)RD * 2));
    void*                                        bufs[kMaxCh];
    for (int c = 0; c < kMaxCh; ++c)
        bufs[c] = scratch[(size_t)c].data();
    for (size_t i = 0; i < also_.size(); ++i)
    {
        IQStreamer* o = also_[i];
        if (!o || !o->soapyDevice() || !o->soapyStream())
            continue;
        for (int k = 0; k < 64; ++k)
        {
            int       fl = 0;
            long long ts = 0;
            const int r =
                o->soapyDevice()->readStream(o->soapyStream(), bufs, (size_t)RD, fl, ts, 0);
            if (r > 0)
                rings[i].push(bufs, (size_t)r);
            if (r < RD)  // empty (timeout / error) or caught up
                break;
        }
    }
}

void TriggerMonitor::loop()
{
    // One buffer per channel of the stream: a 2-channel stream (shared-LO
    // board, rx_count 2) wrote channel 1 through bufs[1] past a one-element
    // array — a stack overflow on every read (ASan, RM-79). RMS uses ch 0.
    static constexpr int            kMaxCh = 16;
    std::vector<std::vector<float>> ch_buf(kMaxCh, std::vector<float>((size_t)RD * 2));
    void*                           bufs[kMaxCh];
    for (int c = 0; c < kMaxCh; ++c)
        bufs[c] = ch_buf[(size_t)c].data();
    int       flags              = 0;
    long long hw_ts              = 0;
    int       caps_done          = 0;
    int       consecutive_errors = 0;
    bool      ok                 = true;

    // pre_trigger_ms was ignored and the block that crossed the threshold was
    // consumed here, so every capture began after the burst start (RM-80).
    // Each stream keeps pre_trigger_ms + one read in a ring, handed to its
    // streamer as the first samples of the capture.
    const size_t pre_n = (size_t)std::clamp<long long>(
        std::llround(std::max(0, params_.pre_trigger_ms) * rate_ / 1000.0), 0, 1LL << 22);
    Ring              prim;
    std::vector<Ring> rings(also_.size());
    prim.init(streamer_ ? streamer_->channelCount() : 1, pre_n + RD);
    for (size_t i = 0; i < also_.size(); ++i)
        rings[i].init(also_[i] ? also_[i]->channelCount() : 1, pre_n + RD);

    while (running_.load())
    {
        // Detection phase: read stream in this thread looking for threshold crossing.
        int ret = dev_->readStream(stream_, bufs, (size_t)RD, flags, hw_ts, 200'000LL);
        drainOthers(rings);
        if (ret == SOAPY_SDR_TIMEOUT)
            continue;
        if (ret < 0)
        {
            // Errors that return immediately used to make this a 100% CPU spin.
            if (ret != SOAPY_SDR_OVERFLOW && ++consecutive_errors >= 50)
            {
                spdlog::error(
                    "TriggerMonitor [{}] stream keeps failing ({}) — giving up", task_id_, ret);
                ok = false;
                break;
            }
            std::this_thread::sleep_for(milliseconds(std::min(consecutive_errors + 1, 20)));
            continue;
        }
        consecutive_errors = 0;
        const int n        = std::min(ret, RD);
        prim.push(bufs, (size_t)n);
        // Threshold per BLK sub-block: a short burst is not diluted by a
        // 4096-sample read.
        float rms   = -1e30f;
        bool  fired = false;
        for (int o = 0; o + BLK <= n || (o == 0 && n > 0); o += BLK)
        {
            rms = rmsDbfs(ch_buf[0].data() + 2 * o, std::min(BLK, n - o));
            if (rms >= params_.threshold_dbfs)
            {
                fired = true;
                break;
            }
        }
        if (!fired)
            continue;

        spdlog::info("TriggerMonitor [{}] TRIGGERED {:.1f}dBFS", task_id_, rms);

        // Hand off to IQStreamer: stop reading from stream so IQStreamer's
        // workerLoop is the sole reader (two concurrent readStream callers on the
        // same SoapySDR::Stream are not safe and would race for packets).
        if (streamer_)
            streamer_->setPrefix(prim.take());
        for (size_t i = 0; i < also_.size(); ++i)
            if (also_[i])
                also_[i]->setPrefix(rings[i].take());
        if (streamer_ && !streamer_->isRunning())
            streamer_->start();
        for (auto* o : also_)
            if (o && !o->isRunning())
                o->start();

        // Wait post_trigger_ms via sleep rather than by counting samples —
        // the stream is owned by IQStreamer during this window.
        auto post_end = steady_clock::now() + milliseconds(params_.post_trigger_ms);
        while (running_.load() && steady_clock::now() < post_end)
            std::this_thread::sleep_for(milliseconds(10));

        if (streamer_ && streamer_->isRunning())
            streamer_->stop();
        for (auto* o : also_)
            if (o && o->isRunning())
                o->stop();
        ++caps_done;
        captures_.store(caps_done);
        spdlog::info("TriggerMonitor [{}] capture #{} done", task_id_, caps_done);
        if (params_.max_captures > 0 && caps_done >= params_.max_captures)
            break;
    }
    if (done_)
        done_(task_id_, ok);
}
}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
