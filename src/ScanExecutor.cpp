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
#include "ScanExecutor.hpp"

#include "IQStreamer.hpp"

#include <spdlog/spdlog.h>

#include <chrono>
#include <thread>

namespace sdr
{
using namespace std::chrono;

ScanExecutor::ScanExecutor(const std::string&       id,
                           const ScanParams&        p,
                           RetuneFn                 retune,
                           std::vector<IQStreamer*> s,
                           DoneCb                   done,
                           int                      settle_reads,
                           int64_t                  settle_samples)
    : task_id_(id)
    , params_(p)
    , retune_(std::move(retune))
    , streamers_(std::move(s))
    , done_(std::move(done))
    , settle_reads_(settle_reads)
    , settle_samples_(settle_samples)
{
}

ScanExecutor::~ScanExecutor()
{
    stop();
}

void ScanExecutor::start()
{
    if (running_.exchange(true))
        return;
    thread_ = std::thread(&ScanExecutor::loop, this);
    spdlog::info("ScanExecutor [{}] started {} entries repeat={}",
                 task_id_,
                 params_.entries.size(),
                 params_.repeat);
}

void ScanExecutor::stop()
{
    {
        std::lock_guard lk(wait_mu_);
        running_.store(false);
    }
    wait_cv_.notify_all();
    if (!thread_.joinable())
        return;
    // If called from the scan thread itself (via done-callback), joining would deadlock.
    if (thread_.get_id() == std::this_thread::get_id())
        thread_.detach();
    else
        thread_.join();
}

void ScanExecutor::loop()
{
    if (params_.entries.empty())
    {
        if (done_)
            done_(task_id_, false);
        return;
    }
    bool go = true, ok = true;
    while (go && running_.load())
    {
        int retuned = 0;
        for (size_t i = 0; i < params_.entries.size() && running_.load(); ++i)
        {
            const auto& e = params_.entries[i];
            step_.store((int)i);
            // Discard the reads that straddle the retune: samples already in
            // the driver pipeline were captured at the previous entry's LO but
            // would go out flagged DWELL_CHANGE with the new CF (RM-65).
            for (auto* s : streamers_)
                if (s && s->isRunning())
                    s->pauseForRetune(settle_reads_, settle_samples_);
            if (!retune_(e.center_freq_hz, e.sample_rate_sps))
            {
                spdlog::warn("ScanExecutor [{}] retune failed step {} ({:.3f} MHz) — skipping",
                             task_id_,
                             i,
                             e.center_freq_hz / 1e6);
                continue;
            }
            ++retuned;
            // ICD §8.4: CF *and* sample rate in the packet header follow the entry.
            for (auto* s : streamers_)
                if (s && s->isRunning())
                {
                    s->updateSampleRate(e.sample_rate_sps);
                    s->updateCenterFreq(e.center_freq_hz);
                }
            // The dwell is time *delivered at this entry*: wait for the
            // settle reads to be dropped first. Timing from retune() let the
            // drain eat the dwell — 0.5 of 5 reads with 20 ms reads (RM-71).
            // 50 ms slices so stop() is not held up; give up after 2 s (a
            // stalled driver never completes its reads).
            for (auto* s : streamers_)
            {
                int slices = 0;
                while (s && s->isRunning() && running_.load()
                       && !s->waitRetuneSettled(std::chrono::milliseconds(50)) && ++slices < 40)
                {
                }
                if (slices >= 40)
                    spdlog::warn("ScanExecutor [{}] step {}: retune drain did not finish in 2 s",
                                 task_id_,
                                 i);
            }
            spdlog::debug("ScanExecutor [{}] step {} cf={:.3f}MHz dwell={}ms",
                          task_id_,
                          i + 1,
                          e.center_freq_hz / 1e6,
                          e.dwell_ms);
            // Sleep for the dwell; stop() wakes us immediately. (The previous
            // loop busy-waited the last 10 ms of every dwell.)
            std::unique_lock lk(wait_mu_);
            wait_cv_.wait_until(lk,
                                steady_clock::now() + milliseconds(e.dwell_ms),
                                [this]
                                {
                                    return !running_.load();
                                });
        }
        // A pass in which no entry could be tuned means the hardware is gone:
        // fail instead of spinning on retune() at 100% CPU (repeat mode) or
        // reporting a scan that never happened as successful.
        if (retuned == 0 && running_.load())
        {
            spdlog::error("ScanExecutor [{}] no entry could be tuned — failing scan", task_id_);
            ok = false;
            go = false;
        }
        if (!params_.repeat)
            go = false;
    }
    spdlog::info("ScanExecutor [{}] done", task_id_);
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
