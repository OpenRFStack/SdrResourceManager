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
#include "SpectrumTimeline.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>

namespace sdr
{

namespace
{
// Packing interval of a slot: the admission budget if one was recorded,
// otherwise the RF slice itself (slots inserted before the split, or by tests).
inline std::pair<double, double> budgetOf(const TimeFreqSlot& s)
{
    if (s.budget_hi_hz > s.budget_lo_hz)
        return {s.budget_lo_hz, s.budget_hi_hz};
    return {s.slice_lo_hz, s.slice_hi_hz};
}

// Distinct physical channels referenced by the overlapping slots. Several
// tasks may consume the same channel (fan-out / DDC sub-bands) and a channel
// used by back-to-back slots is still only one channel — summing the per-slot
// lists double-counted both cases (audit RM-02).
inline std::vector<bool>
usedChannels(const std::vector<const TimeFreqSlot*>& over, bool rx, int max_ch)
{
    std::vector<bool> used(static_cast<size_t>(std::max(max_ch, 0)), false);
    for (auto* s : over)
        for (int c : (rx ? s->rx_channels : s->tx_channels))
            if (c >= 0 && c < max_ch)
                used[static_cast<size_t>(c)] = true;
    return used;
}

inline int countTrue(const std::vector<bool>& v)
{
    return (int)std::count(v.begin(), v.end(), true);
}
}  // namespace

FitResult SpectrumTimeline::canFit(int64_t           t_start,
                                   int64_t           t_stop,
                                   double            cf_hz,
                                   double            bw_hz,
                                   double            sr_sps,
                                   int               rx_count,
                                   int               tx_count,
                                   int               max_rx,
                                   int               max_tx,
                                   double            guard_hz,
                                   bool              shared_lo,
                                   int               preferred_channel,
                                   const FitOptions& opts) const
{
    std::lock_guard lock(mu_);
    FitResult       res;

    // Gather overlapping slots (minus any the caller asked us to ignore).
    std::vector<const TimeFreqSlot*> over;
    for (auto& s : slots_)
    {
        if (!timeOverlap(t_start, t_stop, s.t_start, s.t_stop))
            continue;
        if (opts.ignore && opts.ignore->count(s.task_id))
            continue;
        over.push_back(&s);
    }

    const double req_lo = cf_hz - bw_hz / 2.0;
    const double req_hi = cf_hz + bw_hz / 2.0;
    res.rf_lo           = req_lo;
    res.rf_hi           = req_hi;

    auto      rx_used = usedChannels(over, true, max_rx);
    auto      tx_used = usedChannels(over, false, max_tx);
    const int used_rx = countTrue(rx_used);
    const int used_tx = countTrue(tx_used);

    // ── Independent-channel path ─────────────────────────────────────────
    // Each task tunes its own channel(s); no shared RF window.
    // Only check that enough channels are available.
    if (!shared_lo)
    {
        if (used_rx + rx_count > max_rx)
        {
            res.reject_code   = RejectCode::CHANNEL_COUNT_EXCEEDED;
            res.reject_reason = "RX " + std::to_string(used_rx) + "+" + std::to_string(rx_count)
                                + ">" + std::to_string(max_rx);
            return res;
        }
        if (used_tx + tx_count > max_tx)
        {
            res.reject_code   = RejectCode::CHANNEL_COUNT_EXCEEDED;
            res.reject_reason = "TX " + std::to_string(used_tx) + "+" + std::to_string(tx_count)
                                + ">" + std::to_string(max_tx);
            return res;
        }
        std::vector<int> arx, atx;
        if (preferred_channel >= 0 && rx_count == 1)
        {
            // Independent-LO: preferred channel must be free (no sharing)
            if (preferred_channel >= max_rx || rx_used[static_cast<size_t>(preferred_channel)])
            {
                res.reject_code = RejectCode::CHANNEL_COUNT_EXCEEDED;
                res.reject_reason =
                    "preferred_channel " + std::to_string(preferred_channel)
                    + (preferred_channel >= max_rx ? " out of range" : " already in use");
                return res;
            }
            arx.push_back(preferred_channel);
        }
        else
        {
            for (int i = 0; i < max_rx && (int)arx.size() < rx_count; ++i)
                if (!rx_used[static_cast<size_t>(i)])
                    arx.push_back(i);
        }
        for (int i = 0; i < max_tx && (int)atx.size() < tx_count; ++i)
            if (!tx_used[static_cast<size_t>(i)])
                atx.push_back(i);
        res.ok          = true;
        res.device_cf   = cf_hz;
        res.device_rate = sr_sps;
        res.placed_lo   = req_lo;
        res.placed_hi   = req_hi;
        res.avail_rx    = arx;
        res.avail_tx    = atx;
        return res;
    }

    // ── Shared-LO path ───────────────────────────────────────────────────
    // A scan (or any exclusive holder) retunes the shared LO on its own
    // schedule: nobody may share it, and it may not join an occupied LO.
    for (auto* s : over)
    {
        if (s->exclusive)
        {
            res.reject_code   = RejectCode::RETUNE_CONFLICT;
            res.reject_reason = "Device LO is exclusively held (scan) by task " + s->task_id;
            return res;
        }
    }
    if (opts.exclusive && !over.empty())
    {
        res.reject_code   = RejectCode::RETUNE_CONFLICT;
        res.reject_reason = "Exclusive (scan) request needs an idle LO; "
                            + std::to_string(over.size()) + " task(s) overlap";
        return res;
    }

    double dev_cf   = cf_hz;
    double dev_rate = sr_sps;
    if (!over.empty())
    {
        dev_cf   = over[0]->center_freq_hz;
        dev_rate = over[0]->sample_rate_sps;
        // Every overlapping slot must agree with the request — with future
        // reservations the slots can sit at different CFs at different times
        // and a request spanning them cannot hold one LO (audit RM-01).
        for (auto* s : over)
        {
            if (std::abs(s->center_freq_hz - cf_hz) > 1.0)
            {
                res.reject_code   = RejectCode::RETUNE_CONFLICT;
                res.reject_reason = "Device locked to "
                                    + std::to_string((long long)(s->center_freq_hz / 1e6))
                                    + " MHz (task " + s->task_id + "); requested "
                                    + std::to_string((long long)(cf_hz / 1e6)) + " MHz";
                return res;
            }
            if (std::abs(s->sample_rate_sps - sr_sps) > 1.0)
            {
                res.reject_code   = RejectCode::RETUNE_CONFLICT;
                res.reject_reason = "Device sample rate locked to "
                                    + std::to_string((long long)(s->sample_rate_sps / 1e6))
                                    + " MSPS (task " + s->task_id + "); requested "
                                    + std::to_string((long long)(sr_sps / 1e6)) + " MSPS";
                return res;
            }
        }
    }

    // Slice must fit in device window
    double win_lo = dev_cf - dev_rate / 2.0;
    double win_hi = dev_cf + dev_rate / 2.0;
    if (req_lo < win_lo || req_hi > win_hi)
    {
        res.reject_code   = RejectCode::SPECTRUM_CONFLICT;
        res.reject_reason = "Slice [" + std::to_string((long long)(req_lo / 1e6)) + "–"
                            + std::to_string((long long)(req_hi / 1e6)) + " MHz] outside window ["
                            + std::to_string((long long)(win_lo / 1e6)) + "–"
                            + std::to_string((long long)(win_hi / 1e6)) + " MHz]";
        return res;
    }

    // Bandwidth budget: left-to-right packing with guard. The position found
    // here is bookkeeping only — the task still receives its requested band
    // (rf_lo/rf_hi), which is what slice_offset_hz and the DDC must use.
    using IV = std::pair<double, double>;
    std::vector<IV> occ;
    for (auto* s : over)
    {
        auto [blo, bhi] = budgetOf(*s);
        occ.push_back({blo - guard_hz, bhi + guard_hz});
    }
    std::sort(occ.begin(), occ.end());
    // Guard band separates adjacent tasks. When there are no existing slots,
    // there is nothing to guard against — don't include guard padding on the
    // outer edges of the window so a task that fills the full device BW can fit.
    double guard_inner = over.empty() ? 0.0 : guard_hz;
    double needed      = bw_hz + 2.0 * guard_inner;
    double try_lo      = win_lo;
    bool   placed      = false;
    double p_lo = 0, p_hi = 0;
    for (;;)
    {
        double try_hi = try_lo + needed;
        if (try_hi > win_hi)
            break;
        bool   clash = false;
        double push  = 0;
        for (auto& [olo, ohi] : occ)
        {
            if (try_lo < ohi && olo < try_hi)
            {
                clash = true;
                push  = ohi;
                break;
            }
        }
        if (!clash)
        {
            placed = true;
            p_lo   = try_lo + guard_inner;
            p_hi   = p_lo + bw_hz;
            break;
        }
        try_lo = push;
    }
    if (!placed)
    {
        res.reject_code   = RejectCode::SPECTRUM_CONFLICT;
        res.reject_reason = "No " + std::to_string((long long)(bw_hz / 1e3))
                            + " kHz slot available in device window";
        return res;
    }

    // Channel assignment — preferred_channel overrides the normal count-then-pick logic.
    // On shared-LO devices the requested channel is allowed to already be in use:
    // activateTask detects it in channel_states_ and subscribes to the live IQStreamer
    // (fan-out or DDC) rather than opening new hardware — unless the caller must own
    // the stream exclusively (triggered capture), in which case it has to be free.
    std::vector<int> arx, atx;
    if (preferred_channel >= 0 && rx_count == 1)
    {
        if (preferred_channel >= max_rx)
        {
            res.reject_code = RejectCode::CHANNEL_COUNT_EXCEEDED;
            res.reject_reason =
                "preferred_channel " + std::to_string(preferred_channel) + " out of range";
            return res;
        }
        if (!opts.allow_channel_reuse && rx_used[static_cast<size_t>(preferred_channel)])
        {
            res.reject_code   = RejectCode::CHANNEL_COUNT_EXCEEDED;
            res.reject_reason = "preferred_channel " + std::to_string(preferred_channel)
                                + " already in use and this task cannot share a stream";
            return res;
        }
        arx.push_back(preferred_channel);
    }
    else
    {
        if (used_rx + rx_count > max_rx)
        {
            res.reject_code   = RejectCode::CHANNEL_COUNT_EXCEEDED;
            res.reject_reason = "RX " + std::to_string(used_rx) + "+" + std::to_string(rx_count)
                                + ">" + std::to_string(max_rx);
            return res;
        }
        for (int i = 0; i < max_rx && (int)arx.size() < rx_count; ++i)
            if (!rx_used[static_cast<size_t>(i)])
                arx.push_back(i);
    }
    if (used_tx + tx_count > max_tx)
    {
        res.reject_code   = RejectCode::CHANNEL_COUNT_EXCEEDED;
        res.reject_reason = "TX " + std::to_string(used_tx) + "+" + std::to_string(tx_count) + ">"
                            + std::to_string(max_tx);
        return res;
    }
    for (int i = 0; i < max_tx && (int)atx.size() < tx_count; ++i)
        if (!tx_used[static_cast<size_t>(i)])
            atx.push_back(i);

    res.ok          = true;
    res.device_cf   = dev_cf;
    res.device_rate = dev_rate;
    res.placed_lo   = p_lo;
    res.placed_hi   = p_hi;
    res.avail_rx    = arx;
    res.avail_tx    = atx;
    return res;
}

SpectrumTimeline::CombineResult SpectrumTimeline::canCombine(int64_t t_start,
                                                             int64_t t_stop,
                                                             double  cf_hz,
                                                             double  bw_hz,
                                                             int     rx_count,
                                                             int     max_rx,
                                                             double  guard_hz,
                                                             double  sr_max,
                                                             bool    reuse_channels) const
{
    std::lock_guard lock(mu_);
    CombineResult   res;

    std::vector<const TimeFreqSlot*> over;
    for (auto& s : slots_)
        if (timeOverlap(t_start, t_stop, s.t_start, s.t_stop))
            over.push_back(&s);

    if (over.empty())
    {
        res.reject_reason = "No existing tasks to combine with";
        return res;
    }
    for (auto* s : over)
    {
        if (s->exclusive)
        {
            res.reject_reason = "Device LO is exclusively held (scan) by task " + s->task_id;
            return res;
        }
    }

    // The combined window must cover every task's *requested* RF band
    // (slice_lo/hi). Using relocated packing positions here could move the LO
    // away from an existing task's actual signal.
    double req_lo = cf_hz - bw_hz / 2.0;
    double req_hi = cf_hz + bw_hz / 2.0;
    double min_lo = req_lo, max_hi = req_hi;
    for (auto* s : over)
    {
        min_lo = std::min(min_lo, s->slice_lo_hz);
        max_hi = std::max(max_hi, s->slice_hi_hz);
    }

    double combined_sr = (max_hi - min_lo) + 2.0 * guard_hz;
    double combined_cf = (min_lo + max_hi) / 2.0;

    if (combined_sr > sr_max)
    {
        res.reject_reason = "Combined SR " + std::to_string((long long)(combined_sr / 1e6))
                            + " MSPS exceeds device max "
                            + std::to_string((long long)(sr_max / 1e6)) + " MSPS";
        return res;
    }

    if (reuse_channels)
    {
        // Return the existing channels so the caller can subscribe to the live
        // IQStreamer rather than opening new hardware.
        std::vector<int> existing;
        for (auto* s : over)
        {
            for (int c : s->rx_channels)
            {
                if (std::find(existing.begin(), existing.end(), c) == existing.end())
                    existing.push_back(c);
                if ((int)existing.size() == rx_count)
                    break;
            }
            if ((int)existing.size() == rx_count)
                break;
        }
        if (existing.empty())
        {
            res.reject_reason = "No existing channels to share";
            return res;
        }
        res.ok           = true;
        res.combined_cf  = combined_cf;
        res.combined_sr  = combined_sr;
        res.new_slice_lo = req_lo;
        res.new_slice_hi = req_hi;
        res.avail_rx     = existing;
        return res;
    }

    auto      rx_used = usedChannels(over, true, max_rx);
    const int used_rx = countTrue(rx_used);
    if (used_rx + rx_count > max_rx)
    {
        res.reject_reason = "RX " + std::to_string(used_rx) + "+" + std::to_string(rx_count) + ">"
                            + std::to_string(max_rx);
        return res;
    }
    std::vector<int> arx;
    for (int i = 0; i < max_rx && (int)arx.size() < rx_count; ++i)
        if (!rx_used[static_cast<size_t>(i)])
            arx.push_back(i);

    res.ok           = true;
    res.combined_cf  = combined_cf;
    res.combined_sr  = combined_sr;
    res.new_slice_lo = req_lo;
    res.new_slice_hi = req_hi;
    res.avail_rx     = arx;
    return res;
}

void SpectrumTimeline::updateDeviceTune(double new_cf, double new_sr, int64_t ts, int64_t te)
{
    std::lock_guard lock(mu_);
    for (auto& s : slots_)
    {
        if (!timeOverlap(ts, te, s.t_start, s.t_stop))
            continue;
        s.center_freq_hz  = new_cf;
        s.sample_rate_sps = new_sr;
    }
    spdlog::debug(
        "Timeline: updateDeviceTune cf={:.3f}MHz sr={:.3f}MSPS", new_cf / 1e6, new_sr / 1e6);
}

void SpectrumTimeline::retime(const std::string& task_id, int64_t t_start, int64_t t_stop)
{
    std::lock_guard lock(mu_);
    for (auto& s : slots_)
        if (s.task_id == task_id)
        {
            s.t_start = t_start;
            s.t_stop  = t_stop;
        }
}

int SpectrumTimeline::activeTaskCount(int64_t at_ms) const
{
    std::lock_guard                 lock(mu_);
    std::vector<const std::string*> ids;
    for (auto& s : slots_)
        if (at_ms >= s.t_start && at_ms < s.t_stop)
            ids.push_back(&s.task_id);
    std::sort(ids.begin(),
              ids.end(),
              [](auto* x, auto* y)
              {
                  return *x < *y;
              });
    return (int)std::distance(ids.begin(),
                              std::unique(ids.begin(),
                                          ids.end(),
                                          [](auto* x, auto* y)
                                          {
                                              return *x == *y;
                                          }));
}

void SpectrumTimeline::insert(const TimeFreqSlot& s)
{
    std::lock_guard lock(mu_);
    slots_.push_back(s);
    spdlog::debug("Timeline: insert {} t=[{},{}] f=[{:.2f},{:.2f}]MHz",
                  s.task_id,
                  s.t_start,
                  s.t_stop,
                  s.slice_lo_hz / 1e6,
                  s.slice_hi_hz / 1e6);
}

void SpectrumTimeline::remove(const std::string& id)
{
    std::lock_guard lock(mu_);
    auto            it = std::remove_if(slots_.begin(),
                             slots_.end(),
                             [&](const TimeFreqSlot& s)
                             {
                                 return s.task_id == id;
                             });
    if (it != slots_.end())
    {
        slots_.erase(it, slots_.end());
    }
}

std::vector<TimeFreqSlot> SpectrumTimeline::slotsOverlapping(int64_t ts, int64_t te) const
{
    std::lock_guard           lock(mu_);
    std::vector<TimeFreqSlot> r;
    for (auto& s : slots_)
        if (timeOverlap(ts, te, s.t_start, s.t_stop))
            r.push_back(s);
    return r;
}

double SpectrumTimeline::allocatedBw(int64_t at_ms) const
{
    std::lock_guard lock(mu_);
    double          sum = 0;
    for (auto& s : slots_)
        if (at_ms >= s.t_start && at_ms < s.t_stop)
            sum += s.slice_hi_hz - s.slice_lo_hz;
    return sum;
}

int SpectrumTimeline::usedRx(int64_t at_ms) const
{
    std::lock_guard  lock(mu_);
    std::vector<int> chans;
    for (auto& s : slots_)
        if (at_ms >= s.t_start && at_ms < s.t_stop)
            for (int c : s.rx_channels)
                if (std::find(chans.begin(), chans.end(), c) == chans.end())
                    chans.push_back(c);
    return (int)chans.size();
}

bool SpectrumTimeline::hasExclusive(int64_t ts, int64_t te) const
{
    std::lock_guard lock(mu_);
    for (auto& s : slots_)
        if (s.exclusive && timeOverlap(ts, te, s.t_start, s.t_stop))
            return true;
    return false;
}

int SpectrumTimeline::slotCount() const
{
    std::lock_guard lock(mu_);
    return (int)slots_.size();
}

std::optional<SpectrumTimeline::Window> SpectrumTimeline::activeWindow(int64_t at_ms) const
{
    std::lock_guard lock(mu_);
    for (auto& s : slots_)
        if (at_ms >= s.t_start && at_ms < s.t_stop)
            return Window{s.center_freq_hz, s.sample_rate_sps};
    return std::nullopt;
}

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
