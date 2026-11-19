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
#include "ResourceManager.hpp"

#include "sdr/MessageCodec.hpp"

#include <spdlog/spdlog.h>
#include <uuid/uuid.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace sdr
{

using namespace std::chrono;

static std::string genUuid()
{
    uuid_t uu;
    uuid_generate_random(uu);
    char buf[37];
    uuid_unparse_lower(uu, buf);
    return buf;
}

// Aggregate capability bounds across all loaded devices so validation is
// device-agnostic — a request is sane if at least one device could handle it.
struct CapBounds
{
    double freq_min, freq_max, bw_max, sr_max;
};

static CapBounds
aggregateCaps(const std::unordered_map<std::string, std::unique_ptr<RadioDevice>>& devs)
{
    CapBounds b{std::numeric_limits<double>::max(), 0.0, 0.0, 0.0};
    for (auto& [id, dev] : devs)
    {
        const auto& c = dev->config().caps;
        b.freq_min    = std::min(b.freq_min, c.freq_min_hz);
        b.freq_max    = std::max(b.freq_max, c.freq_max_hz);
        b.bw_max      = std::max(b.bw_max, c.bandwidth_max_hz);
        b.sr_max      = std::max(b.sr_max, c.sample_rate_max_sps);
    }
    return b;
}

// ─────────────────────────────────────────────────────────────────────────
ResourceManager::ResourceManager(const AppConfig&   cfg,
                                 TaskStateChangedCb on_state_change,
                                 TaskErrorCb        on_error)
    : cfg_(cfg)
    , on_state_change_(std::move(on_state_change))
    , on_error_(std::move(on_error))
    , start_time_(steady_clock::now())
{
    port_pool_  = std::make_unique<UdpPortPool>(cfg_.policy.udp_port_pool_start,
                                               cfg_.policy.udp_port_pool_end);
    fft_engine_ = std::make_unique<FftEngine>();

    for (auto& dc : cfg_.devices)
    {
        if (devices_.count(dc.id))
        {
            // A duplicate id used to silently replace the first device.
            spdlog::error("ResourceManager: duplicate device id '{}' in configuration — ignoring "
                          "the second entry",
                          dc.id);
            continue;
        }
        devices_[dc.id]   = std::make_unique<RadioDevice>(dc);
        timelines_[dc.id] = std::make_unique<SpectrumTimeline>();
        device_order_.push_back(dc.id);
    }
}

ResourceManager::~ResourceManager()
{
    closeDevices();
}

int ResourceManager::openDevices()
{
    int ok = 0;
    for (auto& id : device_order_)
    {
        auto& dev = devices_.at(id);
        if (dev->open())
        {
            ++ok;
            spdlog::info("ResourceManager: opened {}", id);
        }
        else
        {
            spdlog::warn("ResourceManager: failed to open {}", id);
        }
    }
    return ok;
}

namespace
{
/// Counts a background callback into a ResourceManager (released on scope
/// exit with release ordering — pairs with closeDevices()' acquire wait).
/// Holds a reference to the counter, not to the callback's captures: the
/// callback's own std::function may be destroyed inside deactivateTask.
struct InFlight
{
    std::atomic<int>& n;

    explicit InFlight(std::atomic<int>& c)
        : n(c)
    {
        n.fetch_add(1, std::memory_order_relaxed);
    }

    ~InFlight()
    {
        n.fetch_sub(1, std::memory_order_release);
    }

    InFlight(const InFlight&)            = delete;
    InFlight& operator=(const InFlight&) = delete;
};
}  // namespace

std::mutex& ResourceManager::hwMutex(const std::string& device_id)
{
    std::lock_guard lk(hw_map_mu_);
    auto&           m = hw_mu_[device_id];
    if (!m)
        m = std::make_unique<std::mutex>();
    return *m;
}

ResourceManager::HwLocks ResourceManager::lockHw(std::vector<std::string> device_ids)
{
    std::sort(device_ids.begin(), device_ids.end());
    device_ids.erase(std::unique(device_ids.begin(), device_ids.end()), device_ids.end());
    HwLocks locks;
    locks.reserve(device_ids.size());
    for (const auto& id : device_ids)
        locks.emplace_back(hwMutex(id));
    return locks;
}

void ResourceManager::closeDevices()
{
    // Deactivate all running tasks first
    std::vector<std::string> active_ids;
    {
        std::lock_guard lock(reg_mu_);
        for (auto& [id, rec] : registry_)
            if (!isTerminalState(rec.state))
                active_ids.push_back(id);
    }
    for (auto& id : active_ids)
        deactivateTask(id, TaskState::CANCELLED, "controller shutdown");

    // A detached activateTask() thread may have already passed its
    // terminal-state check (above) and be mid-flight opening hardware /
    // starting an IQStreamer when this call raced ahead of it. Wait for any
    // such in-flight activations to finish before touching the devices —
    // otherwise dev->close() (and the eventual device destruction) can run
    // concurrently with that thread's SoapySDR calls and IQStreamer worker.
    //
    // While waiting, keep re-running deactivation for any task that currently
    // has a live runtime: an in-flight activateTask() may itself be blocked on
    // a hardware lock held by another task's orphaned runtime (one whose
    // activateTask() raced past the terminal-state checks and inserted a
    // runtime, with hw_lock still held, AFTER our pass(es) above already found
    // it terminal with no runtime to clean up). deactivateTask's "orphaned
    // runtime" path releases that runtime's hw_lock, unblocking the in-flight
    // activation so activations_in_flight_ can reach zero.
    //
    // We must scan runtimes_ directly rather than the active_ids snapshot
    // above: a task can already be terminal (and absent from active_ids) at
    // snapshot time yet still have its activateTask() thread in flight,
    // inserting an orphaned runtime later. Without this, the wait below can
    // deadlock.
    auto deactivateOrphanedRuntimes = [this]()
    {
        std::vector<std::string> ids;
        {
            std::lock_guard lock(rt_mu_);
            for (auto& [id, rt] : runtimes_)
                ids.push_back(id);
        }
        for (auto& id : ids)
            deactivateTask(id, TaskState::CANCELLED, "controller shutdown");
    };

    while (activations_in_flight_.load(std::memory_order_acquire) > 0)
    {  // pairs with the release in the activation threads
        deactivateOrphanedRuntimes();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Final pass for any runtime inserted just as the last in-flight
    // activation finished.
    deactivateOrphanedRuntimes();

    // Defence in depth: nothing may still be reading a device we are about to
    // close (SoapySDR::Device::unmake frees it). Any streamer left in
    // channel_states_ now is a bookkeeping leak — stop it and close its stream.
    // One radio at a time: holding every radio's lock at once (100 devices)
    // overflowed ThreadSanitizer's 64-lock deadlock detector, and nothing
    // needs more than one radio here.
    for (const auto& [dev_id, dev_ptr] : devices_)
    {
        std::lock_guard                                         hwlock(hwMutex(dev_id));
        std::lock_guard                                         lock(rt_mu_);
        std::vector<std::pair<RadioDevice*, SoapySDR::Stream*>> closed;
        for (auto it = channel_states_.begin(); it != channel_states_.end();)
        {
            auto& [key, cs] = *it;
            if (cs.device != dev_ptr.get())
            {
                ++it;
                continue;
            }
            spdlog::warn("closeDevices: stream {} still had consumers — stopping it", key);
            if (cs.streamer)
                cs.streamer->stop();
            if (cs.soapy_stream
                && std::find(
                       closed.begin(), closed.end(), std::make_pair(cs.device, cs.soapy_stream))
                       == closed.end())
            {
                cs.device->deactivateStream(cs.soapy_stream);
                cs.device->closeStream(cs.soapy_stream);
                closed.push_back({cs.device, cs.soapy_stream});
            }
            it = channel_states_.erase(it);
        }
    }
    {
        std::lock_guard lock(rt_mu_);
        channel_states_.clear();  // entries without a known device
    }

    for (auto& [id, dev] : devices_)
        dev->close();
}

// ── Device selection ─────────────────────────────────────────────────────
namespace
{
// DDC applies when the device acquires faster than the client asked and the
// ratio is an integer (the DDC only does integer decimation).
bool ddcApplies(double device_sr, double output_sr)
{
    if (output_sr <= 0.0 || std::abs(device_sr - output_sr) <= 1.0)
        return false;
    const double ratio = device_sr / output_sr;
    const double d     = std::round(ratio);
    return d >= 2.0 && std::abs(ratio - d) / d < 1e-3;
}
}  // namespace

std::optional<double> ResourceManager::acquisitionRate(const RadioDevice& dev, double sr) const
{
    const auto& dc   = dev.config();
    const auto& caps = dc.caps;
    if (sr <= 0.0)
        return std::nullopt;
    if (dc.fixed_sample_rate_hz > 0.0)
    {
        // The hardware never changes rate (avoids the AD9361 BB-filter
        // recalibration); the DDC brings it down to what the client asked for
        // — which therefore has to divide the fixed rate (audit RM-27).
        const double fixed = dc.fixed_sample_rate_hz;
        if (sr > fixed + 1.0)
            return std::nullopt;
        const double ratio = fixed / sr;
        const double d     = std::round(ratio);
        if (d < 1.0 || std::abs(ratio - d) / d >= 1e-3)
            return std::nullopt;
        return fixed;
    }
    if (caps.sample_rate_max_sps > 0.0 && sr > caps.sample_rate_max_sps)
        return std::nullopt;
    // Below the device's hardware floor (e.g. RTL-SDR rejects <225,001 Hz)?
    // Acquire at the smallest integer multiple of the requested rate that
    // clears the floor — activateTask then applies Ddc decimation so the
    // client still gets exactly the rate it asked for.
    if (caps.sample_rate_min_sps > 0.0 && sr < caps.sample_rate_min_sps)
    {
        double decim_f = std::ceil(caps.sample_rate_min_sps / sr);
        if (decim_f > 1e6)
            return std::nullopt;  // unreasonably large decimation ratio
        double dev_sr = sr * std::max(1.0, decim_f);
        if (caps.sample_rate_max_sps > 0.0 && dev_sr > caps.sample_rate_max_sps)
            return std::nullopt;
        return dev_sr;
    }
    return sr;
}

bool ResourceManager::deviceCanServe(
    const RadioDevice& dev, double cf, double bw, double sr, int rx, int tx) const
{
    const auto& caps = dev.config().caps;
    if (cf < caps.freq_min_hz || cf > caps.freq_max_hz)
        return false;
    if (bw > caps.bandwidth_max_hz)
        return false;
    if (rx > caps.rx_channels || tx > caps.tx_channels)
        return false;
    return acquisitionRate(dev, sr).has_value();
}

bool ResourceManager::reservePorts(const std::vector<int>& client_ports,
                                   int                     n,
                                   std::vector<int>&       ports,
                                   std::vector<int>&       pool_ports)
{
    ports.clear();
    pool_ports.clear();
    // A client that pre-binds its socket lists the bound ports so the
    // controller can stream to them immediately (no AMQP/UDP race).
    for (int p : client_ports)
    {
        if ((int)ports.size() == n)
            break;
        ports.push_back(p);
    }
    const int need = n - (int)ports.size();
    if (need > 0)
    {
        pool_ports = port_pool_->allocateN(need);
        if ((int)pool_ports.size() < need)
        {
            port_pool_->releaseAll(pool_ports);
            pool_ports.clear();
            return false;
        }
        ports.insert(ports.end(), pool_ports.begin(), pool_ports.end());
    }
    return true;
}

std::optional<ResourceManager::DeviceCandidate>
ResourceManager::findBestDevice(double              cf,
                                double              bw,
                                double              sr,
                                int                 rx,
                                int                 tx,
                                int64_t             t_start,
                                int64_t             t_stop,
                                const std::string&  preferred,
                                int                 preferred_channel,
                                bool                device_required,
                                const FitOptions&   opts,
                                const DeviceFilter& extra_ok,
                                std::string*        why) const
{
    // When device_required=true only the named device is tried; return nullopt
    // immediately if it can't fit rather than falling back to another device.
    // Otherwise: preferred first, then devices.xml order, least-allocated first
    // (stable, so ties keep configuration order — deterministic).
    std::vector<std::string> ordered;
    if (device_required && !preferred.empty())
    {
        if (devices_.count(preferred))
            ordered.push_back(preferred);
    }
    else
    {
        if (!preferred.empty() && devices_.count(preferred))
            ordered.push_back(preferred);
        for (auto& id : device_order_)
            if (id != preferred)
                ordered.push_back(id);
    }
    if (ordered.size() > 1)
    {
        // Snapshot each device's allocated bandwidth once up front: reading it
        // live inside the comparator would violate stable_sort's consistency
        // precondition while other threads insert/remove slots.
        const int64_t                           now = nowMs();
        std::unordered_map<std::string, double> alloc_bw;
        for (auto& id : ordered)
            alloc_bw[id] = timelines_.at(id)->allocatedBw(now);
        std::stable_sort(ordered.begin(),
                         ordered.end(),
                         [&](const std::string& a, const std::string& b)
                         {
                             if (a == preferred || b == preferred)
                                 return a == preferred && b != preferred;
                             return alloc_bw.at(a) < alloc_bw.at(b);
                         });
    }

    for (auto& id : ordered)
    {
        auto& dev = devices_.at(id);
        if (!dev->isOnline())
        {
            if (why && why->empty())
                *why = id + " offline";
            continue;
        }
        const auto& caps = dev->config().caps;
        if (!deviceCanServe(*dev, cf, bw, sr, rx, tx))
            continue;
        if (extra_ok && !extra_ok(*dev))
            continue;
        const double dev_sr = *acquisitionRate(*dev, sr);
        auto         fit    = timelines_.at(id)->canFit(t_start,
                                             t_stop,
                                             cf,
                                             bw,
                                             dev_sr,
                                             rx,
                                             tx,
                                             caps.rx_channels,
                                             caps.tx_channels,
                                             cfg_.policy.guard_band_hz,
                                             dev->config().shared_lo,
                                             preferred_channel,
                                             opts);
        if (fit.ok)
            return DeviceCandidate{dev.get(), std::move(fit)};
        if (why && why->empty())
            *why = id + ": " + fit.reject_reason;
    }
    return std::nullopt;
}

// ── tryAccept entry point ────────────────────────────────────────────────
TaskResponse ResourceManager::tryAccept(const TaskRequest& req, SnapshotResult* snapshot_out)
{
    // Phase 0: type routing.
    if (req.msg_type == "TASK_REQUEST_SNAPSHOT")
        return doAcceptSnapshot(req, snapshot_out);
    // Each handler plans without locks and takes accept_mu_ only to
    // re-validate its choice and insert the reservation (optimistic
    // admission) — holding it for the whole search serialised every request
    // and the 100-device stress test fell from 911 to 562 req/s.
    if (req.msg_type == "TASK_REQUEST_CALIBRATION")
        return doAcceptCalibration(req);
    if (req.msg_type == "TASK_REQUEST_SCAN")
        return doAcceptScan(req);
    return doAcceptStandard(req);
}

// ── Standard fixed-tune tasks (SCHEDULED / IMMEDIATE / CONTINUOUS) ───────
namespace
{
// Stream descriptors describe what the client will actually receive — the
// same CF / rate its packet headers carry. A DDC sub-band is centred on the
// task's slice at its own rate (offset already removed); a raw stream carries
// the whole device window and slice_offset_hz says where the slice sits.
void describeStreams(const TaskRecord::DeviceAllocation&    alloc,
                     const std::string&                     source_ip,
                     const std::function<std::string(int)>& stream_id,
                     std::vector<AssignedStream>&           out)
{
    const double slice_cf = (alloc.slice_lo_hz + alloc.slice_hi_hz) / 2.0;
    const double slice_bw = alloc.slice_hi_hz - alloc.slice_lo_hz;
    const bool   ddc      = ddcApplies(alloc.sample_rate_sps, alloc.output_sample_rate_sps);
    for (size_t i = 0; i < alloc.rx_channels.size(); ++i)
    {
        AssignedStream as;
        as.stream_id     = stream_id(alloc.rx_channels[i]);
        as.device_id     = alloc.device_id;
        as.channel_type  = "RX";
        as.channel_index = alloc.rx_channels[i];
        as.udp_ip        = source_ip;
        as.udp_port      = alloc.udp_ports[i];
        as.slice_bw_hz   = slice_bw;
        if (ddc)
        {
            const double d     = std::round(alloc.sample_rate_sps / alloc.output_sample_rate_sps);
            as.center_freq_hz  = slice_cf;
            as.slice_offset_hz = 0.0;
            as.sample_rate_sps = alloc.sample_rate_sps / d;
        }
        else
        {
            as.center_freq_hz  = alloc.center_freq_hz;
            as.slice_offset_hz = slice_cf - alloc.center_freq_hz;
            as.sample_rate_sps = alloc.sample_rate_sps;
        }
        out.push_back(as);
    }
}

TaskRecord::DeviceAllocation makeAllocation(const RadioDevice& dev,
                                            const FitResult&   fit,
                                            double             requested_sr,
                                            std::vector<int>   ports)
{
    TaskRecord::DeviceAllocation alloc;
    alloc.device_id       = dev.id();
    alloc.rx_channels     = fit.avail_rx;
    alloc.center_freq_hz  = fit.device_cf;
    alloc.sample_rate_sps = fit.device_rate;
    // Non-zero only when the device runs faster than the client asked (combined
    // window, fixed rate, sub-floor) — activateTask then applies the DDC.
    alloc.output_sample_rate_sps =
        (std::abs(fit.device_rate - requested_sr) > 1.0) ? requested_sr : 0.0;
    // The RF band the task receives is always the requested band (ICD §4.3);
    // the packing position (fit.placed_*) is admission bookkeeping only.
    alloc.slice_lo_hz = fit.rf_lo;
    alloc.slice_hi_hz = fit.rf_hi;
    alloc.udp_ports   = std::move(ports);
    return alloc;
}

TimeFreqSlot makeSlot(const std::string&                  task_id,
                      int64_t                             t_start,
                      int64_t                             t_stop,
                      const TaskRecord::DeviceAllocation& alloc,
                      const FitResult&                    fit,
                      bool                                exclusive)
{
    TimeFreqSlot slot;
    slot.task_id         = task_id;
    slot.t_start         = t_start;
    slot.t_stop          = t_stop;
    slot.center_freq_hz  = alloc.center_freq_hz;
    slot.sample_rate_sps = alloc.sample_rate_sps;
    slot.slice_lo_hz     = alloc.slice_lo_hz;
    slot.slice_hi_hz     = alloc.slice_hi_hz;
    slot.budget_lo_hz    = fit.placed_lo;
    slot.budget_hi_hz    = fit.placed_hi;
    slot.rx_channels     = alloc.rx_channels;
    slot.tx_channels     = alloc.tx_channels;
    slot.exclusive       = exclusive;
    return slot;
}
}  // namespace

TaskRecord ResourceManager::newRecord(const TaskRequest&                        req,
                                      const std::string&                        task_id,
                                      int64_t                                   t_start,
                                      int64_t                                   t_stop,
                                      std::vector<TaskRecord::DeviceAllocation> allocs) const
{
    TaskRecord rec;
    rec.task_id        = task_id;
    rec.request_id     = req.request_id;
    rec.correlation_id = req.correlation_id;
    rec.task_type      = req.task_type;
    rec.schedule_mode  = req.schedule_mode;
    rec.priority       = req.priority;
    rec.rank           = req.rank;
    rec.start_time_ms  = t_start;
    rec.stop_time_ms   = t_stop;
    rec.allocations    = std::move(allocs);
    rec.streaming      = req.streaming;
    if (req.scan_params)
        rec.scan_params = req.scan_params;
    if (req.trigger_params)
        rec.trigger_params = req.trigger_params;
    if (req.cal_params)
        rec.cal_params = req.cal_params;
    rec.accepted_at         = steady_clock::now();
    const bool activate_now = (req.schedule_mode == ScheduleMode::IMMEDIATE
                               || req.schedule_mode == ScheduleMode::CONTINUOUS);
    rec.state               = activate_now ? TaskState::PENDING : TaskState::SCHEDULED;
    return rec;
}

void ResourceManager::commitTask(TaskRecord rec, const RfRequest& rf)
{
    const std::string task_id      = rec.task_id;
    const bool        activate_now = (rec.state == TaskState::PENDING);
    {
        std::lock_guard lock(reg_mu_);
        if (!rf.rx_gain_db.empty() || !rf.rx_agc.empty())
            task_gains_[task_id] = GainRequest{rf.rx_gain_db, rf.rx_agc};
        registry_[task_id] = std::move(rec);
    }
    // Run activateTask in a background thread so the proton AMQP thread
    // is not blocked by SoapySDR hardware I/O (openRxStream can take seconds).
    if (activate_now)
    {
        activations_in_flight_.fetch_add(1, std::memory_order_relaxed);
        std::thread(
            [this, task_id]()
            {
                activateTask(task_id);
                activations_in_flight_.fetch_sub(
                    1, std::memory_order_release);  // publishes the thread's accesses to the
                                                    // shutdown wait
            })
            .detach();
    }
    notifyStateChange(task_id);
}

TaskResponse ResourceManager::doAcceptStandard(const TaskRequest& req)
{
    TaskResponse resp;
    resp.request_id     = req.request_id;
    resp.correlation_id = req.correlation_id;
    resp.schedule_mode  = scheduleModeToString(req.schedule_mode);

    // Phase 1: validate against the union of all loaded device capabilities
    {
        auto caps = aggregateCaps(devices_);
        if (req.rf.center_freq_hz < caps.freq_min || req.rf.center_freq_hz > caps.freq_max)
        {
            resp.reject_code   = RejectCode::FREQ_OUT_OF_RANGE;
            resp.reject_reason = "center_freq outside all loaded device ranges";
            return resp;
        }
        if (req.rf.bandwidth_hz <= 0 || req.rf.bandwidth_hz > caps.bw_max)
        {
            resp.reject_code   = RejectCode::BW_EXCEEDED;
            resp.reject_reason = "bandwidth exceeds all loaded device capabilities";
            return resp;
        }
        if (req.rf.sample_rate_sps <= 0 || req.rf.sample_rate_sps > caps.sr_max)
        {
            resp.reject_code   = RejectCode::SAMPLE_RATE_EXCEEDED;
            resp.reject_reason = "sample_rate exceeds all loaded device capabilities";
            return resp;
        }
    }
    if (req.rf.rx_count < 0 || req.rf.tx_count < 0
        || (req.rf.rx_count == 0 && req.rf.tx_count == 0))
    {
        resp.reject_code   = RejectCode::INVALID_REQUEST;
        resp.reject_reason = "Must request ≥1 channel";
        return resp;
    }
    // TX: channels used to be reserved and advertised (with a UDP port) but
    // no TX stream was ever opened — the client's IQ went nowhere (RM-58).
    if (req.rf.tx_count > 0)
    {
        resp.reject_code   = RejectCode::CHANNEL_COUNT_EXCEEDED;
        resp.reject_reason = "TX is not available: this controller has no TX data path "
                             "(tx_count must be 0)";
        return resp;
    }

    // Phase 2: time window
    int64_t t_start = 0, t_stop = 0;
    int64_t now = nowMs();
    switch (req.schedule_mode)
    {
        case ScheduleMode::SCHEDULED:
            t_start = req.start_time_ms;
            t_stop  = req.end_time_ms;
            if (t_start >= t_stop || t_stop <= now)
            {
                resp.reject_code   = RejectCode::INVALID_SCHEDULE;
                resp.reject_reason = "Invalid time window";
                return resp;
            }
            break;
        case ScheduleMode::IMMEDIATE:
            t_start = now;
            t_stop  = (req.duration_ms > 0) ? now + req.duration_ms
                                            : now + cfg_.policy.default_task_timeout_ms;
            break;
        case ScheduleMode::CONTINUOUS:
            t_start = now;
            t_stop  = TIME_INFINITE;
            break;
    }

    // Phase 3: global cap
    if (countByState(TaskState::SCHEDULED) + countByState(TaskState::PENDING)
            + countByState(TaskState::RUNNING)
        >= cfg_.policy.max_concurrent_tasks)
    {
        resp.reject_code   = RejectCode::TASK_LIMIT_REACHED;
        resp.reject_reason = "Global task cap reached";
        return resp;
    }

    // A scan retunes its LO on its own schedule → it must hold the tuning
    // group exclusively (RM-16). A triggered capture reads the stream itself
    // during detection → it can never borrow a stream another task owns (RM-55).
    FitOptions opts;
    const bool is_scan       = req.scan_params.has_value();
    opts.exclusive           = is_scan;
    opts.allow_channel_reuse = !req.trigger_params.has_value();
    DeviceFilter scan_ok;
    if (is_scan)
    {
        // Frequency / bandwidth / rate of every entry; channel counts are
        // checked per device by canFit (a coherent scan spreads rx_count
        // over several devices). A fixed-rate device can only run entries at
        // exactly its rate (the per-entry rate cannot be decimated mid-scan).
        scan_ok = [this, &req](const RadioDevice& d)
        {
            const double fixed = d.config().fixed_sample_rate_hz;
            for (auto& e : req.scan_params->entries)
            {
                if (!deviceCanServe(d, e.center_freq_hz, e.bandwidth_hz, e.sample_rate_sps, 1, 0))
                    return false;
                if (fixed > 0.0 && std::abs(e.sample_rate_sps - fixed) > 1.0)
                    return false;
            }
            return true;
        };
    }

    if (!req.rf.coherency_group.empty())
        return acceptCoherent(req, std::move(resp), t_start, t_stop, opts, scan_ok);

    // ── Single-device path ───────────────────────────────────────────────────
    const int pref_ch = req.rf.preferred_channel;  // -1 = any; ≥0 = specific
    // required_device is a hard constraint — use it as preferred and suppress fallback.
    const bool         dev_req  = !req.rf.required_device.empty();
    const std::string& pref_dev = dev_req ? req.rf.required_device : req.rf.preferred_device;
    const double cf = req.rf.center_freq_hz, bw = req.rf.bandwidth_hz, sr = req.rf.sample_rate_sps;
    std::string  why;

    std::optional<DeviceCandidate> candidate;
    std::vector<int>               ports, pool_ports;
    bool                           have_ports = false;
    std::unique_lock<std::mutex>   admission(accept_mu_, std::defer_lock);
    auto                           rejectWith = [&](RejectCode code, const std::string& reason)
    {
        if (admission.owns_lock())
            admission.unlock();
        port_pool_->releaseAll(pool_ports);
        resp.reject_code   = code;
        resp.reject_reason = reason;
        return resp;
    };
    for (int attempt = 0;; ++attempt)
    {
        // Plan without locks or side effects: direct fit, then a preemption
        // that provably makes room, then a combined window.
        candidate = findBestDevice(cf,
                                   bw,
                                   sr,
                                   req.rf.rx_count,
                                   0,
                                   t_start,
                                   t_stop,
                                   pref_dev,
                                   pref_ch,
                                   dev_req,
                                   opts,
                                   scan_ok,
                                   &why);
        std::optional<PreemptPlan> preempt;
        bool                       try_combine = false;
        if (!candidate && req.rank > 0)
            preempt = planPreemption(req, t_start, t_stop, opts);
        if (!candidate && !preempt && !opts.exclusive && opts.allow_channel_reuse)
            try_combine = planCombined(req, t_start, t_stop).has_value();
        if (!candidate && !preempt && !try_combine)
            return rejectWith(RejectCode::NO_DEVICE_AVAILABLE,
                              "No device satisfies constraints"
                                  + (why.empty() ? "" : " (" + why + ")"));

        // Reserve ports BEFORE any side effect, so a rejection leaves every
        // existing task and the hardware untouched (RM-22).
        if (!have_ports)
            have_ports = reservePorts(req.streaming.dest_ports, req.rf.rx_count, ports, pool_ports);
        if (!have_ports && !preempt)
            return rejectWith(RejectCode::PORT_POOL_EXHAUSTED, "UDP port pool exhausted");

        admission.lock();
        if (candidate)
        {
            // Re-validate on the chosen device: another thread may have taken it.
            candidate = findBestDevice(cf,
                                       bw,
                                       sr,
                                       req.rf.rx_count,
                                       0,
                                       t_start,
                                       t_stop,
                                       candidate->device->id(),
                                       pref_ch,
                                       true,
                                       opts,
                                       scan_ok,
                                       &why);
        }
        else if (preempt)
        {
            applyPreemption(*preempt, req.rank, req.request_id);
            if (!have_ports)  // the victims' ports are free now
                have_ports =
                    reservePorts(req.streaming.dest_ports, req.rf.rx_count, ports, pool_ports);
            if (!have_ports)
                return rejectWith(RejectCode::PORT_POOL_EXHAUSTED, "UDP port pool exhausted");
            candidate = findBestDevice(cf,
                                       bw,
                                       sr,
                                       req.rf.rx_count,
                                       0,
                                       t_start,
                                       t_stop,
                                       preempt->device_id,
                                       pref_ch,
                                       true,
                                       opts,
                                       scan_ok,
                                       &why);
        }
        else
        {
            // Re-plan under the lock (cheap, rare) and apply.
            if (auto combine = planCombined(req, t_start, t_stop))
            {
                if (!applyCombined(*combine, t_start, t_stop))
                    return rejectWith(RejectCode::INTERNAL_ERROR,
                                      "Combined-window retune failed on "
                                          + combine->cand.device->id());
                candidate = combine->cand;
            }
        }
        if (candidate)
            break;  // admission lock still held → insert below
        admission.unlock();
        if (attempt >= 2)
            return rejectWith(RejectCode::NO_DEVICE_AVAILABLE,
                              "No device satisfies constraints (lost a concurrent race)"
                                  + (why.empty() ? "" : " (" + why + ")"));
    }

    std::string task_id  = genUuid();
    resp.task_id         = task_id;
    resp.actual_start_ms = t_start;
    resp.actual_stop_ms  = t_stop;
    resp.accepted        = true;

    auto       alloc     = makeAllocation(*candidate->device, candidate->fit, sr, ports);
    const bool exclusive = is_scan && candidate->device->config().shared_lo;
    timelines_[alloc.device_id]->insert(
        makeSlot(task_id, t_start, t_stop, alloc, candidate->fit, exclusive));
    admission.unlock();
    describeStreams(
        alloc,
        candidate->device->config().streaming_source_ip,
        [&](int ch)
        {
            return makeStreamId(task_id, "RX", alloc.device_id, ch);
        },
        resp.streams);

    auto rec = newRecord(req, task_id, t_start, t_stop, {alloc});
    spdlog::info("ResourceManager: task {} ACCEPTED on {} state={}",
                 task_id,
                 alloc.device_id,
                 taskStateToString(rec.state));
    commitTask(std::move(rec), req.rf);
    return resp;
}

// ── Coherent multi-device path ──────────────────────────────────────────────
// Devices are taken in devices.xml order so stream i always maps to the same
// physical element (DF arrays depend on it); every device is checked against
// the request's frequency / bandwidth / rate, and partially busy devices
// contribute what they still have free (RM-12).
TaskResponse ResourceManager::acceptCoherent(const TaskRequest&  req,
                                             TaskResponse        resp,
                                             int64_t             t_start,
                                             int64_t             t_stop,
                                             const FitOptions&   opts,
                                             const DeviceFilter& extra_ok)
{
    const double cf = req.rf.center_freq_hz, bw = req.rf.bandwidth_hz, sr = req.rf.sample_rate_sps;

    struct Share
    {
        RadioDevice* dev;
        FitResult    fit;
    };

    std::vector<Share> plan;
    int                remaining = req.rf.rx_count, in_group = 0, online = 0;
    std::string        reasons;
    for (auto& id : device_order_)
    {
        if (remaining <= 0)
            break;
        auto& dev = devices_.at(id);
        if (dev->config().coherency_group != req.rf.coherency_group)
            continue;
        ++in_group;
        if (!dev->isOnline())
        {
            reasons += " " + id + ": offline;";
            continue;
        }
        ++online;
        if (!deviceCanServe(*dev, cf, bw, sr, 1, 0) || (extra_ok && !extra_ok(*dev)))
        {
            reasons +=
                " " + id + ": cannot serve " + std::to_string((long long)(cf / 1e6)) + " MHz;";
            continue;
        }
        const double dev_sr = *acquisitionRate(*dev, sr);
        const auto&  caps   = dev->config().caps;
        for (int n = std::min(remaining, caps.rx_channels); n >= 1; --n)
        {
            auto fit = timelines_.at(id)->canFit(t_start,
                                                 t_stop,
                                                 cf,
                                                 bw,
                                                 dev_sr,
                                                 n,
                                                 0,
                                                 caps.rx_channels,
                                                 caps.tx_channels,
                                                 cfg_.policy.guard_band_hz,
                                                 dev->config().shared_lo,
                                                 -1,
                                                 opts);
            if (fit.ok)
            {
                std::sort(fit.avail_rx.begin(), fit.avail_rx.end());
                plan.push_back({dev.get(), std::move(fit)});
                remaining -= n;
                break;
            }
            if (n == 1)
                reasons += " " + id + ": " + fit.reject_reason + ";";
        }
    }
    if (online == 0)
    {
        resp.reject_code = RejectCode::COHERENCY_UNAVAILABLE;
        resp.reject_reason =
            "No online devices in coherency_group '" + req.rf.coherency_group + "'";
        return resp;
    }
    if (remaining > 0)
    {
        resp.reject_code   = RejectCode::COHERENCY_UNAVAILABLE;
        resp.reject_reason = "Coherency group '" + req.rf.coherency_group + "' can provide "
                             + std::to_string(req.rf.rx_count - remaining) + " of "
                             + std::to_string(req.rf.rx_count) + " RX channels ("
                             + std::to_string(in_group) + " devices):" + reasons;
        return resp;
    }

    std::vector<int> ports, pool_ports;
    if (!reservePorts(req.streaming.dest_ports, req.rf.rx_count, ports, pool_ports))
    {
        resp.reject_code   = RejectCode::PORT_POOL_EXHAUSTED;
        resp.reject_reason = "UDP port pool exhausted";
        return resp;
    }

    // Re-validate every share and insert all of them atomically.
    std::unique_lock<std::mutex> admission(accept_mu_);
    for (auto& share : plan)
    {
        const auto& caps = share.dev->config().caps;
        auto        fit  = timelines_.at(share.dev->id())
                       ->canFit(t_start,
                                t_stop,
                                cf,
                                bw,
                                *acquisitionRate(*share.dev, sr),
                                (int)share.fit.avail_rx.size(),
                                0,
                                caps.rx_channels,
                                caps.tx_channels,
                                cfg_.policy.guard_band_hz,
                                share.dev->config().shared_lo,
                                -1,
                                opts);
        if (!fit.ok)
        {
            admission.unlock();
            port_pool_->releaseAll(pool_ports);
            resp.reject_code = RejectCode::COHERENCY_UNAVAILABLE;
            resp.reject_reason =
                "Device " + share.dev->id() + " was taken concurrently: " + fit.reject_reason;
            return resp;
        }
        std::sort(fit.avail_rx.begin(), fit.avail_rx.end());
        share.fit = std::move(fit);
    }

    std::string task_id  = genUuid();
    resp.task_id         = task_id;
    resp.actual_start_ms = t_start;
    resp.actual_stop_ms  = t_stop;
    resp.accepted        = true;

    std::vector<TaskRecord::DeviceAllocation> allocs;
    size_t                                    port_idx = 0;
    std::string                               dev_list;
    for (auto& share : plan)
    {
        const size_t     n = share.fit.avail_rx.size();
        std::vector<int> dev_ports(ports.begin() + (long)port_idx,
                                   ports.begin() + (long)(port_idx + n));
        port_idx += n;
        auto       alloc     = makeAllocation(*share.dev, share.fit, sr, std::move(dev_ports));
        const bool exclusive = opts.exclusive && share.dev->config().shared_lo;
        timelines_[alloc.device_id]->insert(
            makeSlot(task_id, t_start, t_stop, alloc, share.fit, exclusive));
        describeStreams(
            alloc,
            share.dev->config().streaming_source_ip,
            [&](int ch)
            {
                return makeStreamId(task_id, "RX", alloc.device_id, ch);
            },
            resp.streams);
        dev_list += alloc.device_id + " ";
        allocs.push_back(std::move(alloc));
    }
    admission.unlock();

    auto rec = newRecord(req, task_id, t_start, t_stop, std::move(allocs));
    spdlog::info("ResourceManager: task {} ACCEPTED on [{}] state={} (coherent)",
                 task_id,
                 dev_list,
                 taskStateToString(rec.state));
    commitTask(std::move(rec), req.rf);
    return resp;
}

// ── Scan task ────────────────────────────────────────────────────────────
TaskResponse ResourceManager::doAcceptScan(const TaskRequest& req)
{
    TaskResponse resp;
    resp.request_id     = req.request_id;
    resp.correlation_id = req.correlation_id;
    resp.schedule_mode  = scheduleModeToString(req.schedule_mode);

    if (!req.scan_params || req.scan_params->entries.empty())
    {
        resp.reject_code   = RejectCode::SCAN_ENTRY_INVALID;
        resp.reject_reason = "Scan entries list empty";
        return resp;
    }

    // Validate each entry against the union of all loaded device capabilities
    {
        auto caps = aggregateCaps(devices_);
        for (auto& e : req.scan_params->entries)
        {
            if (e.center_freq_hz < caps.freq_min || e.center_freq_hz > caps.freq_max
                || e.bandwidth_hz <= 0 || e.bandwidth_hz > caps.bw_max || e.sample_rate_sps <= 0
                || e.sample_rate_sps > caps.sr_max || e.dwell_ms < 5)
            {
                resp.reject_code   = RejectCode::SCAN_ENTRY_INVALID;
                resp.reject_reason = "Invalid scan entry at step " + std::to_string(e.step);
                return resp;
            }
        }
    }

    // The reservation holds the tuning group exclusively for the scan's
    // lifetime (doAcceptStandard sets FitOptions::exclusive for scans) and the
    // chosen device must be able to serve every entry, not just entry[0].
    const auto& e0           = req.scan_params->entries[0];
    TaskRequest synth        = req;
    synth.rf.center_freq_hz  = e0.center_freq_hz;
    synth.rf.bandwidth_hz    = e0.bandwidth_hz;
    synth.rf.sample_rate_sps = e0.sample_rate_sps;
    synth.msg_type           = "TASK_REQUEST_CONTINUOUS";
    if (req.start_time_ms > 0)
        synth.schedule_mode = ScheduleMode::SCHEDULED;
    else if (req.schedule_mode == ScheduleMode::IMMEDIATE && req.duration_ms > 0)
        synth.schedule_mode = ScheduleMode::IMMEDIATE;  // e.g. SdrClient::scan(..., duration)
    else
        synth.schedule_mode = ScheduleMode::CONTINUOUS;
    return doAcceptStandard(synth);
}

// ── Snapshot task ────────────────────────────────────────────────────────
TaskResponse ResourceManager::doAcceptSnapshot(const TaskRequest& req, SnapshotResult* out)
{
    TaskResponse resp;
    resp.request_id     = req.request_id;
    resp.correlation_id = req.correlation_id;
    resp.schedule_mode  = "IMMEDIATE";
    auto fail           = [&](RejectCode code, const std::string& why)
    {
        resp.accepted      = false;
        resp.reject_code   = code;
        resp.reject_reason = why;
        if (out)
        {
            out->success   = false;
            out->error_msg = why;
        }
        return resp;
    };

    if (!req.snapshot_params)
        return fail(RejectCode::INVALID_REQUEST, "snapshot params missing");
    auto& sp = *req.snapshot_params;
    if (out)
    {
        *out                 = SnapshotResult{};
        out->center_freq_hz  = sp.center_freq_hz;
        out->bandwidth_hz    = sp.bandwidth_hz;
        out->sample_rate_sps = sp.sample_rate_sps;
        out->fft_size        = sp.fft_size;
        out->n_averages      = sp.n_averages;
    }
    // Validate before touching hardware — client-supplied values can overflow int32.
    if (sp.fft_size <= 0 || sp.n_averages <= 0 || sp.sample_rate_sps <= 0
        || (int64_t)sp.fft_size * sp.n_averages > 1 << 24)
        return fail(RejectCode::INVALID_REQUEST, "snapshot fft_size/n_averages out of range");

    const int     total      = sp.fft_size * sp.n_averages;
    const double  expected_s = total / sp.sample_rate_sps;
    const int64_t budget_ms  = std::max<int64_t>(2000, (int64_t)(4000.0 * expected_s) + 1000);

    // Admission: reserve the device for the capture window, so no task can
    // retune it underneath the snapshot (and vice versa).
    RadioDevice*      dev     = nullptr;
    int               snap_ch = 0;
    const std::string slot_id = "snapshot-" + genUuid();
    {
        std::lock_guard admission(accept_mu_);
        FitOptions      opts;
        opts.allow_channel_reuse = false;
        const int64_t now        = nowMs();
        auto          candidate  = findBestDevice(sp.center_freq_hz,
                                        sp.bandwidth_hz,
                                        sp.sample_rate_sps,
                                        1,
                                        0,
                                        now,
                                        now + budget_ms,
                                        sp.preferred_device,
                                        -1,
                                        false,
                                        opts);
        if (!candidate)
            return fail(RejectCode::NO_DEVICE_AVAILABLE, "No device available for snapshot");
        dev        = candidate->device;
        snap_ch    = candidate->fit.avail_rx[0];
        auto alloc = makeAllocation(*dev, candidate->fit, sp.sample_rate_sps, {});
        timelines_[dev->id()]->insert(
            makeSlot(slot_id, now, now + budget_ms, alloc, candidate->fit, false));
    }

    struct SlotGuard
    {
        SpectrumTimeline* tl;
        std::string       id;

        ~SlotGuard()
        {
            tl->remove(id);
        }
    } slot_guard{timelines_.at(dev->id()).get(), slot_id};

    // Serialize tune+open+activate against concurrent activateTask() threads on
    // the same device.  The hardware lock is released before the blocking
    // readStream loop and re-acquired for teardown.
    SoapySDR::Stream* stream    = nullptr;
    double            actual_sr = sp.sample_rate_sps;
    {
        auto hw_lock = lockHw({dev->id()});
        bool tune_ok = dev->config().shared_lo
                           ? dev->tune(sp.center_freq_hz, sp.sample_rate_sps)
                           : dev->tuneChannel(snap_ch, sp.center_freq_hz, sp.sample_rate_sps);
        if (!tune_ok)
            return fail(RejectCode::INTERNAL_ERROR, "Tune failed");
        if (dev->config().shared_lo && dev->currentRate() > 0.0)
            actual_sr = dev->currentRate();  // fixed_sample_rate_hz devices stay put
        stream = dev->openRxStream({snap_ch});
        if (!stream)
            return fail(RejectCode::INTERNAL_ERROR, "Stream open failed");
        if (!dev->activateStream(stream))
        {
            dev->closeStream(stream);
            return fail(RejectCode::INTERNAL_ERROR, "Stream activate failed");
        }
    }  // release hw_lock — readStream must not hold it (blocks for up to 1s)

    std::vector<float> buf((size_t)total * 2);
    void*              bufs[1]   = {buf.data()};
    int                flags     = 0;
    long long          hw_ts     = 0;
    int                collected = 0, err_count = 0, overflows = 0;
    // Bounded: a device that only ever times out used to spin here forever
    // (on a snapshot thread holding a controller thread slot) — RM-19.
    const auto deadline = steady_clock::now() + milliseconds(budget_ms);
    while (collected < total && steady_clock::now() < deadline)
    {
        bufs[0] = buf.data() + (size_t)collected * 2;
        int ret =
            dev->readStream(stream, bufs, (size_t)(total - collected), flags, hw_ts, 200'000LL);
        if (ret > 0)
        {
            collected += ret;
            err_count = 0;
        }
        else if (ret == SOAPY_SDR_OVERFLOW)
        {
            ++overflows;
        }
        else if (ret != SOAPY_SDR_TIMEOUT)
        {
            if (++err_count >= 5)
                break;
        }
    }
    {
        auto hw_lock = lockHw({dev->id()});
        dev->deactivateStream(stream);
        dev->closeStream(stream);
    }
    // Never FFT a partly-filled (zero-padded) buffer and call it a result.
    if (collected < total)
        return fail(RejectCode::INTERNAL_ERROR,
                    "Snapshot capture incomplete: " + std::to_string(collected) + " of "
                        + std::to_string(total) + " samples ("
                        + (err_count >= 5 ? "read errors" : "device stalled / timed out") + ")");

    auto result = fft_engine_->compute(
        buf, sp.fft_size, sp.n_averages, sp.center_freq_hz, actual_sr, sp.bandwidth_hz, dev->id());
    if (overflows > 0)
        spdlog::warn("Snapshot on {}: {} overflow(s) during capture", dev->id(), overflows);
    if (out)
        *out = result;
    // TASK_RESPONSE first, then the SNAPSHOT_RESULT (published by Controller).
    resp.accepted = result.success;
    resp.task_id  = result.success ? slot_id : "";
    if (!result.success)
    {
        resp.reject_code   = RejectCode::INTERNAL_ERROR;
        resp.reject_reason = result.error_msg;
    }
    return resp;
}

// ── Calibration task ─────────────────────────────────────────────────────
TaskResponse ResourceManager::doAcceptCalibration(const TaskRequest& req)
{
    TaskResponse resp;
    resp.request_id     = req.request_id;
    resp.correlation_id = req.correlation_id;
    resp.schedule_mode  = "IMMEDIATE";

    if (!req.cal_params)
    {
        resp.reject_code   = RejectCode::INVALID_REQUEST;
        resp.reject_reason = "calibration params missing";
        return resp;
    }
    auto& cp = *req.cal_params;
    if (cp.duration_ms <= 0 || cp.rx_count_per_device < 1 || cp.sample_rate_sps <= 0)
    {
        resp.reject_code   = RejectCode::INVALID_REQUEST;
        resp.reject_reason = "calibration duration/rx_count_per_device/sample_rate invalid";
        return resp;
    }

    // Explicit device lists come from the client: every id must exist and be
    // online. (An unknown id used to default-insert a null device and crash
    // the controller — RM-20.)
    std::vector<std::string> target_devs;
    if (cp.devices.empty())
    {
        for (auto& id : device_order_)
        {
            auto& dev = devices_.at(id);
            if (dev->config().coherency_group == cp.coherency_group && dev->isOnline())
                target_devs.push_back(id);
        }
    }
    else
    {
        for (auto& id : cp.devices)
        {
            auto it = devices_.find(id);
            if (it == devices_.end())
            {
                resp.reject_code   = RejectCode::INVALID_REQUEST;
                resp.reject_reason = "Unknown device '" + id + "' in calibration request";
                return resp;
            }
            if (!it->second->isOnline())
            {
                resp.reject_code   = RejectCode::DEVICE_OFFLINE;
                resp.reject_reason = "Device '" + id + "' is offline";
                return resp;
            }
            target_devs.push_back(id);
        }
    }
    if (target_devs.empty())
    {
        resp.reject_code   = RejectCode::COHERENCY_UNAVAILABLE;
        resp.reject_reason = "No devices in group " + cp.coherency_group;
        return resp;
    }
    if (countByState(TaskState::SCHEDULED) + countByState(TaskState::PENDING)
            + countByState(TaskState::RUNNING)
        >= cfg_.policy.max_concurrent_tasks)
    {
        resp.reject_code   = RejectCode::TASK_LIMIT_REACHED;
        resp.reject_reason = "Global task cap reached";
        return resp;
    }

    int64_t t_start = nowMs();
    int64_t t_stop  = t_start + cp.duration_ms;

    // Check all devices are free — and use the channels the timeline says are.
    std::vector<std::pair<RadioDevice*, FitResult>> fits;
    for (auto& dev_id : target_devs)
    {
        auto* dev = devices_.at(dev_id).get();
        if (!deviceCanServe(*dev,
                            cp.center_freq_hz,
                            cp.bandwidth_hz,
                            cp.sample_rate_sps,
                            cp.rx_count_per_device,
                            0))
        {
            resp.reject_code = RejectCode::COHERENCY_UNAVAILABLE;
            resp.reject_reason =
                "Device " + dev_id + " cannot serve the calibration frequency/rate/channels";
            return resp;
        }
        const auto& caps = dev->config().caps;
        auto        fit  = timelines_.at(dev_id)->canFit(t_start,
                                                 t_stop,
                                                 cp.center_freq_hz,
                                                 cp.bandwidth_hz,
                                                 *acquisitionRate(*dev, cp.sample_rate_sps),
                                                 cp.rx_count_per_device,
                                                 0,
                                                 caps.rx_channels,
                                                 caps.tx_channels,
                                                 cfg_.policy.guard_band_hz,
                                                 dev->config().shared_lo);
        if (!fit.ok)
        {
            resp.reject_code   = RejectCode::COHERENCY_UNAVAILABLE;
            resp.reject_reason = "Device " + dev_id + " not free: " + fit.reject_reason;
            return resp;
        }
        std::sort(fit.avail_rx.begin(), fit.avail_rx.end());
        fits.push_back({dev, std::move(fit)});
    }

    const int        total_ports = (int)target_devs.size() * cp.rx_count_per_device;
    std::vector<int> ports, pool_ports;
    if (!reservePorts(req.streaming.dest_ports, total_ports, ports, pool_ports))
    {
        resp.reject_code   = RejectCode::PORT_POOL_EXHAUSTED;
        resp.reject_reason = "Port pool exhausted for calibration";
        return resp;
    }

    std::string task_id  = genUuid();
    resp.task_id         = task_id;
    resp.accepted        = true;
    resp.actual_start_ms = t_start;
    resp.actual_stop_ms  = t_stop;

    std::unique_lock<std::mutex> admission(accept_mu_);
    for (auto& [dev, fit] : fits)
    {
        const auto& caps  = dev->config().caps;
        auto        again = timelines_.at(dev->id())->canFit(t_start,
                                                      t_stop,
                                                      cp.center_freq_hz,
                                                      cp.bandwidth_hz,
                                                      *acquisitionRate(*dev, cp.sample_rate_sps),
                                                      cp.rx_count_per_device,
                                                      0,
                                                      caps.rx_channels,
                                                      caps.tx_channels,
                                                      cfg_.policy.guard_band_hz,
                                                      dev->config().shared_lo);
        if (!again.ok)
        {
            admission.unlock();
            port_pool_->releaseAll(pool_ports);
            resp.accepted = false;
            resp.task_id.clear();
            resp.reject_code = RejectCode::COHERENCY_UNAVAILABLE;
            resp.reject_reason =
                "Device " + dev->id() + " was taken concurrently: " + again.reject_reason;
            return resp;
        }
        std::sort(again.avail_rx.begin(), again.avail_rx.end());
        fit = std::move(again);
    }
    std::vector<TaskRecord::DeviceAllocation> allocs;
    size_t                                    port_idx = 0;
    for (auto& [dev, fit] : fits)
    {
        std::vector<int> dev_ports(ports.begin() + (long)port_idx,
                                   ports.begin() + (long)(port_idx + fit.avail_rx.size()));
        port_idx += fit.avail_rx.size();
        auto alloc = makeAllocation(*dev, fit, cp.sample_rate_sps, std::move(dev_ports));
        timelines_[alloc.device_id]->insert(makeSlot(task_id, t_start, t_stop, alloc, fit, false));
        describeStreams(
            alloc,
            dev->config().streaming_source_ip,
            [&](int ch)
            {
                return makeStreamId(task_id, "RX", alloc.device_id, ch);
            },
            resp.streams);
        allocs.push_back(std::move(alloc));
    }
    admission.unlock();

    TaskRequest as_immediate   = req;
    as_immediate.task_type     = TaskType::CALIBRATION;
    as_immediate.schedule_mode = ScheduleMode::IMMEDIATE;
    auto rec = newRecord(as_immediate, task_id, t_start, t_stop, std::move(allocs));
    commitTask(std::move(rec), req.rf);
    return resp;
}

// ── Rank-based preemption ────────────────────────────────────────────────
// Plans which lower-rank tasks to cancel — lowest rank first, only as many as
// a what-if fit proves necessary, only on a device where that actually makes
// room. Nothing is cancelled until the request is otherwise committed (RM-21).
std::optional<ResourceManager::PreemptPlan> ResourceManager::planPreemption(
    const TaskRequest& req, int64_t t_start, int64_t t_stop, const FitOptions& opts) const
{
    const double cf = req.rf.center_freq_hz, bw = req.rf.bandwidth_hz, sr = req.rf.sample_rate_sps;
    const bool   dev_req = !req.rf.required_device.empty();
    for (auto& dev_id : device_order_)
    {
        if (dev_req && dev_id != req.rf.required_device)
            continue;
        auto& dev = devices_.at(dev_id);
        if (!dev->isOnline() || !deviceCanServe(*dev, cf, bw, sr, req.rf.rx_count, 0))
            continue;
        auto& tl          = timelines_.at(dev_id);
        auto  overlapping = tl->slotsOverlapping(t_start, t_stop);
        if (overlapping.empty())
            continue;

        std::vector<std::pair<int, std::string>> lower;  // (rank, task_id)
        {
            std::lock_guard lock(reg_mu_);
            for (auto& slot : overlapping)
            {
                auto it = registry_.find(slot.task_id);
                if (it == registry_.end() || isTerminalState(it->second.state))
                    continue;
                if (it->second.rank < req.rank)
                    lower.push_back({it->second.rank, slot.task_id});
            }
        }
        std::sort(lower.begin(), lower.end());
        lower.erase(std::unique(lower.begin(), lower.end()), lower.end());
        if (lower.empty())
            continue;

        const auto&                     caps   = dev->config().caps;
        const double                    dev_sr = *acquisitionRate(*dev, sr);
        std::unordered_set<std::string> ignore;
        FitOptions                      what_if = opts;
        what_if.ignore                          = &ignore;
        PreemptPlan plan;
        plan.device_id = dev_id;
        for (auto& [rank, tid] : lower)
        {
            ignore.insert(tid);
            plan.victims.push_back(tid);
            auto fit = tl->canFit(t_start,
                                  t_stop,
                                  cf,
                                  bw,
                                  dev_sr,
                                  req.rf.rx_count,
                                  0,
                                  caps.rx_channels,
                                  caps.tx_channels,
                                  cfg_.policy.guard_band_hz,
                                  dev->config().shared_lo,
                                  req.rf.preferred_channel,
                                  what_if);
            if (fit.ok)
                return plan;
        }
    }
    return std::nullopt;
}

void ResourceManager::applyPreemption(const PreemptPlan& plan,
                                      int                rank,
                                      const std::string& request_id)
{
    const std::string reason = std::string(PREEMPT_TERMINAL_REASON)
                               + " rank=" + std::to_string(rank) + " request=" + request_id;
    for (auto& tid : plan.victims)
    {
        spdlog::info("ResourceManager: preempting {} on {} for rank={} request={}",
                     tid,
                     plan.device_id,
                     rank,
                     request_id);
        deactivateTask(tid, TaskState::CANCELLED, reason);
    }
}

// ── Combined-window retune ───────────────────────────────────────────────
// When a new task at a different CF can't fit the current device window,
// try to expand the window to cover both the existing and new slices.
// Planning is side-effect free; applyCombined() retunes the device, pauses the
// running streamers for PLL settle and moves every consumer onto the new window.
std::optional<ResourceManager::CombinePlan>
ResourceManager::planCombined(const TaskRequest& req, int64_t t_start, int64_t t_stop) const
{
    const double guard = cfg_.policy.guard_band_hz;
    const double cf = req.rf.center_freq_hz, bw = req.rf.bandwidth_hz, sr = req.rf.sample_rate_sps;
    const double req_lo = cf - bw / 2.0, req_hi = cf + bw / 2.0;
    const int    pref_ch = req.rf.preferred_channel;
    const bool   dev_req = !req.rf.required_device.empty();

    for (auto& dev_id : device_order_)
    {
        if (dev_req && dev_id != req.rf.required_device)
            continue;
        auto& dev = devices_.at(dev_id);
        if (!dev->isOnline() || !dev->config().shared_lo)
            continue;
        const auto& caps = dev->config().caps;
        if (cf < caps.freq_min_hz || cf > caps.freq_max_hz)
            continue;
        if (bw > caps.bandwidth_max_hz)
            continue;
        if (req.rf.rx_count > caps.rx_channels)
            continue;
        auto& tl = timelines_.at(dev_id);
        if (tl->hasExclusive(t_start, t_stop))
            continue;  // a scan owns this LO
        auto slots = tl->slotsOverlapping(t_start, t_stop);
        if (slots.empty())
            continue;

        // Rank guard: a lower-rank task never moves (or subscribes to) a
        // higher-rank task's hardware.
        {
            bool            rank_ok = true;
            std::lock_guard lock(reg_mu_);
            for (auto& slot : slots)
            {
                auto it = registry_.find(slot.task_id);
                if (it == registry_.end())
                    continue;
                if (!isTerminalState(it->second.state) && req.rank < it->second.rank)
                {
                    rank_ok = false;
                    break;
                }
            }
            if (!rank_ok)
                continue;
        }

        const double cur_cf = slots[0].center_freq_hz, cur_sr = slots[0].sample_rate_sps;
        const double win_lo = cur_cf - cur_sr / 2.0, win_hi = cur_cf + cur_sr / 2.0;
        const bool   same_cf = std::all_of(slots.begin(),
                                         slots.end(),
                                         [&](const TimeFreqSlot& s)
                                         {
                                             return std::abs(s.center_freq_hz - cf) < 1.0;
                                         });
        // Same CF without a channel preference: canFit already decided (slice
        // budget full) — don't bypass that admission check.
        if (same_cf && pref_ch < 0)
            continue;

        std::vector<int> chans;  // channels to multicast from
        for (auto& s : slots)
            for (int c : s.rx_channels)
                if (std::find(chans.begin(), chans.end(), c) == chans.end())
                    chans.push_back(c);
        if (pref_ch >= 0)
        {
            if (std::find(chans.begin(), chans.end(), pref_ch) == chans.end())
                continue;
            chans = {pref_ch};
        }
        if ((int)chans.size() < req.rf.rx_count)
            continue;
        chans.resize((size_t)req.rf.rx_count);

        CombinePlan plan;
        plan.cand.device        = dev.get();
        plan.cand.fit.ok        = true;
        plan.cand.fit.placed_lo = plan.cand.fit.rf_lo = req_lo;
        plan.cand.fit.placed_hi = plan.cand.fit.rf_hi = req_hi;
        plan.cand.fit.avail_rx                        = chans;
        plan.old_cf                                   = cur_cf;
        plan.old_sr                                   = cur_sr;

        if (req_lo >= win_lo && req_hi <= win_hi)
        {
            // Already inside the live window: subscribe, no hardware change.
            // (Retuning to a "combined" window here used to *narrow* the
            // window and change existing clients' sample rate.)
            plan.cand.fit.device_cf   = cur_cf;
            plan.cand.fit.device_rate = cur_sr;
            spdlog::info(
                "ResourceManager: subscribe on {} cf={:.3f}MHz sr={:.3f}MSPS (no hardware change)",
                dev_id,
                cur_cf / 1e6,
                cur_sr / 1e6);
            return plan;
        }
        if (dev->config().fixed_sample_rate_hz > 0.0)
            continue;  // rate is pinned; cannot widen

        auto cr = tl->canCombine(t_start,
                                 t_stop,
                                 cf,
                                 bw,
                                 req.rf.rx_count,
                                 caps.rx_channels,
                                 guard,
                                 caps.sample_rate_max_sps,
                                 true /* multicast */);
        if (!cr.ok)
        {
            spdlog::debug("planCombined: {} not combinable: {}", dev_id, cr.reject_reason);
            continue;
        }
        // Window rate = k × current rate: every existing consumer keeps its
        // exact rate through an integer DDC (RM-23). Prefer a k the new task
        // can also be decimated from; otherwise it gets the raw window.
        const int k_min  = std::max(2, (int)std::ceil(cr.combined_sr / cur_sr - 1e-9));
        double    chosen = 0.0;
        for (int k = k_min; k <= k_min + 16 && k * cur_sr <= caps.sample_rate_max_sps + 1.0; ++k)
        {
            const double cand_sr = k * cur_sr;
            if (chosen == 0.0)
                chosen = cand_sr;
            if (ddcApplies(cand_sr, sr))
            {
                chosen = cand_sr;
                break;
            }
        }
        if (chosen == 0.0)
            continue;
        plan.cand.fit.device_cf   = cr.combined_cf;
        plan.cand.fit.device_rate = chosen;
        plan.hw_changed           = true;
        return plan;
    }
    return std::nullopt;
}

bool ResourceManager::applyCombined(const CombinePlan& plan, int64_t t_start, int64_t t_stop)
{
    if (!plan.hw_changed)
        return true;
    RadioDevice*      dev    = plan.cand.device;
    const std::string dev_id = dev->id();
    auto&             tl     = timelines_.at(dev_id);
    const double      new_cf = plan.cand.fit.device_cf, new_sr = plan.cand.fit.device_rate;
    // Settle window: drain readStream calls around the retune (per read, not
    // per sample). Shared with the scan path.
    const int settle = dev->config().retune_settle_reads;

    auto slots = tl->slotsOverlapping(t_start, t_stop);
    // Touch the radio only if a task in this window is live right now. A
    // combine between future reservations used to retune the device at
    // admission time, under whatever was running (RM-64); those tasks are
    // tuned to the combined window when they activate.
    bool live = false;
    {
        std::lock_guard lock(rt_mu_);
        for (auto& slot : slots)
            if (runtimes_.count(slot.task_id))
            {
                live = true;
                break;
            }
    }
    {
        // Serialize the tune + timeline update against concurrent activateTask()
        // threads on this radio. Released before rt_mu_ (lock order: hw → rt_mu_).
        auto hw_lock = lockHw({dev_id});
        if (live && !dev->tune(new_cf, new_sr))
        {
            spdlog::warn("applyCombined: tune failed on {}", dev_id);
            return false;
        }
        // Only the slots sharing this window — future reservations elsewhere
        // keep their own CF/SR (RM-63).
        tl->updateDeviceTune(new_cf, new_sr, t_start, t_stop);
    }

    struct Raw
    {
        std::string                   task_id;
        std::shared_ptr<IQStreamer>   st;
        IQStreamer::Clock::time_point epoch;
    };

    std::vector<std::shared_ptr<IQStreamer>> streamers;
    std::vector<Raw>                         raw;
    {
        std::lock_guard lock(rt_mu_);
        for (auto& slot : slots)
        {
            auto it = runtimes_.find(slot.task_id);
            if (it == runtimes_.end())
                continue;
            for (auto& st : it->second.streamers)
                if (std::find(streamers.begin(), streamers.end(), st) == streamers.end())
                    streamers.push_back(st);
            if (!it->second.is_subband_consumer && !it->second.streamers.empty())
                raw.push_back({slot.task_id, it->second.streamers[0], it->second.epoch});
        }
    }
    // Pause, rebuild existing DDC sub-bands for the new window (they used to
    // keep their stale offset/decimation — RM-24), then push the new CF/SR.
    for (auto& st : streamers)
    {
        st->pauseForRetune(settle);
        st->retuneSubBands(new_cf, new_sr);
        st->updateCenterFreq(new_cf);
        st->updateSampleRate(new_sr);
    }

    // Convert raw consumers into DDC sub-bands at their original rate, so they
    // keep seeing only their own band at the rate they asked for.
    std::vector<std::string> to_notify;
    {
        std::lock_guard lk_reg(reg_mu_);
        for (auto& r : raw)
        {
            auto it = registry_.find(r.task_id);
            if (it == registry_.end() || isTerminalState(it->second.state))
                continue;
            for (auto& alloc : it->second.allocations)
            {
                if (alloc.device_id != dev_id || alloc.rx_channels.empty())
                    continue;
                const double out_sr =
                    alloc.output_sample_rate_sps > 0.0 ? alloc.output_sample_rate_sps : plan.old_sr;
                const double task_cf = (alloc.slice_lo_hz + alloc.slice_hi_hz) / 2.0;
                r.st->removeDest(r.task_id);
                for (size_t i = 0; i < alloc.rx_channels.size() && i < alloc.udp_ports.size(); ++i)
                    r.st->addSubBand(
                        r.task_id,
                        makeStreamId(r.task_id, "RX", alloc.device_id, alloc.rx_channels[i]),
                        alloc.rx_channels[i],
                        it->second.streaming.dest_ip,
                        alloc.udp_ports[i],
                        task_cf,
                        out_sr,
                        new_sr,
                        r.epoch);
                alloc.output_sample_rate_sps = out_sr;
                spdlog::info("applyCombined: task {} → DDC sub-band cf={:.3f}MHz sr={:.3f}MSPS",
                             r.task_id,
                             task_cf / 1e6,
                             out_sr / 1e6);
            }
        }
        // Every task sharing the window now runs on the new CF/SR (scheduled
        // ones included — they will be decimated back to their own rate).
        for (auto& slot : slots)
        {
            auto it = registry_.find(slot.task_id);
            if (it == registry_.end())
                continue;
            for (auto& alloc : it->second.allocations)
            {
                if (alloc.device_id != dev_id)
                    continue;
                if (alloc.output_sample_rate_sps <= 0.0)
                    alloc.output_sample_rate_sps = alloc.sample_rate_sps;
                alloc.center_freq_hz  = new_cf;
                alloc.sample_rate_sps = new_sr;
            }
            to_notify.push_back(slot.task_id);
        }
    }
    {
        std::lock_guard lk_rt(rt_mu_);
        for (auto& r : raw)
        {
            auto it = runtimes_.find(r.task_id);
            if (it != runtimes_.end())
                it->second.is_subband_consumer = true;
        }
    }
    for (auto& tid : to_notify)
        notifyStateChange(tid);
    spdlog::info("ResourceManager: combined retune on {} → cf={:.3f}MHz sr={:.3f}MSPS "
                 "(covering {} existing task(s))",
                 dev_id,
                 new_cf / 1e6,
                 new_sr / 1e6,
                 (int)slots.size());
    return true;
}

// ── Stop / Cancel ────────────────────────────────────────────────────────
TaskResponse ResourceManager::stopTask(const std::string& task_id,
                                       const std::string& req_id,
                                       const std::string& reason)
{
    TaskResponse resp;
    resp.request_id = req_id;

    {
        std::lock_guard lock(reg_mu_);
        auto            it = registry_.find(task_id);
        if (it == registry_.end())
        {
            resp.reject_code = RejectCode::TASK_NOT_FOUND;
            return resp;
        }
        auto& rec = it->second;
        if (isTerminalState(rec.state))
        {
            resp.reject_code = RejectCode::TASK_ALREADY_TERMINAL;
            return resp;
        }
        if (rec.schedule_mode == ScheduleMode::SCHEDULED && rec.task_type != TaskType::SCAN
            && rec.task_type != TaskType::TRIGGERED)
        {
            resp.reject_code = RejectCode::TASK_NOT_STOPPABLE;
            return resp;
        }
    }
    deactivateTask(task_id, TaskState::CANCELLED, reason);
    resp.accepted = true;
    resp.task_id  = task_id;
    return resp;
}

TaskResponse ResourceManager::cancelTask(const std::string& task_id,
                                         const std::string& req_id,
                                         const std::string& reason)
{
    TaskResponse resp;
    resp.request_id = req_id;
    {
        std::lock_guard lock(reg_mu_);
        if (!registry_.count(task_id))
        {
            resp.reject_code = RejectCode::TASK_NOT_FOUND;
            return resp;
        }
        if (isTerminalState(registry_[task_id].state))
        {
            resp.reject_code = RejectCode::TASK_ALREADY_TERMINAL;
            return resp;
        }
    }
    deactivateTask(task_id, TaskState::CANCELLED, reason);
    resp.accepted = true;
    resp.task_id  = task_id;
    return resp;
}

// ── Activate a task (move SCHEDULED/PENDING → RUNNING) ───────────────────
void ResourceManager::activateTask(const std::string& task_id)
{
    TaskRecord  rec;
    GainRequest gains;
    {
        std::lock_guard lock(reg_mu_);
        auto            it = registry_.find(task_id);
        if (it == registry_.end())
            return;
        if (isTerminalState(it->second.state))
            return;
        rec              = it->second;
        it->second.state = TaskState::PENDING;
        if (auto g = task_gains_.find(task_id); g != task_gains_.end())
            gains = g->second;
    }

    if (rec.allocations.empty())
    {
        spdlog::error("activateTask [{}]: no allocations", task_id);
        return;
    }

    // Serialize hardware open calls (tune/openRxStream/activateStream) across
    // background threads. Released once per-task hardware setup completes;
    // NOT held for the task's RUNNING lifetime.
    // Only this task's radios: others set up in parallel (RM-75).
    HwLocks hw_lock;
    {
        std::vector<std::string> ids;
        for (const auto& a : rec.allocations)
            ids.push_back(a.device_id);
        hw_lock = lockHw(std::move(ids));
    }

    // Check state again after acquiring hw_lock: task may have been cancelled while waiting.
    {
        std::lock_guard lock(reg_mu_);
        auto            it = registry_.find(task_id);
        if (it == registry_.end() || isTerminalState(it->second.state))
            return;
    }

    TaskRuntime rt;
    rt.device = nullptr;  // set to first device below (used for scan/trigger)
    // One epoch for every stream of the task: timestamp_ns is "ns from task
    // start" (ICD §10.1) and must be comparable across boards (RM-30).
    rt.epoch = IQStreamer::Clock::now();

    auto task_err_cb = [this](const std::string& tid, const std::string& err)
    {
        InFlight guard(activations_in_flight_);  // streamer thread; see scan_done
        spdlog::error("IQStreamer error task={}: {}", tid, err);
        deactivateTask(tid, TaskState::FAILED, err);
    };
    const auto& streaming = rec.streaming;

    // A failure part-way through used to return without registering the
    // runtime: streams already set up on earlier devices kept running (still
    // sending UDP, holding the hardware) and the device was later closed under
    // their worker thread (use-after-free). Register what exists and let
    // deactivateTask tear it down (RM-61).
    auto fail = [&](const std::string& why)
    {
        {
            std::lock_guard lk(rt_mu_);
            runtimes_[task_id] = std::move(rt);
        }
        hw_lock.clear();  // unlock
        deactivateTask(task_id, TaskState::FAILED, why);
    };

    size_t gain_idx  = 0;  // per-request gains apply in allocation/channel order
    auto   applyGain = [&](RadioDevice* dev, int ch)
    {
        const auto& dc  = dev->config();
        double      g   = gain_idx < gains.gain_db.size() ? gains.gain_db[gain_idx] : dc.rx_gain_db;
        bool        agc = gain_idx < gains.agc.size() ? (bool)gains.agc[gain_idx] : dc.rx_agc;
        ++gain_idx;
        g = std::clamp(g, dc.caps.rx_gain_min_db, dc.caps.rx_gain_max_db);
        dev->setRxGain(ch, g, agc);
    };

    std::vector<std::shared_ptr<IQStreamer>>                to_start;  // started together (RM-30)
    std::vector<std::pair<RadioDevice*, SoapySDR::Stream*>> to_activate;
    auto                                                    addUnique = [](auto& v, const auto& x)
    {
        if (std::find(v.begin(), v.end(), x) == v.end())
            v.push_back(x);
    };

    // Set up every allocation — one per device for coherent multi-board tasks.
    for (auto& alloc : rec.allocations)
    {
        auto dev_it = devices_.find(alloc.device_id);
        if (dev_it == devices_.end())
        {
            fail("Device not found: " + alloc.device_id);
            return;
        }
        RadioDevice* dev = dev_it->second.get();
        if (!rt.device)
            rt.device = dev;  // primary device (scan/trigger uses first)

        const bool   shared_lo = dev->config().shared_lo;
        const double out_sr    = alloc.output_sample_rate_sps > 0.0 ? alloc.output_sample_rate_sps
                                                                    : alloc.sample_rate_sps;
        const double task_cf   = (alloc.slice_lo_hz + alloc.slice_hi_hz) / 2.0;
        auto         sid       = [&](int ch)
        {
            return makeStreamId(task_id, "RX", alloc.device_id, ch);
        };
        auto port = [&](size_t i)
        {
            return alloc.udp_ports[i];
        };

        if (shared_lo)
        {
            // All requested channels already live (combined-window multicast /
            // fan-out)? Subscribe to the existing IQStreamer instead of opening hardware.
            std::vector<std::shared_ptr<IQStreamer>>                borrowed;
            std::vector<std::pair<RadioDevice*, SoapySDR::Stream*>> borrowed_hw;
            {
                std::lock_guard lk(rt_mu_);
                for (int ch : alloc.rx_channels)
                {
                    auto it = channel_states_.find(alloc.device_id + ":" + std::to_string(ch));
                    if (it != channel_states_.end())
                    {
                        borrowed.push_back(it->second.streamer);
                        borrowed_hw.push_back({it->second.device, it->second.soapy_stream});
                    }
                }
            }
            if (!borrowed.empty() && borrowed.size() == alloc.rx_channels.size())
            {
                if (rec.trigger_params)
                {
                    fail("triggered task cannot share a live stream");
                    return;
                }
                const double wideband_sr = borrowed[0]->currentSR();
                const bool   needs_ddc   = ddcApplies(wideband_sr, out_sr);
                for (size_t i = 0; i < alloc.rx_channels.size(); ++i)
                {
                    const int ch = alloc.rx_channels[i];
                    if (needs_ddc)
                        borrowed[i]->addSubBand(task_id,
                                                sid(ch),
                                                ch,
                                                streaming.dest_ip,
                                                port(i),
                                                task_cf,
                                                out_sr,
                                                wideband_sr,
                                                rt.epoch);
                    else  // raw fan-out of *this* channel (it used to be channel 0 for all)
                        borrowed[i]->addDest(
                            task_id, sid(ch), streaming.dest_ip, port(i), rt.epoch, ch);
                    addUnique(rt.streamers, borrowed[i]);
                    addUnique(rt.soapy_streams, borrowed_hw[i]);
                }
                if (needs_ddc)
                    rt.is_subband_consumer = true;
                if (!gains.gain_db.empty())
                    spdlog::info(
                        "activateTask [{}]: shares a live stream on {} — gain stays with the "
                        "stream owner",
                        task_id,
                        alloc.device_id);
                spdlog::info("activateTask [{}] {} shared channel(s) on {}",
                             task_id,
                             needs_ddc ? "DDC sub-band on" : "subscribed to",
                             alloc.device_id);
                continue;
            }

            // New channel(s) — tune device, open ONE stream for all channels.
            if (!dev->tune(alloc.center_freq_hz, alloc.sample_rate_sps))
            {
                fail("Tune failed on " + alloc.device_id);
                return;
            }
            for (int ch : alloc.rx_channels)
                applyGain(dev, ch);
            SoapySDR::Stream* s = dev->openRxStream(alloc.rx_channels);
            if (!s)
            {
                fail("openRxStream failed on " + alloc.device_id);
                return;
            }
            rt.soapy_streams.push_back({dev, s});
            to_activate.push_back({dev, s});

            const bool ddc = ddcApplies(alloc.sample_rate_sps, alloc.output_sample_rate_sps);
            IQStreamer::Config sc;
            sc.task_id       = task_id;
            sc.stream_id     = sid(alloc.rx_channels[0]);
            sc.channel_index = alloc.rx_channels[0];
            if (!ddc)
            {
                sc.dest_ip   = streaming.dest_ip;
                sc.dest_port = port(0);
            }
            sc.packet_samples = cfg_.policy.iq_packet_samples;
            sc.task_start_ms  = rec.start_time_ms;
            sc.sample_rate    = alloc.sample_rate_sps;
            sc.task_epoch     = rt.epoch;
            for (size_t i = 1; i < alloc.rx_channels.size(); ++i)
            {
                IQStreamer::ChannelDest ec;
                ec.channel_index = alloc.rx_channels[i];
                ec.stream_id     = sid(alloc.rx_channels[i]);
                if (!ddc)
                {
                    ec.dest_ip   = streaming.dest_ip;
                    ec.dest_port = port(i);
                }
                sc.extra_channels.push_back(ec);
            }
            auto streamer = std::make_shared<IQStreamer>(sc, dev->soapyDevice(), s, task_err_cb);
            streamer->updateCenterFreq(alloc.center_freq_hz);
            if (ddc)
            {  // device runs faster than asked (fixed rate / sub-floor): decimate
                for (size_t i = 0; i < alloc.rx_channels.size(); ++i)
                    streamer->addSubBand(task_id,
                                         sid(alloc.rx_channels[i]),
                                         alloc.rx_channels[i],
                                         streaming.dest_ip,
                                         port(i),
                                         task_cf,
                                         out_sr,
                                         alloc.sample_rate_sps,
                                         rt.epoch);
                rt.is_subband_consumer = true;
            }
            rt.streamers.push_back(streamer);
            to_start.push_back(streamer);
            {
                std::lock_guard lk(rt_mu_);
                for (int ch : alloc.rx_channels)
                    channel_states_[alloc.device_id + ":" + std::to_string(ch)] = {
                        dev, s, streamer};
            }
        }
        else
        {
            // Each channel has its own LO and its own SoapySDR stream. The device
            // acquires at alloc.sample_rate_sps (fixed rate / hardware floor);
            // when that differs from the request, deliver through the DDC.
            const bool ddc = ddcApplies(alloc.sample_rate_sps, alloc.output_sample_rate_sps);
            for (size_t i = 0; i < alloc.rx_channels.size(); ++i)
            {
                const int ch = alloc.rx_channels[i];
                if (!dev->tuneChannel(ch, alloc.center_freq_hz, alloc.sample_rate_sps))
                {
                    fail("tuneChannel failed ch=" + std::to_string(ch));
                    return;
                }
                applyGain(dev, ch);
                SoapySDR::Stream* s = dev->openRxStream({ch});
                if (!s)
                {
                    fail("openRxStream failed ch=" + std::to_string(ch));
                    return;
                }
                rt.soapy_streams.push_back({dev, s});
                to_activate.push_back({dev, s});

                IQStreamer::Config sc;
                sc.task_id       = task_id;
                sc.stream_id     = sid(ch);
                sc.channel_index = ch;
                if (!ddc)
                {
                    sc.dest_ip   = streaming.dest_ip;
                    sc.dest_port = port(i);
                }
                sc.packet_samples = cfg_.policy.iq_packet_samples;
                sc.task_start_ms  = rec.start_time_ms;
                sc.sample_rate    = alloc.sample_rate_sps;
                sc.task_epoch     = rt.epoch;
                auto streamer =
                    std::make_shared<IQStreamer>(sc, dev->soapyDevice(), s, task_err_cb);
                streamer->updateCenterFreq(alloc.center_freq_hz);
                if (ddc)
                {
                    streamer->addSubBand(task_id,
                                         sid(ch),
                                         ch,
                                         streaming.dest_ip,
                                         port(i),
                                         task_cf,
                                         out_sr,
                                         alloc.sample_rate_sps,
                                         rt.epoch);
                    rt.is_subband_consumer = true;
                }
                rt.streamers.push_back(streamer);
                to_start.push_back(streamer);
            }
        }
    }

    // Activate every new stream only now, back to back, so the boards of a
    // coherent task start together instead of one activation apart.
    for (auto& [d, s] : to_activate)
    {
        if (!d->activateStream(s))
        {
            fail("activateStream failed on " + d->id());
            return;
        }
    }
    hw_lock.clear();  // unlock

    const auto&  alloc0 = rec.allocations[0];
    RadioDevice* dev0   = rt.device;

    if (rec.scan_params && dev0)
    {
        std::vector<IQStreamer*> ptrs;
        for (auto& s : rt.streamers)
            ptrs.push_back(s.get());

        // Retune every device of the task — a coherent scan used to move only
        // the first board while all boards' headers advertised the new CF.
        struct Target
        {
            RadioDevice*     dev;
            bool             shared;
            std::vector<int> chans;
        };

        std::vector<Target> targets;
        for (auto& a : rec.allocations)
            if (auto it = devices_.find(a.device_id); it != devices_.end())
                targets.push_back(
                    {it->second.get(), it->second->config().shared_lo, a.rx_channels});
        ScanExecutor::RetuneFn retune_fn = [targets](double cf, double sr)
        {
            for (auto& t : targets)
            {
                if (t.shared)
                {
                    if (!t.dev->tune(cf, sr))
                        return false;
                }
                else
                    for (int ch : t.chans)
                        if (!t.dev->tuneChannel(ch, cf, sr))
                            return false;
            }
            return true;
        };
        // Runs on the scan thread itself, which nobody joins (it detaches
        // when deactivateTask stops it from inside): counted like an
        // activation so closeDevices() / the destructor wait for it. The
        // RM was destroyed under a finishing scan's deactivateTask →
        // notifyStateChange (ASan heap-use-after-free on the registry).
        auto scan_done = [this](const std::string& tid, bool ok)
        {
            InFlight guard(activations_in_flight_);
            deactivateTask(tid,
                           ok ? TaskState::COMPLETED : TaskState::FAILED,
                           ok ? "Scan complete" : "Scan error");
        };
        int settle = 0;
        for (auto& t : targets)
            settle = std::max(settle, t.dev->config().retune_settle_reads);
        rt.scan_exec = std::make_unique<ScanExecutor>(
            task_id, *rec.scan_params, std::move(retune_fn), ptrs, scan_done, settle);
    }

    if (rec.trigger_params && !rt.streamers.empty() && !rt.soapy_streams.empty() && dev0)
    {
        auto trig_done = [this](const std::string& tid, bool ok)
        {
            InFlight guard(activations_in_flight_);  // see scan_done
            deactivateTask(tid,
                           ok ? TaskState::COMPLETED : TaskState::FAILED,
                           ok ? "Trigger done" : "Trigger error");
        };
        // The monitor reads the first stream; on trigger it starts every
        // stream of the task (all boards of a coherent capture).
        std::vector<IQStreamer*> also;
        for (size_t i = 1; i < rt.streamers.size(); ++i)
            also.push_back(rt.streamers[i].get());
        rt.trig_mon = std::make_unique<TriggerMonitor>(task_id,
                                                       *rec.trigger_params,
                                                       dev0->soapyDevice(),
                                                       rt.soapy_streams[0].second,
                                                       rt.streamers[0].get(),
                                                       alloc0.sample_rate_sps,
                                                       trig_done,
                                                       also);
    }

    if (!rec.trigger_params)
        for (auto& st : to_start)
            st->start();
    if (rt.scan_exec)
        rt.scan_exec->start();
    if (rt.trig_mon)
        rt.trig_mon->start();

    {
        std::lock_guard lock(rt_mu_);
        runtimes_[task_id] = std::move(rt);
    }

    // Final state transition: PENDING → RUNNING.
    bool    retimed   = false;
    int64_t new_start = 0, new_stop = 0;
    {
        std::lock_guard lock(reg_mu_);
        if (registry_.count(task_id))
        {
            auto& r = registry_[task_id];
            if (!isTerminalState(r.state))
            {
                // For IMMEDIATE tasks the stop_time was computed at acceptance time.
                // If hw activation was delayed the deadline may already be past.
                // Reset it so the task gets its full intended duration from the
                // moment hardware is actually running.
                if (r.schedule_mode == ScheduleMode::IMMEDIATE && r.stop_time_ms != TIME_INFINITE)
                {
                    int64_t duration_ms = r.stop_time_ms - r.start_time_ms;
                    int64_t now2        = nowMs();
                    r.start_time_ms     = now2;
                    r.stop_time_ms      = now2 + duration_ms;
                    retimed             = true;
                    new_start           = r.start_time_ms;
                    new_stop            = r.stop_time_ms;
                }
                r.state = TaskState::RUNNING;
            }
            // If state is already terminal (task cancelled during hw setup),
            // do NOT overwrite it.
        }
    }
    // A stop / cancel / scan-done that ran while the hardware was being set
    // up found no runtime and returned; the runtime inserted above would then
    // keep streaming and holding its channels forever (RM-68). deactivateTask
    // on an already-terminal task tears down exactly that orphan.
    {
        TaskState   st = TaskState::RUNNING;
        std::string why;
        bool        orphan = false;
        {
            std::lock_guard lock(reg_mu_);
            if (auto it = registry_.find(task_id);
                it != registry_.end() && isTerminalState(it->second.state))
            {
                orphan = true;
                st     = it->second.state;
                why    = it->second.terminal_reason;
            }
        }
        if (orphan)
        {
            spdlog::info("activateTask [{}] ended during hardware setup ({}) — releasing it",
                         task_id,
                         taskStateToString(st));
            deactivateTask(task_id, st, why);
            return;
        }
    }
    // Keep the reservation in step with the registry (it used to keep the
    // acceptance-time window, leaving a gap other tasks could be booked into).
    if (retimed)
        for (auto& a : rec.allocations)
            if (timelines_.count(a.device_id))
                timelines_.at(a.device_id)->retime(task_id, new_start, new_stop);

    std::string dev_list;
    for (auto& a : rec.allocations)
        dev_list += a.device_id + " ";
    spdlog::info("activateTask [{}] RUNNING on [{}]", task_id, dev_list);
    notifyStateChange(task_id);
}

// ── Deactivate ───────────────────────────────────────────────────────────
void ResourceManager::deactivateTask(const std::string& task_id,
                                     TaskState          terminal_state,
                                     const std::string& reason)
{
    // Atomically claim deactivation rights under reg_mu_.
    // If two threads race (e.g. stopTask() vs a background IQStreamer done-callback),
    // only the first one sets the state.  A second concurrent call finds state
    // already terminal but may still need to clean up an orphaned runtime (if
    // the first call ran before activateTask() inserted into runtimes_).
    bool                     already_terminal = false;
    std::vector<std::string> hw_ids;  // radios whose hardware locks teardown takes
    {
        std::lock_guard lock(reg_mu_);
        auto            it = registry_.find(task_id);
        if (it == registry_.end())
            return;
        for (const auto& al : it->second.allocations)
            hw_ids.push_back(al.device_id);
        if (isTerminalState(it->second.state))
        {
            already_terminal = true;  // state set by a prior deactivateTask call
        }
        else
        {
            it->second.state           = terminal_state;
            it->second.terminal_reason = reason;
            terminal_since_[task_id]   = steady_clock::now();
            task_gains_.erase(task_id);
        }
    }
    // If already terminal, only proceed if there is an orphaned runtime to clean.
    if (already_terminal)
    {
        std::lock_guard lock(rt_mu_);
        if (!runtimes_.count(task_id))
            return;  // nothing orphaned
        // Fall through — there's an orphaned runtime inserted by activateTask
        // AFTER our previous deactivateTask cleaned up (found empty runtimes_).
    }

    spdlog::info(
        "deactivateTask [{}] → {} reason={}", task_id, taskStateToString(terminal_state), reason);

    // Stop runtime objects.
    // IMPORTANT: scan_exec/trig_mon are moved out of the runtime and stopped
    // AFTER releasing rt_mu_.  Their done-callbacks call deactivateTask which
    // also acquires rt_mu_; stopping them while holding rt_mu_ would deadlock.
    std::unique_ptr<ScanExecutor>   scan_exec_to_stop;
    std::unique_ptr<TriggerMonitor> trig_mon_to_stop;
    // Stop the executors FIRST. The scan thread calls updateCenterFreq() on the
    // task's IQStreamers and the trigger monitor reads the SoapySDR stream —
    // tearing those down while either still ran was a use-after-free (found
    // by ThreadSanitizer, RM-66). Joined outside rt_mu_: their done-callbacks
    // call deactivateTask(), which takes rt_mu_ (they self-detach if this IS
    // their thread).
    {
        std::lock_guard lock(rt_mu_);
        if (auto it = runtimes_.find(task_id); it != runtimes_.end())
        {
            scan_exec_to_stop = std::move(it->second.scan_exec);
            trig_mon_to_stop  = std::move(it->second.trig_mon);
        }
    }
    if (scan_exec_to_stop)
        scan_exec_to_stop->stop();
    if (trig_mon_to_stop)
        trig_mon_to_stop->stop();
    // Destroyed after the locks are released (an IQStreamer joins its worker).
    std::vector<std::shared_ptr<IQStreamer>> keep_alive;
    {
        if (hw_ids.empty())
        {  // no allocations on record: the radios the runtime actually uses
            std::lock_guard lock(rt_mu_);
            if (auto it = runtimes_.find(task_id); it != runtimes_.end())
                for (const auto& [d, st] : it->second.soapy_streams)
                    if (d)
                        hw_ids.push_back(d->id());
        }
        // The task's hardware locks before rt_mu_ (matching activateTask's
        // lock ordering) — deactivateStream/closeStream below must be
        // serialized against concurrent hardware setup on those radios.
        auto            hwlock = lockHw(hw_ids);
        std::lock_guard lock(rt_mu_);
        auto            it = runtimes_.find(task_id);
        if (it != runtimes_.end())
        {
            auto& rt = it->second;
            // An executor attached after we looked above (orphaned runtime
            // race) is still stopped below, after the streams are gone.
            if (rt.scan_exec)
                scan_exec_to_stop = std::move(rt.scan_exec);
            if (rt.trig_mon)
                trig_mon_to_stop = std::move(rt.trig_mon);

            // Remove this task's subscription from each streamer.
            // Sub-band consumers registered via addSubBand; raw fan-out via addDest.
            // A task can hold raw dests and DDC sub-bands (multi-device); both
            // removals are no-ops when absent.
            for (auto& streamer : rt.streamers)
            {
                streamer->removeSubBand(task_id);
                streamer->removeDest(task_id);
            }

            // Close the SoapySDR stream only when the last consumer (dest or sub-band) is gone.
            // Shared-LO streams are tracked in channel_states_ — close when no consumers remain.
            // Independent-LO streams are NOT in channel_states_ (each task owns its stream
            // exclusively) — always close them directly to avoid leaking the SoapySDR stream.
            for (auto& [d, s] : rt.soapy_streams)
            {
                bool should_close            = false;
                bool found_in_channel_states = false;
                for (auto cs = channel_states_.begin(); cs != channel_states_.end();)
                {
                    if (cs->second.soapy_stream == s)
                    {
                        found_in_channel_states = true;
                        if (cs->second.streamer->totalConsumers() == 0)
                        {
                            cs->second.streamer->stop();
                            should_close = true;
                            cs           = channel_states_.erase(cs);
                        }
                        else
                        {
                            ++cs;
                        }
                    }
                    else
                    {
                        ++cs;
                    }
                }
                if (!found_in_channel_states)
                {
                    // Independent-LO stream exclusively owned by this task — stop all
                    // its IQStreamers (idempotent) and close the stream unconditionally.
                    for (auto& streamer : rt.streamers)
                        streamer->stop();
                    should_close = true;
                }
                if (should_close && d && s)
                {
                    d->deactivateStream(s);
                    d->closeStream(s);
                }
            }
            keep_alive = std::move(rt.streamers);
            runtimes_.erase(it);
        }
    }
    if (scan_exec_to_stop)
        scan_exec_to_stop->stop();
    if (trig_mon_to_stop)
        trig_mon_to_stop->stop();
    keep_alive.clear();

    // Release timeline slots and ports — only on first deactivation.
    // When already_terminal==true we are cleaning an orphaned runtime left by
    // an activateTask() that ran after a prior deactivateTask() already released
    // the ports. Releasing again would corrupt the pool.
    if (!already_terminal)
    {
        std::lock_guard lock(reg_mu_);
        auto            it = registry_.find(task_id);
        if (it != registry_.end())
        {
            for (auto& alloc : it->second.allocations)
            {
                if (timelines_.count(alloc.device_id))
                    timelines_[alloc.device_id]->remove(task_id);
                port_pool_->releaseAll(alloc.udp_ports);
            }
        }
    }
    notifyStateChange(task_id);
}

// ── Scheduler / Watchdog ticks ───────────────────────────────────────────
void ResourceManager::schedulerTick()
{
    int64_t                  now = nowMs();
    std::vector<std::string> to_activate;
    {
        std::lock_guard lock(reg_mu_);
        for (auto& [id, rec] : registry_)
            if (rec.state == TaskState::SCHEDULED && rec.start_time_ms <= now)
            {
                // Claim it here: a task left SCHEDULED until its thread ran
                // could be picked up again by the next tick (two activations).
                rec.state = TaskState::PENDING;
                to_activate.push_back(id);
            }
    }
    for (auto& id : to_activate)
    {
        spdlog::info("schedulerTick: activating {}", id);
        activations_in_flight_.fetch_add(1, std::memory_order_relaxed);
        std::thread(
            [this, id]()
            {
                activateTask(id);
                activations_in_flight_.fetch_sub(
                    1, std::memory_order_release);  // publishes the thread's accesses to the
                                                    // shutdown wait
            })
            .detach();
    }
}

void ResourceManager::watchdogTick()
{
    int64_t                  now = nowMs();
    std::vector<std::string> to_expire;
    {
        std::lock_guard lock(reg_mu_);
        for (auto& [id, rec] : registry_)
            if (rec.state == TaskState::RUNNING && rec.stop_time_ms != TIME_INFINITE
                && rec.stop_time_ms <= now)
                to_expire.push_back(id);
    }
    for (auto& id : to_expire)
    {
        spdlog::info("watchdogTick: expiring {}", id);
        deactivateTask(id, TaskState::COMPLETED, "stop_time reached");
    }
    pruneTerminalTasks();

    // Periodically retry opening devices that failed at startup (e.g. a
    // network-attached device whose mDNS entry wasn't published yet when
    // openDevices() ran). Interval: 30 seconds.
    using namespace std::chrono;
    auto tp_now = steady_clock::now();
    if (tp_now - last_device_open_attempt_ >= seconds(30))
    {
        last_device_open_attempt_ = tp_now;
        for (auto& [id, dev] : devices_)
        {
            if (!dev->isOnline())
            {
                spdlog::info("watchdogTick: retrying open for offline device {}", id);
                if (dev->open())
                    spdlog::info("watchdogTick: {} came online", id);
            }
        }
    }
}

// ── Queries ──────────────────────────────────────────────────────────────
std::vector<TaskRecord> ResourceManager::getActiveTasks() const
{
    std::lock_guard         lock(reg_mu_);
    std::vector<TaskRecord> out;
    for (auto& [id, rec] : registry_)
        if (!isTerminalState(rec.state))
            out.push_back(rec);
    return out;
}

int ResourceManager::countByState(TaskState s) const
{
    std::lock_guard lock(reg_mu_);
    int             n = 0;
    for (auto& [id, rec] : registry_)
        if (rec.state == s)
            ++n;
    return n;
}

std::vector<std::string> ResourceManager::deviceIds() const
{
    std::vector<std::string> ids;
    for (auto& [id, _] : devices_)
        ids.push_back(id);
    return ids;
}

RadioDevice* ResourceManager::getDevice(const std::string& id)
{
    auto it = devices_.find(id);
    return it != devices_.end() ? it->second.get() : nullptr;
}

std::vector<ResourceManager::DeviceSummary> ResourceManager::deviceSummaries() const
{
    std::vector<DeviceSummary> out;
    int64_t                    now = nowMs();
    for (auto& [id, dev] : devices_)
    {
        DeviceSummary s;
        auto          st  = dev->getStatus();
        s.device_id       = id;
        s.driver          = st.driver;
        s.uri             = st.uri;
        s.coherency_group = st.coherency_group;
        s.online          = st.online;
        s.cf_hz           = st.center_freq_hz;
        s.rate_sps        = st.sample_rate;
        s.temp_c          = st.temperature_c;
        if (timelines_.count(id))
        {
            auto& tl       = timelines_.at(id);
            s.alloc_bw_hz  = tl->allocatedBw(now);
            double usable  = st.sample_rate * cfg_.policy.usable_bw_fraction;
            s.free_bw_hz   = std::max(0.0, usable - s.alloc_bw_hz);
            s.active_tasks = tl->activeTaskCount(now);
        }
        out.push_back(s);
    }
    return out;
}

// ── Helpers ──────────────────────────────────────────────────────────────
void ResourceManager::notifyStateChange(const std::string& task_id)
{
    if (!on_state_change_)
        return;
    // Live per-stream metrics (TaskRecord::stream_metrics was never filled).
    std::vector<StreamMetrics> metrics;
    {
        std::lock_guard lk(rt_mu_);
        if (auto it = runtimes_.find(task_id); it != runtimes_.end())
            for (auto& st : it->second.streamers)
                metrics.push_back(st->getMetrics());
    }
    // notify_mu_ keeps TASK_STATUS messages ordered without running the
    // callback (AMQP encode/send) under reg_mu_.
    std::lock_guard order(notify_mu_);
    TaskRecord      rec;
    {
        std::lock_guard lock(reg_mu_);
        auto            it = registry_.find(task_id);
        if (it == registry_.end())
            return;
        rec = it->second;
    }
    rec.stream_metrics = std::move(metrics);
    on_state_change_(rec);
}

void ResourceManager::pruneTerminalTasks()
{
    using namespace std::chrono;
    const auto now = steady_clock::now();
    const auto retention =
        milliseconds(std::max<int64_t>(0, cfg_.policy.terminal_task_retention_ms));
    std::vector<std::pair<steady_clock::time_point, std::string>> terminal;
    {
        std::lock_guard lock(reg_mu_);
        for (auto& [id, t] : terminal_since_)
            terminal.push_back({t, id});
    }
    std::sort(terminal.begin(), terminal.end());  // oldest first
    const size_t             max_keep = (size_t)std::max(0, cfg_.policy.max_terminal_tasks);
    std::vector<std::string> drop;
    for (size_t i = 0; i < terminal.size(); ++i)
    {
        const bool too_old  = now - terminal[i].first >= retention;
        const bool too_many = terminal.size() - i > max_keep;
        if (too_old || too_many)
            drop.push_back(terminal[i].second);
    }
    if (drop.empty())
        return;
    {  // never drop a record whose runtime is still being torn down
        std::lock_guard lk(rt_mu_);
        drop.erase(std::remove_if(drop.begin(),
                                  drop.end(),
                                  [&](const std::string& id)
                                  {
                                      return runtimes_.count(id) > 0;
                                  }),
                   drop.end());
    }
    std::lock_guard lock(reg_mu_);
    for (auto& id : drop)
    {
        auto it = registry_.find(id);
        if (it != registry_.end() && isTerminalState(it->second.state))
            registry_.erase(it);
        terminal_since_.erase(id);
    }
}

std::string ResourceManager::makeStreamId(const std::string& task_id,
                                          const std::string& type,
                                          const std::string& dev_id,
                                          int                ch_idx) const
{
    return task_id + "-" + type + "-" + dev_id + "-" + std::to_string(ch_idx);
}

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
