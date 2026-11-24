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
#include "Controller.hpp"

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <thread>

namespace sdr
{
using namespace std::chrono;

Controller::Controller(const AppConfig& cfg)
    : cfg_(cfg)
    , start_time_(steady_clock::now())
{
    rm_ = std::make_unique<ResourceManager>(
        cfg_,
        [this](const TaskRecord& rec)
        {
            onTaskStateChanged(rec);
        },
        [](const std::string& tid, const std::string& err)
        {
            spdlog::error("Task error {}: {}", tid, err);
        });

    amqp_ =
        std::make_unique<AmqpClient>(cfg_.broker,
                                     [this](const std::string& body, const std::string& reply_to)
                                     {
                                         onMessage(body, reply_to);
                                     });
}

Controller::~Controller()
{
    stop();
}

void Controller::run()
{
    spdlog::info("Controller: starting, version={}", APP_VERSION);

    int opened = rm_->openDevices();
    spdlog::info("Controller: {}/{} devices opened", opened, (int)cfg_.devices.size());

    amqp_->start();
    running_.store(true);

    scheduler_thread_ = std::thread(&Controller::schedulerLoop, this);
    watchdog_thread_  = std::thread(&Controller::watchdogLoop, this);
    heartbeat_thread_ = std::thread(&Controller::heartbeatLoop, this);
    admission_thread_ = std::thread(&Controller::admissionLoop, this);

    spdlog::info("Controller: running");
    while (running_.load())
    {
        std::this_thread::sleep_for(milliseconds(100));
    }
    spdlog::info("Controller: stopped");
}

void Controller::stop()
{
    running_.store(false);
    // Stop AMQP before joining background threads so no new snapshot threads
    // can be spawned via handleSnapshotRequest() while we're cleaning up.
    amqp_->stop();
    {
        std::lock_guard lk(admission_mu_);  // wake the admission thread
        admission_cv_.notify_all();
    }
    if (admission_thread_.joinable())
        admission_thread_.join();
    if (scheduler_thread_.joinable())
        scheduler_thread_.join();
    if (watchdog_thread_.joinable())
        watchdog_thread_.join();
    if (heartbeat_thread_.joinable())
        heartbeat_thread_.join();
    {
        std::lock_guard<std::mutex> lk(snapshot_mu_);
        for (auto& st : snapshot_threads_)
            if (st.t.joinable())
                st.t.join();
        snapshot_threads_.clear();
    }
    rm_->closeDevices();
}

// ── Message routing ──────────────────────────────────────────────────────
void Controller::onMessage(const std::string& body, const std::string& reply_to)
{
    auto req = MessageCodec::decode(body);
    if (!req)
    {
        // Tell the client instead of letting it time out (ICD: INVALID_REQUEST
        // = malformed JSON or missing required fields). Needs a request_id to
        // correlate; without one there is nobody to answer.
        std::string req_id, corr_id;
        try
        {
            auto j = nlohmann::json::parse(body);
            if (j.is_object())
            {
                if (j.contains("request_id") && j["request_id"].is_string())
                    req_id = j["request_id"].get<std::string>();
                if (j.contains("correlation_id") && j["correlation_id"].is_string())
                    corr_id = j["correlation_id"].get<std::string>();
            }
        }
        catch (...)
        {
        }
        spdlog::warn("Controller: failed to decode message{}",
                     req_id.empty() ? " (no request_id — dropped)" : " " + req_id);
        if (!req_id.empty())
            sendReject(req_id,
                       corr_id,
                       RejectCode::INVALID_REQUEST,
                       "Malformed request or missing/invalid fields",
                       reply_to);
        return;
    }
    spdlog::debug("Controller: received msg_type={} req_id={}", req->msg_type, req->request_id);

    if (req->msg_type == "TASK_STOP")
        return handleTaskStop(*req, reply_to);
    if (req->msg_type == "TASK_CANCEL")
        return handleTaskCancel(*req, reply_to);
    if (req->msg_type == "HEALTH_QUERY")
        return handleHealthQuery(*req, reply_to);
    if (req->msg_type == "DEVICE_TEMP_QUERY")
        return handleTempQuery(*req, reply_to);
    if (req->msg_type == "TASK_REQUEST_SNAPSHOT")
        return handleSnapshotRequest(*req, reply_to);
    return handleTaskRequest(*req, reply_to);
}

void Controller::handleTaskRequest(const TaskRequest& req, const std::string& reply_to)
{
    if (req.task_type == TaskType::UNKNOWN)
    {
        sendReject(req.request_id,
                   req.correlation_id,
                   RejectCode::INVALID_TASK_TYPE,
                   "Unknown task_type",
                   reply_to);
        return;
    }
    {
        std::lock_guard lk(admission_mu_);
        if (admission_q_.size() < kMaxAdmissionQueue)
        {
            admission_q_.emplace_back(req, reply_to);
            admission_cv_.notify_one();
            return;
        }
    }
    sendReject(req.request_id,
               req.correlation_id,
               RejectCode::INTERNAL_ERROR,
               "Admission queue full",
               reply_to);
}

void Controller::admissionLoop()
{
    for (;;)
    {
        std::pair<TaskRequest, std::string> item;
        {
            std::unique_lock lk(admission_mu_);
            admission_cv_.wait(lk,
                               [this]
                               {
                                   return !admission_q_.empty() || !running_.load();
                               });
            if (!running_.load())
                return;  // shutting down: queued requests are dropped
            item = std::move(admission_q_.front());
            admission_q_.pop_front();
        }
        auto resp = rm_->tryAccept(item.first);
        amqp_->sendResponse(MessageCodec::encodeTaskResponse(resp), item.second);
    }
}

void Controller::handleTaskStop(const TaskRequest& req, const std::string& reply_to)
{
    // async: the hardware teardown must not run on the AMQP thread (RM-17).
    auto resp = rm_->stopTask(req.task_id, req.request_id, req.reason, /*async=*/true);
    amqp_->sendResponse(MessageCodec::encodeTaskResponse(resp), reply_to);
}

void Controller::handleTaskCancel(const TaskRequest& req, const std::string& reply_to)
{
    auto resp = rm_->cancelTask(req.task_id, req.request_id, req.reason, /*async=*/true);
    amqp_->sendResponse(MessageCodec::encodeTaskResponse(resp), reply_to);
}

void Controller::handleHealthQuery(const TaskRequest& req, const std::string& reply_to)
{
    auto                                      devs = rm_->deviceSummaries();
    std::vector<MessageCodec::DevHealthEntry> entries;
    for (auto& d : devs)
    {
        MessageCodec::DevHealthEntry e;
        e.device_id       = d.device_id;
        e.driver          = d.driver;
        e.uri             = d.uri;
        e.coherency_group = d.coherency_group;
        e.online          = d.online;
        e.active_tasks    = d.active_tasks;
        e.cf_hz           = d.cf_hz;
        e.rate_sps        = d.rate_sps;
        e.alloc_bw_hz     = d.alloc_bw_hz;
        e.free_bw_hz      = d.free_bw_hz;
        e.temp_c          = d.temp_c;
        entries.push_back(e);
    }
    int64_t uptime = duration_cast<seconds>(steady_clock::now() - start_time_).count();
    auto    body   = MessageCodec::encodeHealthQueryResponse(req.request_id,
                                                        entries,
                                                        (int)cfg_.devices.size(),
                                                        (int)std::count_if(entries.begin(),
                                                                           entries.end(),
                                                                           [](auto& e)
                                                                           {
                                                                               return e.online;
                                                                           }),
                                                        rm_->countByState(TaskState::SCHEDULED),
                                                        rm_->countByState(TaskState::PENDING),
                                                        rm_->countByState(TaskState::RUNNING),
                                                        rm_->countByState(TaskState::SCHEDULED)
                                                            + rm_->countByState(TaskState::PENDING)
                                                            + rm_->countByState(TaskState::RUNNING),
                                                        rm_->udpPortsUsed(),
                                                        rm_->udpPortsFree(),
                                                        uptime);
    amqp_->sendResponse(body, reply_to);
}

void Controller::handleSnapshotRequest(const TaskRequest& req, const std::string& reply_to)
{
    // doAcceptSnapshot tunes hardware and blocks in readStream for up to several
    // seconds. Running it on the AMQP reactor thread would starve heartbeats and
    // risk broker disconnect. Track in snapshot_threads_ so stop() can join them
    // before destroying rm_/amqp_ — detach() would leave a dangling 'this'.
    std::lock_guard<std::mutex> lk(snapshot_mu_);
    for (auto it = snapshot_threads_.begin(); it != snapshot_threads_.end();)
    {
        if (it->done->load())
        {
            if (it->t.joinable())
                it->t.join();
            it = snapshot_threads_.erase(it);
        }
        else
            ++it;
    }
    auto done = std::make_shared<std::atomic<bool>>(false);
    snapshot_threads_.push_back(
        {std::thread(
             [this, req, reply_to, done]()
             {
                 SnapshotResult snap;
                 auto           resp = rm_->tryAccept(req, &snap);
                 // TASK_RESPONSE first (existing clients read the first reply), then the
                 // SNAPSHOT_RESULT with the power bins (ICD §8.5/§8.14) — it was never
                 // sent, the FFT was computed and discarded (RM-56).
                 amqp_->sendResponse(MessageCodec::encodeTaskResponse(resp), reply_to);
                 amqp_->sendResponse(MessageCodec::encodeSnapshotResult(req.request_id, snap),
                                     reply_to);
                 done->store(true);
             }),
         done});
}

void Controller::onTaskStateChanged(const TaskRecord& rec)
{
    std::vector<StreamMetrics> m    = rec.stream_metrics;
    auto                       body = MessageCodec::encodeTaskStatus(rec, m);
    amqp_->sendStatus(body);
    spdlog::info("TaskStatus [{}] → {}", rec.task_id, taskStateToString(rec.state));
}

// ── Background threads ───────────────────────────────────────────────────
bool Controller::sleepResponsive(int total_ms)
{
    const int step_ms = 100;
    for (int slept = 0; slept < total_ms; slept += step_ms)
    {
        if (!running_.load())
            return false;
        std::this_thread::sleep_for(milliseconds(std::min(step_ms, total_ms - slept)));
    }
    return running_.load();
}

void Controller::schedulerLoop()
{
    while (running_.load())
    {
        // Wake at the next start / stop_time, not just every tick (a
        // scheduled task used to start up to scheduler_tick_ms late).
        const int64_t wait = rm_->msUntilNextEvent(cfg_.policy.scheduler_tick_ms);
        if (wait > 0 && !sleepResponsive((int)wait))
            break;
        if (!running_.load())
            break;
        rm_->schedulerTick();
    }
}

void Controller::watchdogLoop()
{
    while (running_.load())
    {
        if (!sleepResponsive(cfg_.policy.watchdog_tick_ms))
            break;
        rm_->watchdogTick();
    }
}

void Controller::heartbeatLoop()
{
    while (running_.load())
    {
        if (!sleepResponsive(cfg_.policy.heartbeat_interval_ms))
            break;

        auto                                      devs = rm_->deviceSummaries();
        std::vector<MessageCodec::DevHealthEntry> entries;
        for (auto& d : devs)
        {
            MessageCodec::DevHealthEntry e;
            e.device_id       = d.device_id;
            e.driver          = d.driver;
            e.uri             = d.uri;
            e.coherency_group = d.coherency_group;
            e.online          = d.online;
            e.active_tasks    = d.active_tasks;
            e.cf_hz           = d.cf_hz;
            e.rate_sps        = d.rate_sps;
            e.alloc_bw_hz     = d.alloc_bw_hz;
            e.free_bw_hz      = d.free_bw_hz;
            e.temp_c          = d.temp_c;
            entries.push_back(e);
        }
        amqp_->sendHealth(MessageCodec::encodeDeviceHealth("hb-auto", entries));

        int64_t uptime = duration_cast<seconds>(steady_clock::now() - start_time_).count();
        amqp_->sendHealth(MessageCodec::encodeControllerHealth(
            "hb-auto",
            (int)cfg_.devices.size(),
            (int)std::count_if(entries.begin(),
                               entries.end(),
                               [](auto& e)
                               {
                                   return e.online;
                               }),
            rm_->countByState(TaskState::SCHEDULED),
            rm_->countByState(TaskState::PENDING),
            rm_->countByState(TaskState::RUNNING),
            rm_->countByState(TaskState::SCHEDULED) + rm_->countByState(TaskState::PENDING)
                + rm_->countByState(TaskState::RUNNING),
            rm_->udpPortsUsed(),
            rm_->udpPortsFree(),
            uptime));
    }
}

void Controller::handleTempQuery(const TaskRequest& req, const std::string& reply_to)
{
    std::vector<MessageCodec::TempEntry> entries;
    for (auto& dev_id : rm_->deviceIds())
    {
        MessageCodec::TempEntry e;
        e.device_id = dev_id;
        auto* dev   = rm_->getDevice(dev_id);
        if (!dev)
            continue;
        e.online = dev->isOnline();
        for (auto& s : dev->cachedTemperatures())  // no driver I/O here (RM-17)
        {
            bool ok = !std::isnan(s.value_c);
            e.sensors.push_back({s.name, s.value_c, ok});
        }
        entries.push_back(std::move(e));
    }
    amqp_->sendResponse(MessageCodec::encodeTempResponse(req.request_id, entries), reply_to);
}

void Controller::sendReject(const std::string& req_id,
                            const std::string& corr_id,
                            RejectCode         code,
                            const std::string& reason,
                            const std::string& reply_to)
{
    TaskResponse resp;
    resp.request_id     = req_id;
    resp.correlation_id = corr_id;
    resp.reject_code    = code;
    resp.reject_reason  = reason;
    amqp_->sendResponse(MessageCodec::encodeTaskResponse(resp), reply_to);
}

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
