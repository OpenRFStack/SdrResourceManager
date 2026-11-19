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
#include "AmqpClient.hpp"

#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/receiver_options.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/sender.hpp>
#include <proton/sender_options.hpp>
#include <proton/source_options.hpp>
#include <proton/target_options.hpp>
#include <proton/transport.hpp>
#include <proton/value.hpp>
#include <proton/work_queue.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

namespace sdr
{

// ─── Handler (proton internal class) ────────────────────────────────────
class AmqpClient::Handler : public proton::messaging_handler
{
public:
    Handler(AmqpClient& parent, const BrokerConfig& cfg)
        : parent_(parent)
        , cfg_(cfg)
    {
    }

    void on_container_start(proton::container& c) override
    {
        proton::connection_options copts;
        if (!cfg_.username.empty())
        {
            copts.sasl_allowed_mechs("PLAIN");
            copts.sasl_allow_insecure_mechs(true);
            copts.user(cfg_.username).password(cfg_.password);
        }
        else
        {
            copts.sasl_allowed_mechs("ANONYMOUS");
        }

        proton::reconnect_options ropts;
        ropts.delay(proton::duration(cfg_.reconnect_interval_sec * 1000));
        ropts.max_delay(proton::duration(cfg_.max_reconnect_interval_sec * 1000));
        ropts.max_attempts(0);  // 0 = retry forever
        copts.reconnect(ropts);

        c.connect(cfg_.url, copts);
    }

    void on_connection_open(proton::connection& c) override
    {
        spdlog::info("AmqpClient: connected to {}", cfg_.url);

        // Force ANYCAST routing on queues so Artemis doesn't create MULTICAST
        // addresses, which stall sender credit for ~15 seconds.
        proton::receiver_options req_ropts;
        req_ropts.source(proton::source_options().capabilities({proton::symbol("queue")}));
        c.open_receiver(cfg_.request_queue, req_ropts);

        proton::sender_options resp_sopts;
        resp_sopts.target(proton::target_options().capabilities({proton::symbol("queue")}));
        response_sender_ = c.open_sender(cfg_.response_queue, resp_sopts);
        reply_senders_.clear();  // links of the previous connection are gone

        status_sender_ = c.open_sender(cfg_.status_topic);
        health_sender_ = c.open_sender(cfg_.health_topic);
        parent_.onConnected();
    }

    void on_message(proton::delivery&, proton::message& msg) override
    {
        try
        {
            // Accept JSON sent as an AMQP string or as binary data (many
            // clients/bridges send bytes); binary used to throw and the
            // request was dropped without any reply.
            std::string          body;
            const proton::value& v = msg.body();
            if (v.type() == proton::BINARY)
            {
                auto b = proton::get<proton::binary>(v);
                body.assign(b.begin(), b.end());
            }
            else
            {
                body = proton::get<std::string>(v);
            }
            std::string reply_to = msg.reply_to();  // empty if not set
            parent_.on_message_(body, reply_to);
        }
        catch (const std::exception& ex)
        {
            spdlog::error("AmqpClient: on_message error: {}", ex.what());
        }
    }

    void on_connection_close(proton::connection&) override
    {
        spdlog::warn("AmqpClient: connection closed");
        parent_.onDisconnected();
    }

    void on_transport_error(proton::transport& t) override
    {
        spdlog::error("AmqpClient: transport error: {}", t.error().what());
        parent_.onDisconnected();
    }

    void on_error(const proton::error_condition& e) override
    {
        spdlog::error("AmqpClient: error: {}", e.what());
    }

    void sendOn(proton::sender& s, const std::string& body)
    {
        if (!s)
            return;
        // Do NOT check s.credit() — proton queues the message and sends it
        // when credit arrives. Checking credit causes silent drops when a
        // fresh receiver hasn't yet propagated credit back to this sender.
        try
        {
            proton::message m;
            m.body(body);
            m.content_type("application/json");
            m.durable(false);
            s.send(m);
        }
        catch (const std::exception& ex)
        {
            spdlog::error("AmqpClient: send error: {}", ex.what());
        }
    }

    proton::sender response_sender_;
    proton::sender status_sender_;
    proton::sender health_sender_;

    // Reply links by address (reactor thread only). Consumers with dynamic
    // reply queues (AcquisitionApp, AnalysisApp, DemodApp) used to get a NEW
    // sender link for every response, never closed — unbounded link growth on
    // both the controller and the broker (AMQP-01). Bounded LRU.
    proton::sender replySender(proton::connection conn, const std::string& addr)
    {
        auto it = reply_senders_.find(addr);
        if (it != reply_senders_.end() && it->second.first.active())
        {
            it->second.second = ++reply_clock_;
            return it->second.first;
        }
        if (it != reply_senders_.end())
            reply_senders_.erase(it);
        if (reply_senders_.size() >= kMaxReplySenders)
        {
            auto oldest = std::min_element(reply_senders_.begin(),
                                           reply_senders_.end(),
                                           [](auto& a, auto& b)
                                           {
                                               return a.second.second < b.second.second;
                                           });
            try
            {
                oldest->second.first.close();
            }
            catch (...)
            {
            }
            reply_senders_.erase(oldest);
        }
        proton::sender_options sopts;
        sopts.target(proton::target_options().capabilities({proton::symbol("queue")}));
        proton::sender s     = conn.open_sender(addr, sopts);
        reply_senders_[addr] = {s, ++reply_clock_};
        return s;
    }

    void on_sender_close(proton::sender& s) override
    {
        forget(s);
    }

    void on_sender_error(proton::sender& s) override
    {
        forget(s);
    }

    void forget(proton::sender& s)
    {
        for (auto it = reply_senders_.begin(); it != reply_senders_.end(); ++it)
            if (it->second.first == s)
            {
                reply_senders_.erase(it);
                return;
            }
    }

    static constexpr size_t                                              kMaxReplySenders = 64;
    std::unordered_map<std::string, std::pair<proton::sender, uint64_t>> reply_senders_;
    uint64_t                                                             reply_clock_ = 0;

private:
    AmqpClient&  parent_;
    BrokerConfig cfg_;
};

// ─────────────────────────────────────────────────────────────────────────
AmqpClient::AmqpClient(const BrokerConfig& cfg, MessageHandler on_message)
    : cfg_(cfg)
    , on_message_(std::move(on_message))
{
    handler_   = std::make_unique<Handler>(*this, cfg_);
    container_ = std::make_unique<proton::container>(*handler_);
}

AmqpClient::~AmqpClient()
{
    stop();
}

void AmqpClient::start()
{
    running_.store(true);
    container_stopped_->store(false);
    auto stopped      = container_stopped_;  // shared ownership — safe if detached
    container_thread_ = std::thread(
        [this, stopped]()
        {
            try
            {
                container_->run();
            }
            catch (const std::exception& ex)
            {
                spdlog::error("AmqpClient: container run exception: {}", ex.what());
            }
            stopped->store(true);
        });
    spdlog::info("AmqpClient: started, connecting to {}", cfg_.url);
}

void AmqpClient::stop()
{
    if (!running_.exchange(false))
        return;
    try
    {
        container_->stop();
    }
    catch (...)
    {
    }
    if (container_thread_.joinable())
    {
        // container::stop() doesn't reliably cancel a pending reconnect
        // backoff timer (qpid-proton C++ binding) — if the broker is
        // unreachable right when stop() is called, container_->run() can
        // keep sleeping through its current retry delay before noticing the
        // stop request. With reconnect_options(max_attempts=0) (deliberate,
        // for resilience against a broker that isn't up yet) that delay can
        // be the full max_reconnect_interval_sec. Don't block the whole
        // shutdown path on it — give it a short grace period and detach
        // rather than join if it's still not done; the OS reclaims the
        // thread when the process exits regardless. Confirmed live: without
        // this, `podman stop` on sdr_controller against a real RTL-SDR with
        // no broker present always hit the stop timeout and needed SIGKILL.
        for (int i = 0; i < 30 && !container_stopped_->load(); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (container_stopped_->load())
        {
            container_thread_.join();
        }
        else
        {
            spdlog::warn("AmqpClient: proton thread still in reconnect "
                         "backoff after grace period — detaching");
            container_thread_.detach();
        }
    }
    spdlog::info("AmqpClient: stopped");
}

void AmqpClient::sendResponse(const std::string& body, const std::string& reply_to)
{
    if (!connected_)
    {
        spdlog::warn("AmqpClient: sendResponse while disconnected");
        return;
    }
    // May be called from any thread (e.g. background snapshot thread) — schedule
    // onto the reactor thread via container_->schedule so proton sender objects
    // are only touched from the reactor thread.
    container_->schedule(proton::duration(0),
                         [this, body, reply_to]()
                         {
                             if (reply_to.empty() || reply_to == cfg_.response_queue)
                             {
                                 handler_->sendOn(handler_->response_sender_, body);
                             }
                             else
                             {
                                 proton::connection conn = handler_->response_sender_.connection();
                                 proton::sender     s    = handler_->replySender(conn, reply_to);
                                 handler_->sendOn(s, body);
                             }
                         });
}

void AmqpClient::sendStatus(const std::string& body)
{
    if (!connected_)
        return;
    container_->schedule(proton::duration(0),
                         [this, body]()
                         {
                             handler_->sendOn(handler_->status_sender_, body);
                         });
}

void AmqpClient::sendHealth(const std::string& body)
{
    if (!connected_)
        return;
    container_->schedule(proton::duration(0),
                         [this, body]()
                         {
                             handler_->sendOn(handler_->health_sender_, body);
                         });
}

void AmqpClient::onConnected()
{
    connected_.store(true);
}

void AmqpClient::onDisconnected()
{
    connected_.store(false);
}

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
