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
// AmqpClient against an in-process loopback AMQP peer: sends from other
// threads (RM-87) and links after a broker restart (RM-86).

#include "AmqpClient.hpp"
#include "ConfigParser.hpp"

#include <gtest/gtest.h>
#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/listen_handler.hpp>
#include <proton/listener.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/receiver_options.hpp>
#include <proton/sender.hpp>
#include <proton/work_queue.hpp>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{

/// Loopback AMQP peer: accepts one sender link and grants only the credit the
/// test asks for.
class TestPeer
    : public proton::messaging_handler
    , public proton::listen_handler
{
public:
    explicit TestPeer(int initial_credit, int port = 0)
        : credit_(initial_credit)
        , want_port_(port)
    {
        thread_ = std::thread(
            [this]
            {
                container_.run();
            });
        std::unique_lock lk(mu_);
        cv_.wait_for(lk,
                     5s,
                     [this]
                     {
                         return port_ != 0 || failed_;
                     });
    }

    ~TestPeer()
    {
        shutdown();
    }

    void shutdown()
    {
        if (!thread_.joinable())
            return;
        container_.stop();
        thread_.join();
    }

    int port() const
    {
        return port_;
    }

    bool linked()
    {
        std::lock_guard lk(mu_);
        return wq_ != nullptr;
    }

    void grant(int n)
    {
        proton::work_queue* wq;
        {
            std::lock_guard lk(mu_);
            wq = wq_;
        }
        ASSERT_NE(wq, nullptr);
        wq->add(
            [this, n]
            {
                rcv_.add_credit(n);
            });
    }

    /// Wait until `n` messages arrived or `timeout` passed; returns the count.
    size_t waitFor(size_t n, std::chrono::milliseconds timeout)
    {
        std::unique_lock lk(mu_);
        cv_.wait_for(lk,
                     timeout,
                     [&]
                     {
                         return bodies_.size() >= n;
                     });
        return bodies_.size();
    }

    int links()
    {
        std::lock_guard lk(mu_);
        return links_;
    }

    std::vector<std::string> bodies()
    {
        std::lock_guard lk(mu_);
        return bodies_;
    }

    // listen_handler
    void on_open(proton::listener& l) override
    {
        std::lock_guard lk(mu_);
        port_ = l.port();
        cv_.notify_all();
    }

    void on_error(proton::listener&, const std::string&) override
    {
        std::lock_guard lk(mu_);
        failed_ = true;
        cv_.notify_all();
    }

    // messaging_handler
    void on_container_start(proton::container& c) override
    {
        listener_ = c.listen("127.0.0.1:" + std::to_string(want_port_), *this);
    }

    void on_receiver_open(proton::receiver& r) override
    {
        r.open(proton::receiver_options().credit_window(0));
        if (credit_ > 0)
            r.add_credit(credit_);
        std::lock_guard lk(mu_);
        ++links_;
        rcv_ = r;
        wq_  = &r.work_queue();
    }

    void on_sender_open(proton::sender& s) override
    {
        s.open();
        std::lock_guard lk(mu_);
        ++consumer_links_;
        to_client_ = s;
    }

    /// Send one message to the client's receiver (an inbound frame for it).
    void sendToClient(const std::string& body)
    {
        proton::work_queue* wq;
        {
            std::lock_guard lk(mu_);
            wq = wq_;
        }
        ASSERT_NE(wq, nullptr);
        wq->add(
            [this, body]
            {
                if (to_client_ && to_client_.credit() > 0)
                    to_client_.send(proton::message(body));
            });
    }

    int consumerLinks()
    {
        std::lock_guard lk(mu_);
        return consumer_links_;
    }

    void on_error(const proton::error_condition&) override
    {
    }  // aborted on shutdown

    void on_message(proton::delivery&, proton::message& m) override
    {
        std::lock_guard lk(mu_);
        bodies_.push_back(proton::get<std::string>(m.body()));
        cv_.notify_all();
    }

private:
    int                      credit_;
    int                      want_port_;
    std::mutex               mu_;
    std::condition_variable  cv_;
    int                      port_{0};
    bool                     failed_{false};
    int                      links_{0};
    int                      consumer_links_{0};
    proton::sender           to_client_;
    proton::receiver         rcv_;
    proton::work_queue*      wq_{nullptr};
    std::vector<std::string> bodies_;
    proton::listener         listener_;
    proton::container        container_{*this};
    std::thread              thread_;
};

bool waitUntil(const std::function<bool()>& pred, std::chrono::milliseconds timeout)
{
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < end)
    {
        if (pred())
            return true;
        std::this_thread::sleep_for(5ms);
    }
    return pred();
}

}  // namespace

sdr::BrokerConfig loopback(int port)
{
    sdr::BrokerConfig cfg;
    cfg.url                        = "amqp://127.0.0.1:" + std::to_string(port);
    cfg.username                   = "";
    cfg.reconnect_interval_sec     = 1;
    cfg.max_reconnect_interval_sec = 1;
    return cfg;
}

// Responses, status and health were posted with container::schedule(), whose
// timer task does not wake the connection: the message sat in proton until
// the next inbound frame on that connection (a request, a broker heartbeat).
TEST(AmqpClientSend, RM87_SendGoesOutWithoutInboundTraffic)
{
    TestPeer peer(1000);
    ASSERT_NE(peer.port(), 0);
    sdr::AmqpClient client(loopback(peer.port()),
                           [](const std::string&, const std::string&)
                           {
                           });
    client.start();
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return peer.links() == 3 && client.isConnected();
        },
        3s));
    std::this_thread::sleep_for(100ms);

    const auto t0 = std::chrono::steady_clock::now();
    std::thread(
        [&]
        {
            client.sendResponse("{\"msg_type\":\"TASK_RESPONSE\"}");
        })
        .join();
    const size_t got = peer.waitFor(1, 2s);
    const auto   ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    std::printf("RM87: response %s after %lld ms with an idle connection\n",
                got ? "delivered" : "NOT delivered",
                static_cast<long long>(ms));
    EXPECT_EQ(got, 1u);
    EXPECT_LT(ms, 500);
    client.stop();
}

// Each reconnect opened the request receiver and the response, status and
// health senders again next to the ones proton had already restored, so the
// controller held four more links per broker restart.
TEST(AmqpClientReconnect, RM86_SameLinksAfterBrokerRestart)
{
    auto      peer = std::make_unique<TestPeer>(1000);
    const int port = peer->port();
    ASSERT_NE(port, 0);
    sdr::AmqpClient client(loopback(port),
                           [](const std::string&, const std::string&)
                           {
                           });
    client.start();
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return peer->links() == 3 && peer->consumerLinks() == 1;
        },
        3s));

    peer.reset();
    std::this_thread::sleep_for(300ms);
    peer = std::make_unique<TestPeer>(1000, port);
    ASSERT_EQ(peer->port(), port);
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return peer->links() >= 3 && peer->consumerLinks() >= 1;
        },
        5s));
    std::this_thread::sleep_for(300ms);  // a duplicate set would have arrived by now
    std::printf("RM86: %d sender + %d receiver link(s) after reconnect\n",
                peer->links(),
                peer->consumerLinks());
    EXPECT_EQ(peer->links(), 3);
    EXPECT_EQ(peer->consumerLinks(), 1);

    // The restored status link still carries messages.
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return client.isConnected();
        },
        2s));
    client.sendStatus("{\"msg_type\":\"SDR_STATUS\"}");
    EXPECT_EQ(peer->waitFor(1, 2s), 1u);
    client.stop();
}

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
