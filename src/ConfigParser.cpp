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
#include "ConfigParser.hpp"

#include <tinyxml2.h>

#include <algorithm>
#include <stdexcept>

namespace sdr
{
using namespace tinyxml2;

static const char* req(XMLElement* p, const char* n)
{
    auto* e = p ? p->FirstChildElement(n) : nullptr;
    if (!e || !e->GetText())
        throw std::runtime_error(std::string("Missing XML <") + n + ">");
    return e->GetText();
}

static std::string opt(XMLElement* p, const char* n, const char* d = "")
{
    if (!p)
        return d;
    auto* e = p->FirstChildElement(n);
    return (e && e->GetText()) ? e->GetText() : d;
}

static double optD(XMLElement* p, const char* n, double d)
{
    if (!p)
        return d;
    auto* e = p->FirstChildElement(n);
    if (!e || !e->GetText())
        return d;
    try
    {
        return std::stod(e->GetText());
    }
    catch (...)
    {
        return d;
    }
}

static int optI(XMLElement* p, const char* n, int d)
{
    if (!p)
        return d;
    auto* e = p->FirstChildElement(n);
    if (!e || !e->GetText())
        return d;
    try
    {
        return std::stoi(e->GetText());
    }
    catch (...)
    {
        return d;
    }
}

static int64_t optLL(XMLElement* p, const char* n, int64_t d)
{
    if (!p)
        return d;
    auto* e = p->FirstChildElement(n);
    if (!e || !e->GetText())
        return d;
    try
    {
        return std::stoll(e->GetText());
    }
    catch (...)
    {
        return d;
    }
}

AppConfig ConfigParser::parse(const std::string& path)
{
    XMLDocument doc;
    if (doc.LoadFile(path.c_str()) != XML_SUCCESS)
        throw std::runtime_error("Cannot load: " + path + " — " + doc.ErrorStr());
    auto* root = doc.FirstChildElement("sdr_controller");
    if (!root)
        throw std::runtime_error("<sdr_controller> not found in " + path);

    AppConfig cfg;

    if (auto* b = root->FirstChildElement("broker"))
    {
        cfg.broker.url                        = opt(b, "url", "amqp://localhost:5672");
        cfg.broker.username                   = opt(b, "username", "guest");
        cfg.broker.password                   = opt(b, "password", "guest");
        cfg.broker.request_queue              = opt(b, "request_queue", "sdr.task.request");
        cfg.broker.response_queue             = opt(b, "response_queue", "sdr.task.response");
        cfg.broker.status_topic               = opt(b, "status_topic", "sdr.status");
        cfg.broker.health_topic               = opt(b, "health_topic", "sdr.health");
        cfg.broker.reconnect_interval_sec     = optI(b, "reconnect_interval_sec", 5);
        cfg.broker.max_reconnect_interval_sec = optI(b, "max_reconnect_interval_sec", 60);
        cfg.broker.send_queue_depth           = optI(b, "send_queue_depth", 512);
    }

    if (auto* p = root->FirstChildElement("policy"))
    {
        cfg.policy.max_concurrent_tasks       = optI(p, "max_concurrent_tasks", 64);
        cfg.policy.guard_band_hz              = optD(p, "guard_band_hz", 200e3);
        cfg.policy.usable_bw_fraction         = optD(p, "usable_bw_fraction", 0.80);
        cfg.policy.default_task_timeout_ms    = optLL(p, "default_task_timeout_ms", 3'600'000LL);
        cfg.policy.scheduler_tick_ms          = optI(p, "scheduler_tick_ms", 200);
        cfg.policy.watchdog_tick_ms           = optI(p, "watchdog_tick_ms", 1000);
        cfg.policy.sensor_refresh_ms          = std::max(100, optI(p, "sensor_refresh_ms", 5000));
        cfg.policy.udp_port_pool_start        = optI(p, "udp_port_pool_start", 30'000);
        cfg.policy.udp_port_pool_end          = optI(p, "udp_port_pool_end", 31'999);
        cfg.policy.iq_packet_samples          = optI(p, "iq_packet_samples", 1024);
        cfg.policy.heartbeat_interval_ms      = optI(p, "heartbeat_interval_ms", 30'000);
        cfg.policy.terminal_task_retention_ms = optLL(p, "terminal_task_retention_ms", 600'000LL);
        cfg.policy.max_terminal_tasks         = optI(p, "max_terminal_tasks", 1000);
        std::string rcp                       = opt(p, "retune_conflict_policy", "REJECT_NEW");
        cfg.policy.retune_conflict_policy     = (rcp == "CANCEL_LOWER")
                                                    ? RetuneConflictPolicy::CANCEL_LOWER
                                                    : RetuneConflictPolicy::REJECT_NEW;
    }
    if (cfg.policy.udp_port_pool_start >= cfg.policy.udp_port_pool_end)
        throw std::runtime_error("udp_port_pool_start must be < udp_port_pool_end");
    // One UDP datagram carries a 32-byte header + 8 bytes/sample and may not
    // exceed 65507 bytes: larger values made every sendmsg() fail with
    // EMSGSIZE (silently — no packets at all). 183 fits a 1500-byte MTU.
    if (cfg.policy.iq_packet_samples < 1 || cfg.policy.iq_packet_samples > 8184)
        throw std::runtime_error(
            "iq_packet_samples must be in [1, 8184] (65507-byte UDP limit); got "
            + std::to_string(cfg.policy.iq_packet_samples));

    // Intervals of 0 made the watchdog / heartbeat / scheduler threads spin:
    // watchdog_tick_ms and heartbeat_interval_ms 0 ran the RM at 279 % CPU
    // and published heartbeats without pause (RM-104).
    auto positive = [](const char* name, long long v)
    {
        if (v <= 0)
            throw std::runtime_error(std::string("<") + name + "> must be > 0; got "
                                     + std::to_string(v));
    };
    positive("scheduler_tick_ms", cfg.policy.scheduler_tick_ms);
    positive("watchdog_tick_ms", cfg.policy.watchdog_tick_ms);
    positive("heartbeat_interval_ms", cfg.policy.heartbeat_interval_ms);
    positive("max_concurrent_tasks", cfg.policy.max_concurrent_tasks);
    positive("default_task_timeout_ms", cfg.policy.default_task_timeout_ms);
    positive("reconnect_interval_sec", cfg.broker.reconnect_interval_sec);
    if (!(cfg.policy.usable_bw_fraction > 0.0 && cfg.policy.usable_bw_fraction <= 1.0))
        throw std::runtime_error("<usable_bw_fraction> must be in (0, 1]; got "
                                 + std::to_string(cfg.policy.usable_bw_fraction));

    auto* devs = root->FirstChildElement("devices");
    if (!devs)
        throw std::runtime_error("<devices> not found");

    for (auto* d = devs->FirstChildElement("device"); d; d = d->NextSiblingElement("device"))
    {
        const char* id = d->Attribute("id");
        if (!id)
            throw std::runtime_error("<device> missing id attribute");
        DeviceConfig dc;
        dc.id                  = id;
        dc.driver              = opt(d, "driver", "remote");
        dc.uri                 = opt(d, "uri", "");
        dc.label               = opt(d, "label", dc.id.c_str());
        dc.streaming_source_ip = opt(d, "streaming_source_ip", "127.0.0.1");
        dc.coherency_group     = opt(d, "coherency_group", "");
        {
            std::string sl = opt(d, "shared_lo", "true");
            dc.shared_lo   = (sl != "false" && sl != "0");
        }
        auto* c                     = d->FirstChildElement("capabilities");
        dc.caps.rx_channels         = optI(c, "rx_channels", 2);
        dc.caps.tx_channels         = optI(c, "tx_channels", 2);
        dc.caps.freq_min_hz         = optD(c, "freq_min_hz", 70e6);
        dc.caps.freq_max_hz         = optD(c, "freq_max_hz", 6e9);
        dc.caps.bandwidth_max_hz    = optD(c, "bandwidth_max_hz", 56e6);
        dc.caps.sample_rate_max_sps = optD(c, "sample_rate_max_sps", 61.44e6);
        dc.caps.sample_rate_min_sps = optD(c, "sample_rate_min_sps", 0.0);
        dc.caps.rx_gain_min_db      = optD(c, "rx_gain_min_db", -3.0);
        dc.caps.rx_gain_max_db      = optD(c, "rx_gain_max_db", 71.0);
        dc.caps.tx_atten_min_db     = optD(c, "tx_atten_min_db", 0.0);
        dc.caps.tx_atten_max_db     = optD(c, "tx_atten_max_db", 89.0);
        dc.fixed_sample_rate_hz     = optD(d, "fixed_sample_rate_hz", 0.0);
        dc.retune_settle_reads      = std::clamp((int)optD(d, "retune_settle_reads", 2.0), 0, 1000);
        dc.retune_settle_samples    = std::clamp(
            (int64_t)optD(d, "retune_settle_samples", 0.0), (int64_t)0, (int64_t)100'000'000);
        {
            std::string agc = opt(d, "rx_agc", "false");
            dc.rx_agc       = (agc == "true" || agc == "1");
        }
        dc.rx_gain_db = optD(d, "rx_gain_db", 30.0);
        // Impossible capabilities made every request fail to fit, with no
        // hint why (or, for rx_channels < 0, sized vectors negatively).
        const auto& cp  = dc.caps;
        auto        bad = [&](const std::string& what)
        {
            throw std::runtime_error("<device id=\"" + dc.id + "\">: " + what);
        };
        if (cp.rx_channels < 0 || cp.tx_channels < 0 || cp.rx_channels + cp.tx_channels == 0)
            bad("needs at least one rx or tx channel (and none negative)");
        if (!(cp.freq_min_hz >= 0.0 && cp.freq_min_hz < cp.freq_max_hz))
            bad("freq_min_hz must be >= 0 and < freq_max_hz");
        if (!(cp.sample_rate_max_sps > 0.0 && cp.sample_rate_min_sps <= cp.sample_rate_max_sps))
            bad("sample_rate_min_sps must be <= sample_rate_max_sps (> 0)");
        if (!(cp.bandwidth_max_hz > 0.0))
            bad("bandwidth_max_hz must be > 0");
        cfg.devices.push_back(std::move(dc));
    }
    if (cfg.devices.empty())
        throw std::runtime_error("No <device> entries found");
    for (size_t i = 0; i < cfg.devices.size(); ++i)
        for (size_t j = i + 1; j < cfg.devices.size(); ++j)
            if (cfg.devices[i].id == cfg.devices[j].id)
                throw std::runtime_error("Duplicate <device id=\"" + cfg.devices[i].id
                                         + "\"> — device ids must be unique");
    return cfg;
}

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
