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
#include "FakeSoapyDevice.hpp"

#include "FakeSoapyControl.hpp"

#include <SoapySDR/Registry.hpp>

#include <mutex>
#include <stdexcept>
#include <string>

// ── Global control variable definitions ──────────────────────────────────────
namespace FakeSoapy
{
std::atomic<bool>   fail_open{false};
std::atomic<bool>   fail_read{false};
std::atomic<bool>   overflow_next{false};
std::atomic<int>    timeout_count{0};
std::atomic<int>    samples_per_read{256};
std::atomic<float>  sample_value{0.0f};
std::atomic<double> reported_temp{25.0};
std::atomic<int>    read_delay_us{500};  // 0.5 ms — throttles IQStreamer threads
std::atomic<float>  channel_step{0.0f};
std::atomic<double> tone_hz{0.0};
std::atomic<float>  tone_amplitude{0.5f};
std::atomic<bool>   hw_time{false};
std::atomic<int>    activate_delay_us{0};
std::atomic<int>    activate_delay_calls{0};
std::atomic<bool>   activate_delay_limited{false};
std::atomic<int>    max_concurrent_reads{0};
std::atomic<int>    tx_freq_set_count{0};
std::atomic<double> last_rx_rate{0.0};
std::atomic<int>    rx_rate_set_count{0};
std::atomic<int>    rx_freq_set_count{0};
std::atomic<int>    tune_delay_us{0};
std::atomic<int>    sensor_delay_us{0};
std::atomic<int>    make_delay_us{0};
std::atomic<int>    stream_mtu{1024};
std::atomic<bool>   realtime_stream{false};
std::atomic<int>    tune_cal_delay_us{0};
std::atomic<int>    open_streams{0};
std::atomic<int>    fail_setup_stream_at{-1};
std::atomic<int>    setup_stream_calls{0};
std::atomic<int>    pipeline_reads{0};
std::atomic<int>    fail_setup_stream_count{0};
static std::mutex   g_args_mu;
static std::string  g_last_args;

std::string lastMakeArgs()
{
    std::lock_guard lk(g_args_mu);
    return g_last_args;
}

void setLastMakeArgs(std::string a)
{
    std::lock_guard lk(g_args_mu);
    g_last_args = std::move(a);
}

void reset()
{
    fail_open.store(false);
    fail_read.store(false);
    overflow_next.store(false);
    timeout_count.store(0);
    samples_per_read.store(256);
    sample_value.store(0.0f);
    reported_temp.store(25.0);
    read_delay_us.store(500);
    channel_step.store(0.0f);
    tone_hz.store(0.0);
    tone_amplitude.store(0.5f);
    hw_time.store(false);
    activate_delay_us.store(0);
    activate_delay_calls.store(0);
    activate_delay_limited.store(false);
    max_concurrent_reads.store(0);
    tx_freq_set_count.store(0);
    last_rx_rate.store(0.0);
    rx_rate_set_count.store(0);
    rx_freq_set_count.store(0);
    tune_delay_us.store(0);
    sensor_delay_us.store(0);
    make_delay_us.store(0);
    stream_mtu.store(1024);
    realtime_stream.store(false);
    tune_cal_delay_us.store(0);
    open_streams.store(0);
    fail_setup_stream_at.store(-1);
    setup_stream_calls.store(0);
    pipeline_reads.store(0);
    fail_setup_stream_count.store(0);
    setLastMakeArgs("");
}
}  // namespace FakeSoapy

// ── SoapySDR plugin registration ──────────────────────────────────────────────
static SoapySDR::KwargsList findFake(const SoapySDR::Kwargs& args)
{
    if (FakeSoapy::fail_open.load())
        return {};
    SoapySDR::Kwargs k;
    k["driver"] = "fake";
    k["label"]  = "Fake SDR Device (unit-test)";
    // Echo the unit's identity, as real drivers return its serial / uri:
    // SoapySDR::Device::make() caches devices by the find() result, so
    // without this every "serial=N" fake was the same cached instance.
    for (const char* key : {"serial", "uri", "addr"})
        if (auto it = args.find(key); it != args.end())
            k[key] = it->second;
    return {k};
}

static SoapySDR::Device* makeFake(const SoapySDR::Kwargs& args)
{
    if (FakeSoapy::fail_open.load())
        throw std::runtime_error("FakeSoapy: forced open failure");
    std::string a;
    for (auto& [k, v] : args)
        a += (a.empty() ? "" : ",") + k + "=" + v;
    FakeSoapy::setLastMakeArgs(a);
    if (int d = FakeSoapy::make_delay_us.load(); d > 0)
        std::this_thread::sleep_for(std::chrono::microseconds(d));
    return new FakeSoapyDevice(args);
}

// Static constructor registers "fake" driver before any test runs.
static SoapySDR::Registry gFakeRegistry("fake", &findFake, &makeFake, SOAPY_SDR_ABI_VERSION);

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
