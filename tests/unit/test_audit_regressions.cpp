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
// ════════════════════════════════════════════════════════════════════════════
//  test_audit_regressions.cpp
//
//  One test per finding from the 2026-11 software-only audit (finding IDs
//  match _savepoints/FINDINGS.md). Every test asserts the behaviour the ICD
//  (docs/ICD.md) or the physics requires, and was run against the
//  unmodified main/1.0 code first to confirm it fails there.
//
//  No hardware: everything runs on the in-process "fake" SoapySDR driver.
//  Several tests measure the delivered IQ itself (e.g. the frequency of a
//  known tone computed with the sample rate advertised in the packet header)
//  so bookkeeping errors cannot hide behind self-consistent metadata.
// ════════════════════════════════════════════════════════════════════════════
#include "ConfigParser.hpp"
#include "Ddc.hpp"
#include "FakeSoapyControl.hpp"
#include "FftEngine.hpp"
#include "IQStreamer.hpp"
#include "RadioDevice.hpp"
#include "ResourceManager.hpp"
#include "ScanExecutor.hpp"
#include "SpectrumTimeline.hpp"
#include "sdr/Types.hpp"

#include <SoapySDR/Device.hpp>
#include <SoapySDR/Formats.hpp>
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <complex>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <future>
#include <limits>
#include <thread>
#include <vector>

using namespace sdr;
using namespace std::chrono;

namespace
{

int64_t epochMs()
{
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void noopState(const TaskRecord&)
{
}

void noopErr(const std::string&, const std::string&)
{
}

// Same shape as test_resource_manager.cpp's makeTestConfig, but each test can
// give its own port range so ctest's parallel processes never collide.
AppConfig cfgA(int                             n_devices = 1,
               int                             port_lo   = 46'000,
               int                             port_hi   = 46'199,
               const std::vector<std::string>& ids       = {})
{
    AppConfig cfg;
    cfg.policy.max_concurrent_tasks    = 32;
    cfg.policy.guard_band_hz           = 200e3;
    cfg.policy.usable_bw_fraction      = 0.80;
    cfg.policy.default_task_timeout_ms = 60'000;
    cfg.policy.udp_port_pool_start     = port_lo;
    cfg.policy.udp_port_pool_end       = port_hi;
    cfg.policy.iq_packet_samples       = 128;
    cfg.policy.heartbeat_interval_ms   = 10'000;
    for (int i = 0; i < n_devices; ++i)
    {
        DeviceConfig dc;
        dc.id                       = ids.empty() ? "fake-" + std::to_string(i) : ids[(size_t)i];
        dc.driver                   = "fake";
        dc.uri                      = "fake";
        dc.label                    = dc.id;
        dc.streaming_source_ip      = "127.0.0.1";
        dc.coherency_group          = "grp";
        dc.caps.rx_channels         = 2;
        dc.caps.tx_channels         = 2;
        dc.caps.freq_min_hz         = 70e6;
        dc.caps.freq_max_hz         = 6e9;
        dc.caps.bandwidth_max_hz    = 56e6;
        dc.caps.sample_rate_max_sps = 61.44e6;
        cfg.devices.push_back(dc);
    }
    return cfg;
}

TaskRequest continuousReq(
    const std::string& id, double cf, double bw, double sr, int rx, std::vector<int> ports = {})
{
    TaskRequest r;
    r.msg_type             = "TASK_REQUEST_CONTINUOUS";
    r.request_id           = id;
    r.schema_version       = "2.0";
    r.task_type            = TaskType::NARROWBAND;
    r.schedule_mode        = ScheduleMode::CONTINUOUS;
    r.start_time_ms        = epochMs();
    r.end_time_ms          = TIME_INFINITE;
    r.rf.center_freq_hz    = cf;
    r.rf.bandwidth_hz      = bw;
    r.rf.sample_rate_sps   = sr;
    r.rf.rx_count          = rx;
    r.rf.tx_count          = 0;
    r.streaming.dest_ip    = "127.0.0.1";
    r.streaming.dest_ports = std::move(ports);
    return r;
}

TaskRequest snapshotReq(int fft, int avg)
{
    TaskRequest r;
    r.msg_type   = "TASK_REQUEST_SNAPSHOT";
    r.request_id = "snap";
    r.task_type  = TaskType::SNAPSHOT;
    SnapshotParams sp;
    sp.center_freq_hz  = 915e6;
    sp.bandwidth_hz    = 1e6;
    sp.sample_rate_sps = 1e6;
    sp.fft_size        = fft;
    sp.n_averages      = avg;
    r.snapshot_params  = sp;
    return r;
}

TaskRequest calibrationReq(std::vector<std::string> devices)
{
    TaskRequest r;
    r.msg_type   = "TASK_REQUEST_CALIBRATION";
    r.request_id = "cal";
    r.task_type  = TaskType::CALIBRATION;
    CalibrationParams cp;
    cp.center_freq_hz      = 915e6;
    cp.bandwidth_hz        = 1e6;
    cp.sample_rate_sps     = 1e6;
    cp.duration_ms         = 2000;
    cp.rx_count_per_device = 1;
    cp.coherency_group     = "grp";
    cp.devices             = std::move(devices);
    r.cal_params           = cp;
    r.streaming.dest_ip    = "127.0.0.1";
    return r;
}

ScanEntry entry(double cf, double sr, int dwell_ms)
{
    ScanEntry e;
    e.center_freq_hz  = cf;
    e.bandwidth_hz    = sr * 0.8;
    e.sample_rate_sps = sr;
    e.dwell_ms        = dwell_ms;
    return e;
}

// Pre-bound loopback UDP receiver (the ICD's "dest_ports" pattern).
struct UdpRx
{
    int fd = -1, port = 0;

    explicit UdpRx(int fixed_port = 0)
    {
        fd     = ::socket(AF_INET, SOCK_DGRAM, 0);
        int rb = 4 << 20;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rb, sizeof rb);
        sockaddr_in a{};
        a.sin_family      = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port        = htons((uint16_t)fixed_port);
        if (::bind(fd, (sockaddr*)&a, sizeof a) != 0)
        {
            ::close(fd);
            fd = -1;
            return;
        }
        socklen_t l = sizeof a;
        ::getsockname(fd, (sockaddr*)&a, &l);
        port = ntohs(a.sin_port);
    }

    ~UdpRx()
    {
        if (fd >= 0)
            ::close(fd);
    }

    bool recv(IqPacketHeader& h, std::vector<std::complex<float>>& iq, int timeout_ms = 1500)
    {
        pollfd p{fd, POLLIN, 0};
        if (fd < 0 || ::poll(&p, 1, timeout_ms) <= 0)
            return false;
        static thread_local uint8_t buf[65'536];
        ssize_t                     n = ::recv(fd, buf, sizeof buf, 0);
        if (n < (ssize_t)sizeof h)
            return false;
        std::memcpy(&h, buf, sizeof h);
        iq.resize(h.num_samples);
        std::memcpy(iq.data(), buf + sizeof h, (size_t)h.num_samples * 8);
        return true;
    }

    // Discard whatever is queued, for at most `ms` (a healthy stream never goes idle).
    void drain(int ms = 30)
    {
        IqPacketHeader                   h{};
        std::vector<std::complex<float>> pkt;
        auto                             dl = steady_clock::now() + milliseconds(ms);
        while (steady_clock::now() < dl && recv(h, pkt, 5))
        {
        }
    }

    // Collect ≥ n samples of non-overflow payload; returns the last header.
    bool collect(size_t                            n,
                 std::vector<std::complex<float>>& out,
                 IqPacketHeader&                   last,
                 int                               timeout_ms = 3000)
    {
        auto                             dl = steady_clock::now() + milliseconds(timeout_ms);
        std::vector<std::complex<float>> pkt;
        while (out.size() < n && steady_clock::now() < dl)
        {
            if (!recv(last, pkt, 200))
                continue;
            if (last.flags & IQ_FLAG_OVERFLOW)
                continue;
            out.insert(out.end(), pkt.begin(), pkt.end());
        }
        return out.size() >= n;
    }
};

// Frequency of a single complex tone, using the sample rate the stream claims.
double toneHz(const std::vector<std::complex<float>>& x, double fs)
{
    std::complex<double> acc{0, 0};
    for (size_t i = 1; i < x.size(); ++i)
        acc += std::complex<double>(x[i]) * std::conj(std::complex<double>(x[i - 1]));
    return std::arg(acc) * fs / (2.0 * M_PI);
}

template <class F>
bool waitUntil(F f, int timeout_ms)
{
    auto dl = steady_clock::now() + milliseconds(timeout_ms);
    while (steady_clock::now() < dl)
    {
        if (f())
            return true;
        std::this_thread::sleep_for(milliseconds(5));
    }
    return f();
}

}  // namespace

// ═══ RM-20 — calibration with an unknown device id crashes the controller ═══
// doAcceptCalibration skipped unknown ids in the fit check, then evaluated
// devices_[id]->config() / timelines_[id]->insert(): operator[] default-inserts
// a null unique_ptr and dereferences it. Any AMQP client could crash the RM.
TEST(AuditRegression, RM20_CalibrationWithUnknownDeviceIsRejected)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(2), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    auto resp = rm.tryAccept(calibrationReq({"fake-0", "no-such-device"}));
    EXPECT_FALSE(resp.accepted);
    EXPECT_EQ(resp.reject_code, RejectCode::INVALID_REQUEST);
    // The device maps must not have grown a null entry.
    EXPECT_EQ(rm.deviceIds().size(), 2u);
    EXPECT_EQ(rm.deviceSummaries().size(), 2u);
}

TEST(AuditRegression, RM20_CalibrationOnOfflineDeviceIsRejectedUpFront)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(2), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    rm.getDevice("fake-1")->close();
    auto resp = rm.tryAccept(calibrationReq({"fake-0", "fake-1"}));
    EXPECT_FALSE(resp.accepted);
    EXPECT_EQ(resp.reject_code, RejectCode::DEVICE_OFFLINE);
}

// ═══ RM-19 — snapshot hangs on a stalled device / reports zero-padded data ═══
TEST(AuditRegression, RM19_SnapshotGivesUpOnStalledDevice)
{
    FakeSoapy::reset();
    FakeSoapy::read_delay_us.store(200);
    ResourceManager rm(cfgA(1), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    FakeSoapy::timeout_count.store(std::numeric_limits<int>::max());  // every read times out
    auto       fut      = std::async(std::launch::async,
                          [&]
                          {
                              return rm.tryAccept(snapshotReq(1024, 4));
                          });
    const bool finished = fut.wait_for(seconds(8)) == std::future_status::ready;
    FakeSoapy::timeout_count.store(0);  // un-stick a hung loop so the test can finish
    auto resp = fut.get();
    EXPECT_TRUE(finished) << "snapshot never returned while the device was stalled";
    EXPECT_FALSE(resp.accepted) << "a stalled device must not produce a snapshot";
}

TEST(AuditRegression, RM19_SnapshotWithReadErrorsIsNotReportedAsSuccess)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    FakeSoapy::fail_read.store(true);  // every read fails → loop breaks after 5 errors
    auto resp = rm.tryAccept(snapshotReq(1024, 4));
    EXPECT_FALSE(resp.accepted) << "FFT of an all-zero buffer was reported as a valid snapshot";
}

// ═══ RM-60 — scan with every retune failing ═══
TEST(AuditRegression, RM60_RepeatScanWithAllRetunesFailingStopsAndFails)
{
    std::atomic<long> calls{0};
    std::atomic<bool> done{false}, ok{true};
    ScanParams        p;
    p.repeat  = true;
    p.entries = {entry(100e6, 1e6, 20), entry(200e6, 1e6, 20)};
    ScanExecutor ex(
        "scan-fail",
        p,
        [&](double, double)
        {
            ++calls;
            return false;
        },
        {},
        [&](const std::string&, bool o)
        {
            ok   = o;
            done = true;
        });
    ex.start();
    EXPECT_TRUE(waitUntil(
        [&]
        {
            return done.load();
        },
        2000))
        << "repeat scan kept spinning; retune calls so far = " << calls.load();
    ex.stop();
    EXPECT_FALSE(ok.load());
    EXPECT_LT(calls.load(), 100) << "retune hammered " << calls.load() << " times";
}

// ═══ RM-35 — header sample rate not updated on scan entry change (ICD §8.4) ═══
TEST(AuditRegression, RM35_ScanUpdatesStreamSampleRateOnEntryChange)
{
    FakeSoapy::reset();
    SoapySDR::Kwargs  k{{"driver", "fake"}};
    SoapySDR::Device* dev = SoapySDR::Device::make(k);
    ASSERT_NE(dev, nullptr);
    auto*              s = dev->setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, {0});
    IQStreamer::Config sc;
    sc.task_id        = "t";
    sc.stream_id      = "t-rx0";
    sc.sample_rate    = 1e6;
    sc.packet_samples = 128;
    {
        IQStreamer st(sc, dev, s);
        st.start();  // no destinations: reads and discards
        st.updateSampleRate(1e6);
        std::atomic<bool> done{false};
        ScanParams        p;
        p.repeat  = false;
        p.entries = {entry(100e6, 1e6, 20), entry(200e6, 2e6, 20)};
        ScanExecutor ex(
            "t",
            p,
            [](double, double)
            {
                return true;
            },
            {&st},
            [&](const std::string&, bool)
            {
                done = true;
            });
        ex.start();
        ASSERT_TRUE(waitUntil(
            [&]
            {
                return done.load();
            },
            2000));
        ex.stop();
        st.stop();
        EXPECT_DOUBLE_EQ(st.currentCF(), 200e6);
        EXPECT_DOUBLE_EQ(st.currentSR(), 2e6)
            << "header still advertises the previous entry's rate";
    }
    dev->closeStream(s);
    SoapySDR::Device::unmake(dev);
}

// ═══ RM-11 — ScanExecutor busy-waits the last 10 ms of every dwell ═══
TEST(AuditRegression, RM11_ScanDwellDoesNotBusyWait)
{
    ScanParams p;
    p.repeat  = true;
    p.entries = {entry(100e6, 1e6, 20), entry(200e6, 1e6, 20)};
    ScanExecutor ex(
        "t",
        p,
        [](double, double)
        {
            return true;
        },
        {},
        [](const std::string&, bool)
        {
        });
    const std::clock_t c0 = std::clock();
    const auto         w0 = steady_clock::now();
    ex.start();
    std::this_thread::sleep_for(milliseconds(800));
    ex.stop();
    const double cpu_s  = double(std::clock() - c0) / CLOCKS_PER_SEC;
    const double wall_s = duration<double>(steady_clock::now() - w0).count();
    EXPECT_LT(cpu_s / wall_s, 0.15)
        << "scan thread burned " << cpu_s << " s CPU in " << wall_s << " s of 20 ms dwells";
}

// ═══ RM-16 — a scan hops the shared LO but only reserves entry[0] ═══
TEST(AuditRegression, RM16_TaskCannotShareTheLoOfARunningScan)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    TaskRequest scan;
    scan.msg_type          = "TASK_REQUEST_SCAN";
    scan.request_id        = "scan";
    scan.task_type         = TaskType::SCAN;
    scan.rf.rx_count       = 1;
    scan.streaming.dest_ip = "127.0.0.1";
    ScanParams sp;
    sp.repeat  = true;
    sp.entries = {entry(100e6, 10e6, 50), entry(300e6, 10e6, 50)};
    for (auto& e : sp.entries)
        e.bandwidth_hz = 1e6;  // leave room in the 10 MHz window
    scan.scan_params = sp;
    ASSERT_TRUE(rm.tryAccept(scan).accepted);
    // Same CF/SR as entry[0], free channel 1 on the same shared-LO device.
    auto resp = rm.tryAccept(continuousReq("victim", 100e6, 0.5e6, 10e6, 1));
    EXPECT_FALSE(resp.accepted) << "accepted onto a shared LO that the scan retunes every 50 ms";
}

// ═══ RM-01 — future reservations at different CFs ═══
TEST(AuditRegression, RM01_RequestSpanningTwoReservationsAtDifferentCfsIsRejected)
{
    SpectrumTimeline tl;
    const int64_t    t0 = 1'000'000;
    TimeFreqSlot     a;
    a.task_id         = "A";
    a.t_start         = t0;
    a.t_stop          = t0 + 1000;
    a.center_freq_hz  = 100e6;
    a.sample_rate_sps = 10e6;
    a.slice_lo_hz     = 99.5e6;
    a.slice_hi_hz     = 100.5e6;
    a.rx_channels     = {0};
    TimeFreqSlot b    = a;
    b.task_id         = "B";
    b.t_start         = t0 + 1000;
    b.t_stop          = t0 + 2000;
    b.center_freq_hz  = 200e6;
    b.slice_lo_hz     = 199.5e6;
    b.slice_hi_hz     = 200.5e6;
    tl.insert(a);
    tl.insert(b);
    // Overlaps A (100 MHz) then B (200 MHz); a shared LO cannot serve both.
    auto fit = tl.canFit(t0 + 500,
                         t0 + 1500,
                         100e6,
                         1e6,
                         10e6,
                         1,
                         0,
                         /*max_rx=*/4,
                         /*max_tx=*/0,
                         200e3,
                         /*shared_lo=*/true);
    EXPECT_FALSE(fit.ok);
    EXPECT_EQ(fit.reject_code, RejectCode::RETUNE_CONFLICT);
}

// ═══ RM-02 — channel usage double-counted ═══
TEST(AuditRegression, RM02_FanOutOnChannel0DoesNotConsumeChannel1)
{
    SpectrumTimeline tl;
    TimeFreqSlot     a;
    a.task_id         = "A";
    a.t_start         = 0;
    a.t_stop          = TIME_INFINITE;
    a.center_freq_hz  = 100e6;
    a.sample_rate_sps = 10e6;
    a.slice_lo_hz     = 99.5e6;
    a.slice_hi_hz     = 100.5e6;
    a.rx_channels     = {0};
    TimeFreqSlot fan  = a;
    fan.task_id       = "fan";
    fan.slice_lo_hz   = 101e6;
    fan.slice_hi_hz   = 101.2e6;
    tl.insert(a);
    tl.insert(fan);  // two consumers of the same physical channel 0
    auto fit = tl.canFit(1000, TIME_INFINITE, 100e6, 1e6, 10e6, 1, 0, 2, 0, 200e3, true);
    ASSERT_TRUE(fit.ok) << fit.reject_reason;
    ASSERT_EQ(fit.avail_rx.size(), 1u);
    EXPECT_EQ(fit.avail_rx[0], 1);
}

TEST(AuditRegression, RM02_SequentialSlotsOnOneChannelLeaveTheOtherFree)
{
    SpectrumTimeline tl;
    TimeFreqSlot     a;
    a.task_id         = "A";
    a.t_start         = 0;
    a.t_stop          = 1000;
    a.center_freq_hz  = 100e6;
    a.sample_rate_sps = 10e6;
    a.slice_lo_hz     = 99.5e6;
    a.slice_hi_hz     = 100.5e6;
    a.rx_channels     = {0};
    TimeFreqSlot b    = a;
    b.task_id         = "B";
    b.t_start         = 1000;
    b.t_stop          = 2000;
    tl.insert(a);
    tl.insert(b);
    auto fit = tl.canFit(500, 1500, 100e6, 1e6, 10e6, 1, 0, 2, 0, 200e3, /*shared_lo=*/false);
    ASSERT_TRUE(fit.ok) << fit.reject_reason;
    EXPECT_EQ(fit.avail_rx, std::vector<int>{1});
}

// ═══ RM-22 — combined retune side effects survive a rejected request ═══
TEST(AuditRegression, RM22_RejectedCombinedRetuneLeavesHardwareUntouched)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(2, 46'200, 46'201), noopState, noopErr);  // only 2 UDP ports
    ASSERT_EQ(rm.openDevices(), 2);
    auto a = rm.tryAccept(continuousReq("A", 100e6, 1e6, 2e6, 1));
    ASSERT_TRUE(a.accepted);
    const std::string dev_a = a.streams.at(0).device_id;
    const std::string dev_b = dev_a == "fake-0" ? "fake-1" : "fake-0";
    auto              b     = continuousReq("B", 500e6, 1e6, 2e6, 1);
    b.rf.required_device    = dev_b;
    ASSERT_TRUE(rm.tryAccept(b).accepted);  // port pool now exhausted
    waitUntil(
        [&]
        {
            return rm.countByState(TaskState::RUNNING) == 2;
        },
        2000);
    const double cf_before = rm.getDevice(dev_a)->currentCF();
    // Near A on the same device → only satisfiable by widening A's window.
    auto c = rm.tryAccept(continuousReq("C", 101.5e6, 1e6, 2e6, 1));
    EXPECT_FALSE(c.accepted);
    EXPECT_EQ(c.reject_code, RejectCode::PORT_POOL_EXHAUSTED);
    EXPECT_DOUBLE_EQ(rm.getDevice(dev_a)->currentCF(), cf_before)
        << "device was retuned for a request that was then rejected";
}

// ═══ RM-23 — combined retune silently changes an existing client's rate ═══
TEST(AuditRegression, RM23_CombinedRetunePreservesExistingClientsSampleRate)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1, 46'210, 46'229), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx rx_a;
    ASSERT_TRUE(rm.tryAccept(continuousReq("A", 100e6, 1e6, 2e6, 1, {rx_a.port})).accepted);
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.countByState(TaskState::RUNNING) == 1;
        },
        2000));
    auto c = rm.tryAccept(continuousReq("C", 101.5e6, 1e6, 2e6, 1));
    ASSERT_TRUE(c.accepted) << c.reject_reason;
    // Drain packets that predate the retune, then look at A's stream.
    std::this_thread::sleep_for(milliseconds(100));
    rx_a.drain();
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> pkt;
    ASSERT_TRUE(rx_a.recv(h, pkt, 1500));
    EXPECT_EQ(h.sample_rate, 2'000'000u)
        << "task A asked for 2 MS/s and now receives " << h.sample_rate << " S/s";
}

// ═══ RM-58 — TX is reserved and advertised but never activated ═══
TEST(AuditRegression, RM58_TxRequestIsNotAcceptedWhileTxIsUnavailable)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    auto r        = continuousReq("tx", 915e6, 1e6, 1e6, 1);
    r.rf.tx_count = 1;
    auto resp     = rm.tryAccept(r);
    EXPECT_FALSE(resp.accepted) << "TX stream advertised on a port nothing listens on";
    EXPECT_EQ(resp.reject_code, RejectCode::CHANNEL_COUNT_EXCEEDED);
}

// ═══ RM-12 — coherent multi-device path ═══
TEST(AuditRegression, RM12_CoherentRequestWithTxIsNotSilentlyDropped)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(2), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    auto r               = continuousReq("coh-tx", 915e6, 1e6, 1e6, 2);
    r.rf.coherency_group = "grp";
    r.rf.tx_count        = 1;
    auto resp            = rm.tryAccept(r);
    EXPECT_FALSE(resp.accepted) << "accepted with " << resp.streams.size()
                                << " RX streams and no TX";
}

TEST(AuditRegression, RM12_CoherentAllocationSkipsDevicesThatCannotTune)
{
    FakeSoapy::reset();
    auto cfg                        = cfgA(3);
    cfg.devices[0].caps.freq_max_hz = 1e9;  // fake-0 cannot reach 2.4 GHz
    ResourceManager rm(cfg, noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 3);
    auto r               = continuousReq("coh-24", 2.4e9, 1e6, 1e6, 4);
    r.rf.coherency_group = "grp";
    auto resp            = rm.tryAccept(r);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    for (auto& s : resp.streams)
        EXPECT_NE(s.device_id, "fake-0") << "allocated a device whose range ends at 1 GHz";
}

// Order-independent variant: only 2 of the 4 group channels can reach 2.4 GHz.
TEST(AuditRegression, RM12_CoherentRequestCountsOnlyCapableDevices)
{
    FakeSoapy::reset();
    auto cfg                        = cfgA(2);
    cfg.devices[0].caps.freq_max_hz = 1e9;
    ResourceManager rm(cfg, noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    auto r               = continuousReq("coh-4", 2.4e9, 1e6, 1e6, 4);
    r.rf.coherency_group = "grp";
    auto resp            = rm.tryAccept(r);
    EXPECT_FALSE(resp.accepted) << "4 coherent channels promised; only 2 can tune 2.4 GHz";
    EXPECT_EQ(resp.reject_code, RejectCode::COHERENCY_UNAVAILABLE);
}

TEST(AuditRegression, RM12_CoherentStreamsAreOrderedByConfiguration)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(3, 46'000, 46'199, {"zulu", "alpha", "mike"}), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 3);
    auto r               = continuousReq("coh-order", 915e6, 1e6, 1e6, 6);
    r.rf.coherency_group = "grp";
    auto resp            = rm.tryAccept(r);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    ASSERT_EQ(resp.streams.size(), 6u);
    const char* want[] = {"zulu", "zulu", "alpha", "alpha", "mike", "mike"};
    for (size_t i = 0; i < 6; ++i)
    {
        EXPECT_EQ(resp.streams[i].device_id, want[i]) << "stream " << i;
        EXPECT_EQ(resp.streams[i].channel_index, (int)(i % 2)) << "stream " << i;
    }
}

TEST(AuditRegression, RM12_CoherentRequestHonoursPreboundDestPorts)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(2), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    UdpRx r0, r1, r2, r3;
    auto  r = continuousReq("coh-ports", 915e6, 1e6, 1e6, 4, {r0.port, r1.port, r2.port, r3.port});
    r.rf.coherency_group = "grp";
    auto resp            = rm.tryAccept(r);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    ASSERT_EQ(resp.streams.size(), 4u);
    EXPECT_EQ(resp.streams[0].udp_port, r0.port);
    EXPECT_EQ(resp.streams[3].udp_port, r3.port);
}

// ═══ RM-27 — fixed_sample_rate_hz: hardware rate ≠ advertised rate ═══
// The fake emits a tone at +100 kHz of the rate the controller programmed.
// Measured with the rate in the packet header it must still read 100 kHz.
TEST(AuditRegression, RM27_FixedSampleRateIsDecimatedToTheRequestedRate)
{
    FakeSoapy::reset();
    FakeSoapy::tone_hz.store(100e3);
    FakeSoapy::samples_per_read.store(1024);
    auto cfg                            = cfgA(1, 46'240, 46'259);
    cfg.devices[0].fixed_sample_rate_hz = 8e6;
    ResourceManager rm(cfg, noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx rx;
    auto  resp = rm.tryAccept(continuousReq("fixed", 915e6, 0.8e6, 1e6, 1, {rx.port}));
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    std::vector<std::complex<float>> iq;
    IqPacketHeader                   h{};
    ASSERT_TRUE(rx.collect(4096, iq, h));
    EXPECT_DOUBLE_EQ(FakeSoapy::last_rx_rate.load(), 8e6);
    EXPECT_EQ(h.sample_rate, 1'000'000u);
    EXPECT_NEAR(toneHz(iq, h.sample_rate), 100e3, 2e3)
        << "samples are not at the advertised rate (8 MS/s data labelled 1 MS/s?)";
}

// ═══ RM-07 — sub-band DDC always reads channel 0 ═══
TEST(AuditRegression, RM07_SubBandOnChannel1CarriesChannel1Samples)
{
    FakeSoapy::reset();
    FakeSoapy::sample_value.store(0.0f);
    FakeSoapy::channel_step.store(1.0f);  // ch0 = 0+0j, ch1 = 1+1j
    SoapySDR::Kwargs  k{{"driver", "fake"}};
    SoapySDR::Device* dev = SoapySDR::Device::make(k);
    ASSERT_NE(dev, nullptr);
    auto* s = dev->setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, {0, 1});
    UdpRx ch1_raw, sub;
    {
        IQStreamer::Config sc;
        sc.task_id        = "owner";
        sc.stream_id      = "owner-rx0";
        sc.sample_rate    = 1e6;
        sc.packet_samples = 128;
        IQStreamer::ChannelDest ec;
        ec.channel_index = 1;
        ec.stream_id     = "owner-rx1";
        ec.dest_ip       = "127.0.0.1";
        ec.dest_port     = ch1_raw.port;
        sc.extra_channels.push_back(ec);
        IQStreamer st(sc, dev, s);
        st.updateCenterFreq(100e6);
        st.addSubBand(
            "nb", "nb-rx1", /*channel_index=*/1, "127.0.0.1", sub.port, 100e6, 0.5e6, 1e6);
        st.start();
        std::vector<std::complex<float>> iq;
        IqPacketHeader                   h{};
        bool                             got = sub.collect(512, iq, h);
        st.stop();
        ASSERT_TRUE(got);
        EXPECT_EQ(h.channel_index, 1);
        std::complex<float> mean{0, 0};
        for (size_t i = 256; i < iq.size(); ++i)
            mean += iq[i];  // skip filter warm-up
        mean /= float(iq.size() - 256);
        EXPECT_NEAR(mean.real(), 1.0f, 0.05f) << "sub-band labelled ch1 carries ch0 samples";
    }
    dev->closeStream(s);
    SoapySDR::Device::unmake(dev);
}

// ═══ RM-55 — a triggered task stops another task's shared stream ═══
TEST(AuditRegression, RM55_TriggeredTaskDoesNotStopAnotherTasksStream)
{
    FakeSoapy::reset();
    FakeSoapy::sample_value.store(0.5f);  // always above the trigger threshold
    ResourceManager rm(cfgA(1, 46'260, 46'279), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx rx_a;
    ASSERT_TRUE(rm.tryAccept(continuousReq("A", 100e6, 1e6, 2e6, 1, {rx_a.port})).accepted);
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.countByState(TaskState::RUNNING) == 1;
        },
        2000));
    TaskRequest t          = continuousReq("T", 100e6, 1e6, 2e6, 1);
    t.msg_type             = "TASK_REQUEST_TRIGGERED";
    t.task_type            = TaskType::TRIGGERED;
    t.schedule_mode        = ScheduleMode::IMMEDIATE;
    t.duration_ms          = 5000;
    t.rf.preferred_channel = 0;
    TriggerParams tp;
    tp.threshold_dbfs  = -100.0;
    tp.post_trigger_ms = 50;
    tp.max_captures    = 1;
    t.trigger_params   = tp;
    rm.tryAccept(t);  // whether accepted on its own channel or rejected, A must survive
    std::this_thread::sleep_for(milliseconds(400));
    rx_a.drain();
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> pkt;
    EXPECT_TRUE(rx_a.recv(h, pkt, 1000)) << "task A's stream stopped after T's capture";
    EXPECT_LE(FakeSoapy::max_concurrent_reads.load(), 1)
        << "two threads read the same SoapySDR stream concurrently";
}

// ═══ RM-30 — coherent devices share no time base ═══
// ICD §10.1: timestamp_ns is "nanoseconds from task start". With 300 ms
// activations the second board starts streaming ~300 ms after the first, so
// its first timestamp must be ~300 ms larger — not ~0 like the first board's.
TEST(AuditRegression, RM30_CoherentStreamsShareTheTaskTimeBase)
{
    FakeSoapy::reset();
    FakeSoapy::activate_delay_us.store(300'000);
    ResourceManager rm(cfgA(2, 46'280, 46'299), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    // Coherent path ignores dest_ports on main/1.0 (RM-12), so pre-bind the
    // first four pool ports instead.
    UdpRx p0(46'280), p1(46'281), p2(46'282), p3(46'283);
    auto  r = continuousReq("coh-ts", 915e6, 1e6, 1e6, 4, {p0.port, p1.port, p2.port, p3.port});
    r.rf.coherency_group = "grp";
    auto resp            = rm.tryAccept(r);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    UdpRx*                   rx[4] = {&p0, &p1, &p2, &p3};
    double                   first_ts_ms[4];
    steady_clock::time_point first_rx[4];
    for (int i = 0; i < 4; ++i)
    {
        IqPacketHeader                   h{};
        std::vector<std::complex<float>> pkt;
        ASSERT_TRUE(rx[i]->recv(h, pkt, 3000)) << "no packets on stream " << i;
        first_ts_ms[i] = h.timestamp_ns / 1e6;
        first_rx[i]    = steady_clock::now();
    }
    // Stream 0 (first board) vs stream 2 (second board): the arrival-time
    // offset of the clocks they report must agree within 50 ms.
    auto rx_ms = [&](int i)
    {
        return duration<double, std::milli>(first_rx[i] - first_rx[0]).count();
    };
    const double skew = (rx_ms(2) - first_ts_ms[2]) - (rx_ms(0) - first_ts_ms[0]);
    EXPECT_LT(std::abs(skew), 50.0)
        << "board 2 timestamps are offset by " << skew << " ms from board 1 (per-device t0)";
}

// ═══ RM-08 — DDC anti-alias filter does not scale with decimation ═══
TEST(AuditRegression, RM08_DdcRejectsAliasesAtLargeDecimation)
{
    const double fs    = 10e6;
    const int    decim = 50;  // → 200 kS/s, Nyquist ±100 kHz
    auto         run   = [&](double tone_hz)
    {
        Ddc                ddc(0.0, fs, decim);
        const int          n = 400'000;
        std::vector<float> in(2 * (size_t)n), out(2 * (size_t)(n / decim + 2));
        for (int i = 0; i < n; ++i)
        {
            double ph     = 2.0 * M_PI * tone_hz * i / fs;
            in[2 * i]     = (float)std::cos(ph);
            in[2 * i + 1] = (float)std::sin(ph);
        }
        int    m   = ddc.process(in.data(), n, out.data());
        double p   = 0;
        int    cnt = 0;
        for (int i = m / 2; i < m; ++i, ++cnt)  // skip warm-up
            p += out[2 * i] * out[2 * i] + out[2 * i + 1] * out[2 * i + 1];
        return 10.0 * std::log10(p / cnt + 1e-30);
    };
    const double pass_db  = run(20e3);   // in band
    const double alias_db = run(250e3);  // folds to +50 kHz
    EXPECT_NEAR(pass_db, 0.0, 1.0);
    EXPECT_LT(alias_db - pass_db, -40.0)
        << "250 kHz tone aliases into a 200 kS/s output at " << alias_db - pass_db << " dB";
}

// ═══ RM-09 — non-integer decimation advertised at the requested rate ═══
TEST(AuditRegression, RM09_SubBandAdvertisesItsActualOutputRate)
{
    FakeSoapy::reset();
    FakeSoapy::tone_hz.store(300e3);
    FakeSoapy::samples_per_read.store(1024);
    SoapySDR::Kwargs  k{{"driver", "fake"}};
    SoapySDR::Device* dev = SoapySDR::Device::make(k);
    dev->setSampleRate(SOAPY_SDR_RX, 0, 10e6);
    auto* s = dev->setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, {0});
    UdpRx sub;
    {
        IQStreamer::Config sc;
        sc.task_id        = "o";
        sc.stream_id      = "o";
        sc.sample_rate    = 10e6;
        sc.packet_samples = 128;
        IQStreamer st(sc, dev, s);
        st.updateCenterFreq(100e6);
        st.addSubBand("nb", "nb", 0, "127.0.0.1", sub.port, 100e6, 3e6, 10e6);  // 10/3 → decim 3
        st.start();
        std::vector<std::complex<float>> iq;
        IqPacketHeader                   h{};
        bool                             got = sub.collect(8192, iq, h);
        st.stop();
        ASSERT_TRUE(got);
        EXPECT_NEAR(toneHz(iq, h.sample_rate), 300e3, 3e3)
            << "header says " << h.sample_rate << " S/s for a ÷3 stream from 10 MS/s";
    }
    dev->closeStream(s);
    SoapySDR::Device::unmake(dev);
}

// ═══ RM-56 — SNAPSHOT_RESULT never published (ICD §8.5 / §8.14) ═══
// The FFT was computed and discarded; the caller now gets the bins, and the
// spectral peak sits on the known tone (+100 kHz on a 1 MS/s capture).
TEST(AuditRegression, RM56_SnapshotReturnsPowerBinsWithToneAtTheRightFrequency)
{
    FakeSoapy::reset();
    FakeSoapy::tone_hz.store(100e3);
    FakeSoapy::read_delay_us.store(50);
    ResourceManager rm(cfgA(1), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    SnapshotResult snap;
    auto           resp = rm.tryAccept(snapshotReq(1024, 4), &snap);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    ASSERT_TRUE(snap.success) << snap.error_msg;
    ASSERT_EQ(snap.power_bins.size(), 1024u);
    size_t       peak    = (size_t)(std::max_element(snap.power_bins.begin(), snap.power_bins.end())
                           - snap.power_bins.begin());
    const double peak_hz = snap.freq_axis_start_hz + peak * snap.freq_axis_step_hz;
    EXPECT_NEAR(peak_hz - 915e6, 100e3, 2 * snap.freq_axis_step_hz);
}

// ═══ RM-61 — failed activation of board 2 leaks board 1's stream ═══
TEST(AuditRegression, RM61_FailedMultiDeviceActivationReleasesEverything)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(2, 46'300, 46'319), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    FakeSoapy::fail_setup_stream_at.store(1);  // board 1 opens, board 2 fails
    UdpRx p0, p1, p2, p3;
    auto  r = continuousReq("coh-fail", 915e6, 1e6, 1e6, 4, {p0.port, p1.port, p2.port, p3.port});
    r.rf.coherency_group = "grp";
    ASSERT_TRUE(rm.tryAccept(r).accepted);
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.countByState(TaskState::FAILED) == 1;
        },
        2000));
    EXPECT_TRUE(waitUntil(
        [&]
        {
            return FakeSoapy::open_streams.load() == 0;
        },
        1000))
        << FakeSoapy::open_streams.load() << " SoapySDR stream(s) left open by a failed task";
    p0.drain(50);
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> pkt;
    EXPECT_FALSE(p0.recv(h, pkt, 300)) << "board 1 still streams for a FAILED task";
    EXPECT_EQ(rm.udpPortsUsed(), 0);
}

// ═══ RM-63 — combined retune rewrote reservations outside its window ═══
TEST(AuditRegression, RM63_CombinedRetuneDoesNotRetargetLaterReservations)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1, 46'320, 46'339), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    auto imm = [&](const std::string& id, double cf)
    {
        auto r          = continuousReq(id, cf, 1e6, 2e6, 1);
        r.msg_type      = "TASK_REQUEST_IMMEDIATE";
        r.schedule_mode = ScheduleMode::IMMEDIATE;
        r.duration_ms   = 5000;
        return r;
    };
    auto sched =
        [&](const std::string& id, double cf, int64_t from_ms, int64_t len_ms, double bw = 1e6)
    {
        auto r          = continuousReq(id, cf, bw, 2e6, 1);
        r.msg_type      = "TASK_REQUEST_SCHEDULED";
        r.schedule_mode = ScheduleMode::SCHEDULED;
        r.start_time_ms = epochMs() + from_ms;
        r.end_time_ms   = r.start_time_ms + len_ms;
        return r;
    };
    ASSERT_TRUE(rm.tryAccept(imm("A", 100e6)).accepted);
    ASSERT_TRUE(rm.tryAccept(sched("F", 500e6, 60'000, 60'000)).accepted);  // later, elsewhere
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.countByState(TaskState::RUNNING) == 1;
        },
        2000));
    ASSERT_TRUE(rm.tryAccept(imm("C", 101.5e6)).accepted);  // combines with A
    // In F's window the LO is at 500 MHz: a same-CF request there must fit.
    // (200 kHz so it fits beside F's 1 MHz slice in the 2 MHz window.)
    const double live_cf = rm.getDevice("fake-0")->currentCF();
    auto         g       = rm.tryAccept(sched("G", 500e6, 65'000, 10'000, 0.2e6));
    EXPECT_TRUE(g.accepted) << "F's reservation was retargeted by the combine: " << g.reject_reason;
    // With F's slot corrupted, G was admitted by "combining" with it — which
    // retuned the live radio to 500 MHz under the running tasks A and C.
    EXPECT_DOUBLE_EQ(rm.getDevice("fake-0")->currentCF(), live_cf);
}

// ═══ RM-64 — combining two FUTURE reservations retuned the live device now ═══
TEST(AuditRegression, RM64_FutureCombineDoesNotTouchLiveHardware)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1, 46'400, 46'419), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    auto a          = continuousReq("A", 100e6, 1e6, 2e6, 1);
    a.msg_type      = "TASK_REQUEST_IMMEDIATE";
    a.schedule_mode = ScheduleMode::IMMEDIATE;
    a.duration_ms   = 5000;
    ASSERT_TRUE(rm.tryAccept(a).accepted);
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.countByState(TaskState::RUNNING) == 1;
        },
        2000));
    auto sched = [&](const std::string& id, double cf, int64_t from_ms)
    {
        auto r          = continuousReq(id, cf, 1e6, 2e6, 1);
        r.msg_type      = "TASK_REQUEST_SCHEDULED";
        r.schedule_mode = ScheduleMode::SCHEDULED;
        r.start_time_ms = epochMs() + from_ms;
        r.end_time_ms   = r.start_time_ms + 30'000;
        return r;
    };
    ASSERT_TRUE(rm.tryAccept(sched("F", 500e6, 60'000)).accepted);
    auto g = rm.tryAccept(sched("G", 501.5e6, 62'000));  // only fits by widening F's window
    ASSERT_TRUE(g.accepted) << g.reject_reason;
    EXPECT_DOUBLE_EQ(rm.getDevice("fake-0")->currentCF(), 100e6)
        << "live device retuned for reservations that start in a minute";
}

// ═══ RM-38 — registry never pruned ═══
TEST(AuditRegression, RM38_TerminalTasksArePruned)
{
    FakeSoapy::reset();
    auto cfg                              = cfgA(1, 46'340, 46'359);
    cfg.policy.terminal_task_retention_ms = 0;
    ResourceManager rm(cfg, noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    for (int i = 0; i < 20; ++i)
    {
        auto r    = continuousReq("t" + std::to_string(i), 915e6, 1e6, 1e6, 1);
        auto resp = rm.tryAccept(r);
        ASSERT_TRUE(resp.accepted);
        waitUntil(
            [&]
            {
                return rm.countByState(TaskState::RUNNING) == 1;
            },
            1000);
        rm.cancelTask(resp.task_id, "c", "done");
    }
    EXPECT_EQ(rm.countByState(TaskState::CANCELLED), 20);
    rm.watchdogTick();
    EXPECT_EQ(rm.countByState(TaskState::CANCELLED), 0) << "terminal records are kept forever";
}

// ═══ RM-62 — coherent scan only retuned the first board ═══
TEST(AuditRegression, RM62_CoherentScanRetunesEveryBoard)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(2, 46'360, 46'379), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    TaskRequest scan;
    scan.msg_type           = "TASK_REQUEST_SCAN";
    scan.request_id         = "cscan";
    scan.task_type          = TaskType::SCAN;
    scan.rf.rx_count        = 4;
    scan.rf.coherency_group = "grp";
    scan.streaming.dest_ip  = "127.0.0.1";
    ScanParams sp;
    sp.repeat        = false;
    sp.entries       = {entry(100e6, 1e6, 150), entry(300e6, 1e6, 400)};
    scan.scan_params = sp;
    auto resp        = rm.tryAccept(scan);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.getDevice("fake-0")->currentCF() == 300e6;
        },
        2000));
    EXPECT_DOUBLE_EQ(rm.getDevice("fake-1")->currentCF(), 300e6)
        << "board 2 never left the first scan entry";
}

// ═══ RM-65 — samples from the previous scan LO labelled with the new CF ═══
// The scan retuned and flagged the next packet DWELL_CHANGE + new CF at once;
// reads already in the driver pipeline (here: one read deep) still carried
// the previous entry's LO. The fake tags every sample with the LO it was
// captured at (CF / 1e9).
TEST(AuditRegression, RM65_ScanRetuneDoesNotLabelOldLoSamplesWithNewCf)
{
    FakeSoapy::reset();
    FakeSoapy::pipeline_reads.store(1);
    FakeSoapy::read_delay_us.store(1000);
    ResourceManager rm(cfgA(1, 46'400, 46'419), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx rx;
    ASSERT_GT(rx.port, 0);
    TaskRequest scan;
    scan.msg_type             = "TASK_REQUEST_SCAN";
    scan.request_id           = "lo-scan";
    scan.task_type            = TaskType::SCAN;
    scan.rf.rx_count          = 1;
    scan.streaming.dest_ip    = "127.0.0.1";
    scan.streaming.dest_ports = {rx.port};
    ScanParams sp;
    sp.repeat        = false;
    sp.entries       = {entry(100e6, 1e6, 150), entry(300e6, 1e6, 300)};
    scan.scan_params = sp;
    auto resp        = rm.tryAccept(scan);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> iq;
    int                              labelled_new = 0, wrong = 0;
    auto                             dl = steady_clock::now() + seconds(3);
    while (steady_clock::now() < dl && labelled_new < 20)
    {
        if (!rx.recv(h, iq, 200) || iq.empty())
            continue;
        if (h.center_freq_hz != 300'000'000ull)
            continue;
        ++labelled_new;
        if (std::abs(iq[0].real() - 0.3f) > 1e-4f)
            ++wrong;
    }
    ASSERT_GT(labelled_new, 0);
    EXPECT_EQ(wrong, 0) << wrong << " packet(s) labelled 300 MHz carried 100 MHz samples";
}

// ═══ RM-67 — throughput_mbps was megabits per *read*, not per second ═══
TEST(AuditRegression, RM67_ThroughputIsPerSecond)
{
    FakeSoapy::reset();
    FakeSoapy::read_delay_us.store(1000);
    SoapySDR::Kwargs  k{{"driver", "fake"}};
    SoapySDR::Device* dev = SoapySDR::Device::make(k);
    ASSERT_NE(dev, nullptr);
    auto*              s = dev->setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, {0});
    UdpRx              rx;
    IQStreamer::Config sc;
    sc.task_id        = "tp";
    sc.stream_id      = "tp-rx0";
    sc.sample_rate    = 1e6;
    sc.packet_samples = 128;
    sc.dest_ip        = "127.0.0.1";
    sc.dest_port      = rx.port;
    {
        IQStreamer st(sc, dev, s);
        st.start();
        std::this_thread::sleep_for(milliseconds(300));
        auto m1 = st.getMetrics();
        auto t1 = steady_clock::now();
        std::this_thread::sleep_for(milliseconds(1000));
        auto m2 = st.getMetrics();
        auto t2 = steady_clock::now();
        st.stop();
        const double secs = duration<double>(t2 - t1).count();
        const double pkts = (double)(m2.packets_sent - m1.packets_sent);
        const double mbps =
            ((double)(m2.samples_total - m1.samples_total) * 8.0 + pkts * IQ_PACKET_HEADER_SIZE)
            * 8.0 / secs / 1e6;
        ASSERT_GT(mbps, 1.0);
        EXPECT_NEAR(m2.throughput_mbps, mbps, 0.35 * mbps)
            << "reported " << m2.throughput_mbps << " Mbit/s, measured " << mbps;
    }
    dev->closeStream(s);
    SoapySDR::Device::unmake(dev);
}

// ═══ RM-68 — task stopped during hardware setup keeps streaming forever ═══
// stopTask() during activation found no runtime and returned; activateTask
// then inserted the runtime and started the stream with nobody left to stop it.
TEST(AuditRegression, RM68_StopDuringActivationReleasesTheStream)
{
    FakeSoapy::reset();
    FakeSoapy::activate_delay_us.store(400'000);
    ResourceManager rm(cfgA(1, 46'420, 46'439), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx rx;
    auto  resp = rm.tryAccept(continuousReq("orphan", 915e6, 200e3, 1e6, 1, {rx.port}));
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    std::this_thread::sleep_for(milliseconds(100));  // inside activateStream()
    rm.stopTask(resp.task_id, "stop-1", "test");
    std::this_thread::sleep_for(milliseconds(700));  // activation has finished
    rx.drain(50);
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> iq;
    EXPECT_FALSE(rx.recv(h, iq, 300)) << "stopped task is still streaming";
    EXPECT_TRUE(waitUntil(
        []
        {
            return FakeSoapy::open_streams.load() == 0;
        },
        1000))
        << FakeSoapy::open_streams.load() << " SoapySDR stream(s) still open";
}

// ═══ RM-69 — openRxStream failures re-made the device under a live stream ═══
// Two consecutive setupStream failures triggered reopen(), which unmakes the
// SoapySDR device that running IQStreamers read from (use-after-free).
TEST(AuditRegression, RM69_NoReopenWhileStreamsAreOpen)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1, 46'440, 46'459), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx rx;
    auto  resp = rm.tryAccept(continuousReq("live", 915e6, 200e3, 1e6, 1, {rx.port}));
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> iq;
    ASSERT_TRUE(rx.recv(h, iq, 2000));
    RadioDevice*      dev    = rm.getDevice("fake-0");
    SoapySDR::Device* before = dev->soapyDevice();
    FakeSoapy::fail_setup_stream_count.store(2);
    EXPECT_EQ(dev->openRxStream({1}), nullptr);
    EXPECT_EQ(dev->openRxStream({1}), nullptr);
    EXPECT_EQ(dev->soapyDevice(), before) << "device re-made under a running stream";
    rx.drain(20);
    EXPECT_TRUE(rx.recv(h, iq, 1000)) << "live stream died";
}

// ═══ RM-70 — sample rate not restored after a reopen ═══
TEST(AuditRegression, RM70_TuneAfterReopenProgramsTheRate)
{
    FakeSoapy::reset();
    DeviceConfig dc = cfgA(1).devices[0];
    RadioDevice  dev(dc);
    ASSERT_TRUE(dev.open());
    ASSERT_TRUE(dev.tune(100e6, 2e6));
    EXPECT_DOUBLE_EQ(FakeSoapy::last_rx_rate.load(), 2e6);
    ASSERT_TRUE(dev.reopen());
    FakeSoapy::last_rx_rate.store(0.0);  // the new device runs at its default
    ASSERT_TRUE(dev.tune(100e6, 2e6));
    EXPECT_DOUBLE_EQ(FakeSoapy::last_rx_rate.load(), 2e6)
        << "tune() skipped setSampleRate on the re-made device";
}

// ═══ RM-14 — kwargs-style <uri> for drivers that are not libiio ═══
TEST(AuditRegression, RM14_KwargsUriSelectsTheUnit)
{
    FakeSoapy::reset();
    DeviceConfig dc = cfgA(1).devices[0];
    dc.uri          = "serial=ABC123,type=b200";
    RadioDevice dev(dc);
    ASSERT_TRUE(dev.open());
    const std::string a = FakeSoapy::lastMakeArgs();
    EXPECT_NE(a.find("serial=ABC123"), std::string::npos) << a;
    EXPECT_NE(a.find("type=b200"), std::string::npos) << a;
    EXPECT_EQ(a.find("uri="), std::string::npos) << a;
    // libiio style is unchanged (close first: SoapySDR caches a device by its
    // enumerated kwargs, and the fake enumerates identically)
    dev.close();
    dc.uri = "ip:192.168.2.1";
    RadioDevice dev2(dc);
    ASSERT_TRUE(dev2.open());
    EXPECT_NE(FakeSoapy::lastMakeArgs().find("uri=ip:192.168.2.1"), std::string::npos)
        << FakeSoapy::lastMakeArgs();
}

// ═══ RM-15 — snapshot levels ignore the Hann coherent gain ═══
TEST(AuditRegression, RM15_SnapshotFullScaleToneIsZeroDbfs)
{
    const int          N = 1024, A = 4;
    std::vector<float> s((size_t)N * A * 2);
    for (int i = 0; i < N * A; ++i)
    {
        const double ph      = 2 * M_PI * 128.0 * i / N;  // bin-centred
        s[2 * (size_t)i]     = (float)std::cos(ph);
        s[2 * (size_t)i + 1] = (float)std::sin(ph);
    }
    FftEngine fe;
    auto      r = fe.compute(s, N, A, 915e6, 1e6, 1e6, "fake-0");
    ASSERT_TRUE(r.success) << r.error_msg;
    double peak = -1e9;
    for (double v : r.power_bins)
        peak = std::max(peak, v);
    EXPECT_NEAR(peak, 0.0, 0.1);
}

// ═══ RM-03 — slices are centred on the requested band (ICD §4.3 example) ═══
TEST(AuditRegression, RM03_SliceIsTheRequestedBand)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    auto resp = rm.tryAccept(continuousReq("A", 100e6, 100e3, 1e6, 1));
    ASSERT_TRUE(resp.accepted);
    EXPECT_DOUBLE_EQ(resp.streams[0].center_freq_hz + resp.streams[0].slice_offset_hz, 100e6)
        << "slice packed away from the requested frequency (offset "
        << resp.streams[0].slice_offset_hz << " Hz)";
}

// ═══ RM-07b — borrowed 2-channel subscription delivered channel 0 twice ═══
TEST(AuditRegression, RM07b_CombinedTwoChannelConsumersGetTheirOwnChannels)
{
    FakeSoapy::reset();
    FakeSoapy::sample_value.store(0.0f);
    FakeSoapy::channel_step.store(1.0f);  // ch0 = 0+0j, ch1 = 1+1j
    FakeSoapy::samples_per_read.store(1024);
    ResourceManager rm(cfgA(1, 46'380, 46'399), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx a0, a1, c0, c1;
    ASSERT_TRUE(
        rm.tryAccept(continuousReq("A", 100e6, 0.5e6, 2e6, 2, {a0.port, a1.port})).accepted);
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.countByState(TaskState::RUNNING) == 1;
        },
        2000));
    auto c = rm.tryAccept(continuousReq("C", 101.5e6, 0.5e6, 2e6, 2, {c0.port, c1.port}));
    ASSERT_TRUE(c.accepted) << c.reject_reason;
    std::this_thread::sleep_for(milliseconds(150));
    auto meanMag = [](UdpRx& rx)
    {
        rx.drain(20);
        std::vector<std::complex<float>> iq;
        IqPacketHeader                   h{};
        if (!rx.collect(8192, iq, h))
            return -1.0f;
        float m = 0;
        for (size_t i = iq.size() / 2; i < iq.size(); ++i)
            m += std::abs(iq[i]);
        return m / float(iq.size() - iq.size() / 2);
    };
    const float c0m = meanMag(c0), c1m = meanMag(c1);
    EXPECT_LT(c0m, 0.1f);
    EXPECT_GT(c1m, 0.5f) << "channel-1 stream of the combined task carries channel 0";
}

// ═══ CFG-03 / CFG-07 — configuration validation ═══
namespace
{
std::string writeXml(const std::string& devices, int packet_samples = 183)
{
    static int        n = 0;
    const std::string path =
        "/tmp/audit_cfg_" + std::to_string(::getpid()) + "_" + std::to_string(n++) + ".xml";
    std::ofstream f(path);
    f << "<?xml version=\"1.0\"?><sdr_controller><policy><iq_packet_samples>" << packet_samples
      << "</iq_packet_samples></policy><devices>" << devices << "</devices></sdr_controller>";
    return path;
}
}  // namespace

TEST(AuditRegression, CFG03_DuplicateDeviceIdsAreRejected)
{
    auto path = writeXml("<device id=\"pluto-0\"><driver>fake</driver></device>"
                         "<device id=\"pluto-0\"><driver>fake</driver></device>");
    EXPECT_THROW(ConfigParser::parse(path), std::runtime_error)
        << "second pluto-0 silently replaced the first";
}

TEST(AuditRegression, CFG07_PacketSizeBeyondUdpLimitIsRejected)
{
    auto ok  = writeXml("<device id=\"d\"><driver>fake</driver></device>", 8184);
    auto bad = writeXml("<device id=\"d\"><driver>fake</driver></device>", 9000);
    EXPECT_NO_THROW(ConfigParser::parse(ok));
    EXPECT_THROW(ConfigParser::parse(bad), std::runtime_error)
        << "9000 samples = 72032-byte datagrams: every send fails with EMSGSIZE";
}

// ═══ RM-71 — the scan dwell lost the reads discarded after a retune ═══
// The dwell timer started when retune() returned, but the streamer then
// drops `retune_settle_reads` reads (RM-65) — inside the dwell. With slow
// driver reads (here 20 ms, 3 settle reads) a 100 ms dwell delivered ~2 reads;
// with an MTU-sized read longer than the dwell an entry delivered nothing.
TEST(AuditRegression, RM71_ScanDwellExcludesTheRetuneDrain)
{
    FakeSoapy::reset();
    FakeSoapy::read_delay_us.store(20'000);
    auto cfg                           = cfgA(1, 46'460, 46'479);
    cfg.devices[0].retune_settle_reads = 3;
    ResourceManager rm(cfg, noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx rx;
    ASSERT_GT(rx.port, 0);
    TaskRequest scan;
    scan.msg_type             = "TASK_REQUEST_SCAN";
    scan.request_id           = "dwell-scan";
    scan.task_type            = TaskType::SCAN;
    scan.rf.rx_count          = 1;
    scan.streaming.dest_ip    = "127.0.0.1";
    scan.streaming.dest_ports = {rx.port};
    ScanParams sp;
    sp.repeat        = false;
    sp.entries       = {entry(100e6, 1e6, 100), entry(300e6, 1e6, 100), entry(500e6, 1e6, 100)};
    scan.scan_params = sp;
    auto resp        = rm.tryAccept(scan);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> iq;
    // Span of the reads labelled with the middle entry (bounded both sides).
    steady_clock::time_point first{}, last{};
    auto                     dl = steady_clock::now() + seconds(3);
    while (steady_clock::now() < dl)
    {
        if (!rx.recv(h, iq, 300))
            continue;
        if (h.center_freq_hz == 300'000'000ull)
        {
            last = steady_clock::now();
            if (first == steady_clock::time_point{})
                first = last;
        }
        if (h.center_freq_hz == 500'000'000ull)
            break;
    }
    ASSERT_NE(first, steady_clock::time_point{});
    // 100 ms dwell of 20 ms reads: ≥ 4 reads (60 ms first→last) must be the
    // entry's own. Before: the 3 dropped reads came out of the dwell (20 ms).
    const auto span_ms = duration_cast<milliseconds>(last - first).count();
    EXPECT_GE(span_ms, 50) << "middle entry delivered reads over only " << span_ms << " ms";
}

// Scan of N entries on one device; returns when the task has finished.
static void runScan(ResourceManager& rm, const std::vector<double>& cfs, int dwell_ms = 20)
{
    UdpRx       rx;
    TaskRequest scan;
    scan.msg_type             = "TASK_REQUEST_SCAN";
    scan.request_id           = "hop-scan";
    scan.task_type            = TaskType::SCAN;
    scan.rf.rx_count          = 1;
    scan.streaming.dest_ip    = "127.0.0.1";
    scan.streaming.dest_ports = {rx.port};
    ScanParams sp;
    sp.repeat = false;
    for (double cf : cfs)
        sp.entries.push_back(entry(cf, 1e6, dwell_ms));
    scan.scan_params = sp;
    auto resp        = rm.tryAccept(scan);
    ASSERT_TRUE(resp.accepted) << resp.reject_reason;
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.countByState(TaskState::COMPLETED) == 1;
        },
        5000));
}

// ═══ RM-13 — every RX tune also retuned the (unused) TX LO ═══
// TX is not streamed (RM-58 rejects tx_count > 0), yet tune() programmed the
// TX PLL on every scan hop: a second PLL write / remote RPC per entry, and
// the idle TX LO parked on the RX frequency (LO leakage into the RX band).
TEST(AuditRegression, RM13_ScanDoesNotMoveTheTxLo)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(1, 46'480, 46'499), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    FakeSoapy::tx_freq_set_count.store(0);
    FakeSoapy::rx_freq_set_count.store(0);
    runScan(rm, {100e6, 200e6, 300e6, 400e6, 500e6});
    EXPECT_GE(FakeSoapy::rx_freq_set_count.load(), 5);
    EXPECT_EQ(FakeSoapy::tx_freq_set_count.load(), 0)
        << "TX LO programmed " << FakeSoapy::tx_freq_set_count.load() << "x for an RX-only scan";
}

// ═══ RM-73 — tuneChannel() re-programmed an unchanged sample rate per hop ═══
// shared_lo=false devices (RTL-SDR, HackRF, B210) take the per-channel path:
// setSampleRate on every scan entry, although the rate never changes. On
// many drivers that reconfigures the ADC / filters and drops samples.
TEST(AuditRegression, RM73_PerChannelScanSetsTheRateOnce)
{
    FakeSoapy::reset();
    auto cfg                 = cfgA(1, 46'500, 46'519);
    cfg.devices[0].shared_lo = false;
    ResourceManager rm(cfg, noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    FakeSoapy::rx_rate_set_count.store(0);
    FakeSoapy::rx_freq_set_count.store(0);
    runScan(rm, {100e6, 200e6, 300e6, 400e6, 500e6});
    EXPECT_GE(FakeSoapy::rx_freq_set_count.load(), 5);
    EXPECT_LE(FakeSoapy::rx_rate_set_count.load(), 1)
        << "sample rate programmed " << FakeSoapy::rx_rate_set_count.load() << "x at one rate";
}

// ═══ RM-75 — one global hardware lock serialised setup across all radios ═══
// hw_activation_mu_ was held for a task's whole hardware setup (and taken by
// admission's combined retune): a slow open/activate on one radio (USB
// re-enumeration, SoapyRemote, AD9361 calibration) stalled every other radio.
TEST(AuditRegression, RM75_SlowActivationOnOneRadioDoesNotBlockAnother)
{
    FakeSoapy::reset();
    FakeSoapy::activate_delay_us.store(800'000);
    FakeSoapy::activate_delay_calls.store(1);  // only the first activation is slow
    FakeSoapy::activate_delay_limited.store(true);
    ResourceManager rm(cfgA(2, 46'520, 46'539), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    UdpRx a_rx, b_rx;
    auto  a              = continuousReq("slow", 915e6, 200e3, 1e6, 1, {a_rx.port});
    a.rf.required_device = "fake-0";
    ASSERT_TRUE(rm.tryAccept(a).accepted);
    std::this_thread::sleep_for(milliseconds(100));  // A is inside activateStream()
    auto b               = continuousReq("fast", 433e6, 200e3, 1e6, 1, {b_rx.port});
    b.rf.required_device = "fake-1";
    const auto t0        = steady_clock::now();
    ASSERT_TRUE(rm.tryAccept(b).accepted);
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> iq;
    ASSERT_TRUE(b_rx.recv(h, iq, 3000));
    const auto ms = duration_cast<milliseconds>(steady_clock::now() - t0).count();
    std::printf("RM-75: fake-1 first packet %lld ms after tryAccept (fake-0 still activating)\n",
                (long long)ms);
    EXPECT_LT(ms, 400) << "fake-1 streamed after " << ms << " ms — waited for fake-0's activation";
}

// ═══ RM-13 (cost) — AD9361 timing model: a > 100 MHz step re-calibrates ═══
// With the TX LO following every RX tune, each large scan hop paid the VCO
// calibration twice (RX and TX PLL). 4 entries alternating 100 / 900 MHz,
// 280 ms per calibration (Zynq-7020 + AD9361), 5 ms dwells.
TEST(AuditRegression, RM13_LargeScanHopCalibratesOnlyTheRxPll)
{
    FakeSoapy::reset();
    FakeSoapy::tune_delay_us.store(15);
    FakeSoapy::tune_cal_delay_us.store(280'000);
    ResourceManager rm(cfgA(1, 46'540, 46'559), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    const auto t0 = steady_clock::now();
    runScan(rm, {900e6, 100e6, 900e6, 100e6}, 5);
    const auto ms = duration_cast<milliseconds>(steady_clock::now() - t0).count();
    std::printf("RM-13: 4 large-step scan hops took %lld ms\n", (long long)ms);
    EXPECT_LT(ms, 1700) << "≈ 2 PLL calibrations per hop (TX LO retuned too)";
}

// ═══ RM-17 — hardware I/O on the AMQP thread ═══
// TASK_STOP / TASK_CANCEL tore the task down inline: closing the stream and
// joining a streamer blocked in readStream (here 400 ms reads). The AMQP
// path now uses stopTask(async=true): terminal at once, hardware released on
// a background thread.
TEST(AuditRegression, RM17_AsyncStopReturnsBeforeTheTeardown)
{
    FakeSoapy::reset();
    FakeSoapy::read_delay_us.store(400'000);
    ResourceManager rm(cfgA(1, 46'560, 46'579), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    auto stopTime = [&](const std::string& id, bool async)
    {
        UdpRx rx;
        auto  resp = rm.tryAccept(continuousReq(id, 915e6, 200e3, 1e6, 1, {rx.port}));
        EXPECT_TRUE(resp.accepted) << resp.reject_reason;
        EXPECT_TRUE(waitUntil(
            [&]
            {
                return rm.countByState(TaskState::RUNNING) == 1;
            },
            2000));
        std::this_thread::sleep_for(milliseconds(50));  // streamer inside a read
        const auto t0 = steady_clock::now();
        auto       st = rm.stopTask(resp.task_id, "s-" + id, "test", async);
        const auto ms = duration_cast<milliseconds>(steady_clock::now() - t0).count();
        EXPECT_TRUE(st.accepted);
        EXPECT_TRUE(waitUntil(
            []
            {
                return FakeSoapy::open_streams.load() == 0;
            },
            3000));
        return ms;
    };
    const auto sync_ms  = stopTime("sync", false);
    const auto async_ms = stopTime("async", true);
    std::printf("RM-17: stopTask %lld ms inline vs %lld ms async\n",
                (long long)sync_ms,
                (long long)async_ms);
    EXPECT_GT(sync_ms, 100);  // the reproducer: inline stop waits for the read
    EXPECT_LT(async_ms, 20);
}

// After an async stop the channels are still being released; a request for
// them right away must wait for that (bounded) instead of being rejected.
TEST(AuditRegression, RM17_RequestRightAfterAsyncStopGetsTheChannels)
{
    FakeSoapy::reset();
    FakeSoapy::read_delay_us.store(300'000);
    ResourceManager rm(cfgA(1, 46'580, 46'599), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx a_rx, b_rx;
    auto  a = rm.tryAccept(continuousReq("both", 915e6, 200e3, 1e6, 2, {a_rx.port, a_rx.port}));
    ASSERT_TRUE(a.accepted) << a.reject_reason;
    ASSERT_TRUE(waitUntil(
        [&]
        {
            return rm.countByState(TaskState::RUNNING) == 1;
        },
        2000));
    std::this_thread::sleep_for(milliseconds(50));
    ASSERT_TRUE(rm.stopTask(a.task_id, "stop-a", "test", /*async=*/true).accepted);
    auto b = rm.tryAccept(continuousReq("next", 433e6, 200e3, 1e6, 2, {b_rx.port, b_rx.port}));
    EXPECT_TRUE(b.accepted) << "rejected while the stopped task was releasing: " << b.reject_reason;
}

// HEALTH_QUERY / heartbeats read every radio's sensors on the AMQP thread
// (getStatus → readSensor; one RPC per radio on SoapyRemote). Now cached,
// refreshed on the watchdog thread.
TEST(AuditRegression, RM17_HealthSummaryDoesNoSensorIo)
{
    FakeSoapy::reset();
    ResourceManager rm(cfgA(2, 46'600, 46'619), noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 2);
    FakeSoapy::sensor_delay_us.store(200'000);
    auto t0 = steady_clock::now();
    rm.refreshSensors();  // what the watchdog thread now pays
    const auto refresh_ms = duration_cast<milliseconds>(steady_clock::now() - t0).count();
    t0                    = steady_clock::now();
    auto       devs       = rm.deviceSummaries();
    const auto ms         = duration_cast<milliseconds>(steady_clock::now() - t0).count();
    std::printf("RM-17: sensor read %lld ms (watchdog thread), deviceSummaries %lld ms\n",
                (long long)refresh_ms,
                (long long)ms);
    ASSERT_EQ(devs.size(), 2u);
    EXPECT_LT(ms, 20) << "health summary waited for the sensors";
    EXPECT_FALSE(std::isnan(devs[0].temp_c));
}

// ═══ RM-77 — the retune drain was counted in whole reads ═══
// With MTU-sized reads (here 32 768 samples, 6.5 ms each) two settle reads
// cost 13 ms per scan entry although the stale data is a buffer of samples.
// <retune_settle_samples> drops exactly that many, trimming inside a read.
static long long scanWallMs(int64_t settle_samples)
{
    FakeSoapy::reset();
    FakeSoapy::samples_per_read.store(32'768);
    FakeSoapy::stream_mtu.store(32'768);
    FakeSoapy::read_delay_us.store(6500);
    auto cfg                             = cfgA(1, 46'620, 46'639);
    cfg.devices[0].retune_settle_samples = settle_samples;
    ResourceManager rm(cfg, noopState, noopErr);
    EXPECT_EQ(rm.openDevices(), 1);
    std::vector<double> cfs;
    for (int i = 0; i < 10; ++i)
        cfs.push_back(100e6 + i * 8e6);
    const auto t0 = steady_clock::now();
    runScan(rm, cfs, 5);
    return duration_cast<milliseconds>(steady_clock::now() - t0).count();
}

TEST(AuditRegression, RM77_SampleExactDrainShortensScanEntries)
{
    const auto reads_ms   = scanWallMs(0);     // default: 2 whole reads
    const auto samples_ms = scanWallMs(4096);  // 4 k stale samples
    std::printf(
        "RM-77: 10-entry scan %lld ms (2 reads) vs %lld ms (4096 samples)\n", reads_ms, samples_ms);
    EXPECT_LT(samples_ms, reads_ms * 3 / 4);
}

// Sample-exact drain still drops every stale sample: pipeline one read deep
// (1024 samples carrying the previous LO), drain exactly 1024.
TEST(AuditRegression, RM77_SampleDrainDropsTheStaleSamples)
{
    FakeSoapy::reset();
    FakeSoapy::pipeline_reads.store(1);
    FakeSoapy::samples_per_read.store(1024);
    FakeSoapy::read_delay_us.store(1000);
    auto cfg                             = cfgA(1, 46'640, 46'659);
    cfg.devices[0].retune_settle_samples = 1024;
    ResourceManager rm(cfg, noopState, noopErr);
    ASSERT_EQ(rm.openDevices(), 1);
    UdpRx       rx;
    TaskRequest scan;
    scan.msg_type             = "TASK_REQUEST_SCAN";
    scan.request_id           = "lo-scan-s";
    scan.task_type            = TaskType::SCAN;
    scan.rf.rx_count          = 1;
    scan.streaming.dest_ip    = "127.0.0.1";
    scan.streaming.dest_ports = {rx.port};
    ScanParams sp;
    sp.repeat        = false;
    sp.entries       = {entry(100e6, 1e6, 150), entry(300e6, 1e6, 300)};
    scan.scan_params = sp;
    ASSERT_TRUE(rm.tryAccept(scan).accepted);
    IqPacketHeader                   h{};
    std::vector<std::complex<float>> iq;
    int                              labelled_new = 0, wrong = 0;
    auto                             dl = steady_clock::now() + seconds(3);
    while (steady_clock::now() < dl && labelled_new < 20)
    {
        if (!rx.recv(h, iq, 200) || iq.empty() || h.center_freq_hz != 300'000'000ull)
            continue;
        ++labelled_new;
        for (auto& v : iq)
            if (std::abs(v.real() - 0.3f) > 1e-4f)
            {
                ++wrong;
                break;
            }
    }
    ASSERT_GT(labelled_new, 0);
    EXPECT_EQ(wrong, 0) << wrong << " packet(s) labelled 300 MHz carried 100 MHz samples";
}

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
