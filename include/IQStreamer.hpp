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
/**
 * @file IQStreamer.hpp
 * @brief SoapySDR IQ capture thread — reads hardware and fans out to multiple UDP sockets.
 *
 * Owns one SoapySDR::Stream and one background worker thread. Supports multiple
destinations (addDest) and DDC sub-band consumers (addSubBand). Used by
ResourceManager::activateTask() for all IQ delivery.
 *
 * ## Packet timing (ICD §10.1)
 * timestamp_ns is "nanoseconds from task start". Each consumer carries the
 * epoch of its own task; packet times are derived from the sample count
 * (jitter-free, exact increments of num_samples/fs), anchored to the host
 * clock at the first read and re-anchored after an overflow. When the driver
 * reports hardware time (SOAPY_SDR_HAS_TIME) that is used instead, so sample
 * gaps are represented exactly. All streams of one task share the same epoch,
 * so timestamps from different boards are directly comparable.
 */
#pragma once
#include "Ddc.hpp"
#include "sdr/Types.hpp"

#include <SoapySDR/Device.hpp>
#include <netinet/in.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace sdr
{

class IQStreamer
{
public:
    using Clock = std::chrono::steady_clock;

    // Extra channel descriptor for multi-channel shared streams.
    struct ChannelDest
    {
        int         channel_index = 0;
        std::string stream_id;
        std::string dest_ip;
        int         dest_port = 0;
    };

    struct Config
    {
        std::string task_id;
        std::string stream_id;
        int         channel_index = 0;
        std::string dest_ip;  // initial dest; additional dests via addDest()
        int         dest_port      = 0;
        int         packet_samples = 1024;
        int64_t     task_start_ms  = 0;
        double      sample_rate    = 0.0;
        // Channels 1..N of a multi-channel SoapySDR stream.
        // When non-empty, workerLoop reads N+1 buffers and demuxes each channel
        // to its own UDP port — only ONE IQStreamer owns the stream.
        std::vector<ChannelDest> extra_channels;
        // Epoch of the owning task (timestamp_ns = time since this instant).
        // Default (zero) = the instant start() is called.
        Clock::time_point task_epoch{};
        // Samples per readStream call. 0 = the driver's stream MTU, rounded to
        // whole packets (fewer driver calls; packets are still packet_samples).
        int read_chunk_samples = 0;
    };

    IQStreamer(const Config&     cfg,
               SoapySDR::Device* dev,
               SoapySDR::Stream* stream,
               TaskErrorCb       on_error = nullptr);
    ~IQStreamer();
    IQStreamer(const IQStreamer&)            = delete;
    IQStreamer& operator=(const IQStreamer&) = delete;

    void start();
    void stop();

    bool isRunning() const
    {
        return running_.load(std::memory_order_acquire);
    }

    void updateCenterFreq(double new_cf_hz);
    void updateSampleRate(double new_sr_sps);

    double currentCF() const
    {
        return current_cf_.load(std::memory_order_relaxed);
    }

    double currentSR() const
    {
        return (double)current_sr_hz_.load(std::memory_order_relaxed);
    }

    // Drain `settle_reads` readStream() calls without sending UDP packets.
    // Call *before* the retune: the read in flight and whatever the driver
    // already buffered still carry the old LO (RM-65).
    void pauseForRetune(int settle_reads);

    /// Block until the reads requested by pauseForRetune() have been dropped
    /// (or the streamer stops / @p timeout passes). The scan dwell starts
    /// after this, so it is not eaten by the drain (RM-71). Returns false on
    /// timeout.
    bool waitRetuneSettled(std::chrono::milliseconds timeout) const;

    // Multi-destination fan-out. Each task subscribes with its own UDP endpoint.
    // start() auto-adds the dest from Config if dest_ip is non-empty.
    // `epoch` = the subscribing task's start (default: this streamer's epoch).
    // `channel_index` = which channel of a multi-channel stream to deliver
    // (-1 = the primary channel).
    void addDest(const std::string& task_id,
                 const std::string& stream_id,
                 const std::string& ip,
                 int                port,
                 Clock::time_point  epoch         = {},
                 int                channel_index = -1);
    // Removes every destination of the task (all channels); returns the
    // remaining primary-channel destination count.
    int removeDest(const std::string& task_id);
    int destCount() const;  // primary-channel destinations

    // DDC sub-band fan-out. The wideband stream is mixed, filtered, and decimated
    // to the task's requested band before sending. One sub-band per narrowband task.
    // cf_hz: sub-band center frequency; output_sr_hz: decimated sample rate;
    // wideband_sr_hz: current hardware sample rate (used to compute decimation ratio).
    // The packet header advertises the rate actually produced (wideband / D).
    void addSubBand(const std::string& task_id,
                    const std::string& stream_id,
                    int                channel_index,
                    const std::string& ip,
                    int                port,
                    double             cf_hz,
                    double             output_sr_hz,
                    double             wideband_sr_hz,
                    Clock::time_point  epoch = {});
    int  removeSubBand(const std::string& task_id);  // returns remaining total consumers
    int  subBandCount() const;
    int  totalConsumers() const;  // distinct tasks still subscribed (any channel)

    // Rebuild every sub-band DDC after the hardware window moved (combined
    // retune): new mixer offset and decimation, same output CF and rate.
    // Returns false if some sub-band's rate no longer divides new_sr_hz
    // (that sub-band is then muted rather than delivering wrong data).
    bool retuneSubBands(double new_cf_hz, double new_sr_hz);

    StreamMetrics getMetrics() const;

private:
    struct Dest
    {
        std::string       task_id;
        std::string       stream_id;
        int               fd            = -1;
        int               channel_index = 0;
        int               buf_index     = 0;  // which channel buffer feeds it
        uint32_t          seq           = 0;
        bool              first         = true;
        Clock::time_point epoch{};
    };

    // DDC sub-band: one per narrowband task sharing this wideband stream.
    struct SubBand
    {
        std::string          task_id;
        std::string          stream_id;
        int                  channel_index  = 0;
        int                  buf_index      = 0;  // -1 = channel not in this stream
        int                  dest_fd        = -1;
        uint32_t             seq            = 0;
        bool                 first          = true;
        uint64_t             center_freq_hz = 0;
        double               output_sr      = 0.0;  // requested
        uint32_t             sample_rate_hz = 0;    // advertised (= wideband / D)
        Clock::time_point    epoch{};
        std::unique_ptr<Ddc> ddc;
        std::vector<float>   out_buf;
    };

    Config            cfg_;
    SoapySDR::Device* dev_;
    SoapySDR::Stream* stream_;
    TaskErrorCb       on_error_;

    std::atomic<bool>     running_{false};
    std::atomic<double>   current_cf_{0.0};
    std::atomic<bool>     dwell_changed_{false};
    std::atomic<int>      drain_countdown_{0};
    std::atomic<uint32_t> current_sr_hz_{0};
    std::thread           thread_;
    Clock::time_point     start_epoch_{};  // used when a consumer has no epoch

    // Both dests_ and subbands_ are protected by dests_mu_.
    mutable std::mutex   dests_mu_;
    std::vector<Dest>    dests_;
    std::vector<SubBand> subbands_;

    mutable std::mutex mu_;
    StreamMetrics      metrics_;
    uint64_t           packets_dropped_ = 0;  // guarded by mu_

    int  bufIndexFor(int channel_index) const;
    int  openConnectedUdp(const std::string& ip, int port, const char* who) const;
    void workerLoop();
    // Send `n` samples from `samples` to one socket as ⌈n/P⌉ packets in a
    // single sendmmsg(). Returns packets the kernel accepted.
    int sendBatch(int          fd,
                  uint32_t&    seq,
                  bool&        first,
                  int          ch_idx,
                  uint64_t     cf_hz,
                  uint32_t     sr_hz,
                  int64_t      ts0_ns,
                  double       ns_per_sample,
                  const float* samples,
                  int          n,
                  uint8_t      flags);
};

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
