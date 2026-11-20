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
#pragma once
#include <atomic>
#include <string>

// Global knobs tests set before/between operations.
// All fields are atomic so IQStreamer threads can safely read them
// while the test thread writes them.
namespace FakeSoapy
{
extern std::atomic<bool>   fail_open;         // make() throws on true
extern std::atomic<bool>   fail_read;         // readStream → NOT_SUPPORTED
extern std::atomic<bool>   overflow_next;     // readStream → OVERFLOW once
extern std::atomic<int>    timeout_count;     // readStream → TIMEOUT this many times
extern std::atomic<int>    samples_per_read;  // samples returned per readStream call
extern std::atomic<float>  sample_value;      // I/Q fill value (same for I and Q)
extern std::atomic<double> reported_temp;     // readSensor("temp0")
extern std::atomic<int>    read_delay_us;     // µs sleep inside readStream (throttle)

// ── Knobs added for the audit regression tests (defaults = old behaviour) ──
// Every channel of a multi-channel stream is filled; channel c gets
// sample_value + c * channel_step (I and Q), so tests can tell channels apart.
extern std::atomic<float> channel_step;
// When non-zero, readStream emits a phase-continuous complex tone
// tone_amplitude·e^{j2π·tone_hz·n/fs}, where fs is the RX rate the
// controller programmed via setSampleRate (1 MS/s if never set). This makes
// sample-rate / centre-frequency bookkeeping errors measurable from the IQ.
extern std::atomic<double> tone_hz;
extern std::atomic<float>  tone_amplitude;
// When true, readStream sets SOAPY_SDR_HAS_TIME and reports
// timeNs = samples_since_activate · 1e9 / fs.
extern std::atomic<bool> hw_time;
extern std::atomic<int>  activate_delay_us;       // µs sleep inside activateStream
extern std::atomic<int>  activate_delay_calls;    // with activate_delay_limited: only the next N
extern std::atomic<bool> activate_delay_limited;  //   activateStream calls sleep activate_delay_us
// setupStream() throws on the N-th call (0-based, counted across all fake
// devices); -1 = never. Lets a test fail one board of a multi-board task.
extern std::atomic<int> fail_setup_stream_at;
extern std::atomic<int> setup_stream_calls;

// Driver pipeline model: when > 0, every sample carries (as I and Q value)
// the RX centre frequency / 1e9 that was set `pipeline_reads` readStream
// calls earlier — samples already in flight when the LO moves keep the old
// frequency, as with a real driver's buffers.
extern std::atomic<int> pipeline_reads;
// setupStream() throws while this is > 0 (decremented per call).
extern std::atomic<int> fail_setup_stream_count;

// Observations (written by the fake, read by tests).
extern std::atomic<int> max_concurrent_reads;  // peak simultaneous readStream calls on one device
extern std::atomic<int> tx_freq_set_count;     // setFrequency(SOAPY_SDR_TX, …) calls
extern std::atomic<int> rx_rate_set_count;     // setSampleRate(SOAPY_SDR_RX, …) calls
extern std::atomic<int> rx_freq_set_count;     // setFrequency(SOAPY_SDR_RX, …) calls
extern std::atomic<bool>
    realtime_stream;                    // samples accrue at fs from activation; tag = capture time
extern std::atomic<int>    stream_mtu;  // getStreamMTU (RM read size), default 1024
extern std::atomic<int>    sensor_delay_us;    // readSensor sleep (e.g. SoapyRemote RPC)
extern std::atomic<int>    tune_delay_us;      // setFrequency sleep, step <= 100 MHz (LO hop)
extern std::atomic<int>    tune_cal_delay_us;  // setFrequency sleep, step > 100 MHz (VCO cal)
extern std::atomic<double> last_rx_rate;       // last setSampleRate(SOAPY_SDR_RX, …)
extern std::atomic<int>    open_streams;       // setupStream − closeStream

void reset();  // restore all fields to their defaults
// "k1=v1,k2=v2" of the kwargs the most recent make() received.
std::string lastMakeArgs();
void        setLastMakeArgs(std::string s);
}  // namespace FakeSoapy

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
