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
 * @file Ddc.hpp
 * @brief Digital Down-Converter: frequency shift + FIR low-pass filter + decimate.
 *
 * Converts a wideband CF32 IQ stream to a narrower sub-band by mixing to
baseband, filtering, and decimating. One instance per sub-band consumer in
the shared-LO fan-out path.
 */
#pragma once
#include <vector>

namespace sdr
{

// Digital Down-Converter: complex mix → real FIR low-pass filter → decimate.
// One instance per sub-band task. Not thread-safe internally — caller must
// ensure external synchronisation (IQStreamer holds subbands_mu_ during process).
//
// Filter design (audit RM-08): a Kaiser-windowed sinc whose length scales with
// the decimation ratio D, so the anti-alias performance does not collapse as
// D grows. Passband ±(pass_fraction/2)/D, stopband from (1 − pass_fraction/2)/D
// (nothing above it folds into the passband), ≈60 dB stopband attenuation.
// Only output samples are computed (polyphase), so cost per *input* sample is
// ≈18 multiply-adds regardless of D.
//
// Mixer (audit RM-10): a renormalised complex rotator instead of std::cos/sin
// per input sample; history is kept de-interleaved and contiguous so the dot
// product vectorises.
class Ddc
{
public:
    // offset_hz     – frequency to shift to baseband (positive shifts left)
    // wideband_sr   – input sample rate in Hz
    // decim         – integer decimation ratio (>= 1; 1 = mix only, no filter)
    // n_taps        – FIR length; 0 (default) = design for D automatically
    // pass_fraction – usable fraction of the output Nyquist band (default 0.8,
    //                 matching policy.usable_bw_fraction)
    Ddc(double offset_hz,
        double wideband_sr,
        int    decim,
        int    n_taps        = 0,
        double pass_fraction = 0.8);

    // Process n_in CF32 interleaved samples from `in`, write decimated CF32 to
    // `out`.  `out` must have capacity >= (ceil(n_in / decim) + 1) * 2 floats.
    // Returns number of output *samples* written (each sample = 2 floats).
    int process(const float* in, int n_in, float* out);

    int decimRatio() const
    {
        return decim_;
    }

    bool valid() const
    {
        return decim_ > 0 && (decim_ == 1 || !taps_.empty());
    }

    int numTaps() const
    {
        return (int)taps_.size();
    }

    double offsetHz() const
    {
        return offset_hz_;
    }

private:
    double offset_hz_ = 0.0;
    int    decim_;
    // Rotator z = e^{-jφ}, advanced by w = e^{-j·2π·offset/sr} each sample.
    double             rot_re_ = 1.0, rot_im_ = 0.0;
    double             w_re_ = 1.0, w_im_ = 0.0;
    int                next_out_ = 0;  // inputs still needed for the next output
    std::vector<float> taps_;          // reversed FIR (symmetric, unity DC gain)
    std::vector<float> hi_, hq_;       // history (taps−1) + current block, I and Q
};

}  // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
