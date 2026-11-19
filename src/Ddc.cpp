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
#include "Ddc.hpp"
#include <algorithm>
#include <cmath>
#include <cstring>

namespace sdr {

namespace {

// Zeroth-order modified Bessel function (series), for the Kaiser window.
double besselI0(double x) {
    double sum = 1.0, term = 1.0, q = x * x / 4.0;
    for (int k = 1; k < 64; ++k) {
        term *= q / (double(k) * double(k));
        sum  += term;
        if (term < 1e-12 * sum) break;
    }
    return sum;
}

// Kaiser-windowed sinc low-pass. cutoff_norm is relative to the input rate
// (0 < cutoff_norm ≤ 0.5). Unity DC gain.
std::vector<float> designLpf(int n_taps, double cutoff_norm, double beta) {
    std::vector<float> taps((size_t)n_taps);
    const double M  = n_taps - 1;
    const double i0 = besselI0(beta);
    double sum = 0.0;
    for (int i = 0; i < n_taps; ++i) {
        const double n    = i - M / 2.0;
        const double sinc = (n == 0.0) ? 2.0 * cutoff_norm
                                       : std::sin(2.0 * M_PI * cutoff_norm * n) / (M_PI * n);
        const double r    = (M > 0) ? (2.0 * i / M - 1.0) : 0.0;
        const double win  = besselI0(beta * std::sqrt(std::max(0.0, 1.0 - r * r))) / i0;
        taps[(size_t)i] = (float)(sinc * win);
        sum += taps[(size_t)i];
    }
    for (auto& t : taps) t = (float)(t / sum);
    return taps;
}

// 8 independent partial sums: lets the compiler vectorise the reduction
// without -ffast-math (no reassociation of a single accumulator).
inline void dot2(const float* h, const float* xi, const float* xq, int n,
                 float& oi, float& oq) {
    float si[8] = {}, sq[8] = {};
    int k = 0;
    for (; k + 8 <= n; k += 8)
        for (int j = 0; j < 8; ++j) {
            si[j] += h[k + j] * xi[k + j];
            sq[j] += h[k + j] * xq[k + j];
        }
    float ai = 0.f, aq = 0.f;
    for (; k < n; ++k) { ai += h[k] * xi[k]; aq += h[k] * xq[k]; }
    for (int j = 0; j < 8; ++j) { ai += si[j]; aq += sq[j]; }
    oi = ai; oq = aq;
}

constexpr double kKaiserBeta     = 5.653;  // ≈60 dB stopband (β = 0.1102·(A − 8.7))
constexpr double kTapsPerDecim   = 18.1;   // N ≈ (A − 8) / (2.285·Δω), Δω = 2π·0.2/D
constexpr int    kMaxTaps        = 4095;

} // namespace

Ddc::Ddc(double offset_hz, double wideband_sr, int decim, int n_taps, double pass_fraction)
    : offset_hz_(offset_hz), decim_(decim)
{
    // Multiply by exp(-j*2π*offset*t) to shift signal at +offset to DC.
    const double inc = (wideband_sr > 0.0) ? 2.0 * M_PI * offset_hz / wideband_sr : 0.0;
    w_re_ = std::cos(inc);
    w_im_ = -std::sin(inc);
    next_out_ = decim_;
    if (decim_ < 1) return;            // invalid — valid() reports false
    if (decim_ == 1 && n_taps <= 0) return;   // pure frequency shift

    pass_fraction = std::clamp(pass_fraction, 0.1, 0.95);
    if (n_taps <= 0) {
        // Transition band = (1 − pass_fraction)/D of the input rate.
        const double scale = 0.2 / (1.0 - pass_fraction);
        n_taps = (int)std::lround(kTapsPerDecim * scale * decim_);
        n_taps = std::clamp(n_taps, 31, kMaxTaps) | 1;   // odd → integer group delay
    }
    // Cutoff midway between passband edge and first frequency that can alias.
    const double cutoff = std::min(0.5, 0.5 / decim_);
    taps_ = designLpf(n_taps, cutoff, kKaiserBeta);
    std::reverse(taps_.begin(), taps_.end());   // dot product runs oldest → newest
    hi_.assign((size_t)n_taps - 1, 0.0f);
    hq_.assign((size_t)n_taps - 1, 0.0f);
}

int Ddc::process(const float* in, int n_in, float* out) {
    if (decim_ < 1 || n_in <= 0) return 0;

    const size_t hist = taps_.empty() ? 0 : taps_.size() - 1;
    hi_.resize(hist + (size_t)n_in);
    hq_.resize(hist + (size_t)n_in);

    // ── Mix ──────────────────────────────────────────────────────────────
    double zr = rot_re_, zi = rot_im_;
    float* mi = hi_.data() + hist;
    float* mq = hq_.data() + hist;
    for (int i = 0; i < n_in; ++i) {
        const float xi = in[2 * i], xq = in[2 * i + 1];
        const float fr = (float)zr, fi = (float)zi;
        mi[i] = xi * fr - xq * fi;
        mq[i] = xi * fi + xq * fr;
        const double nr = zr * w_re_ - zi * w_im_;
        zi = zr * w_im_ + zi * w_re_;
        zr = nr;
    }
    const double mag = std::sqrt(zr * zr + zi * zi);   // renormalise once per block
    rot_re_ = zr / mag; rot_im_ = zi / mag;

    if (taps_.empty()) {               // decim == 1, mix only
        for (int i = 0; i < n_in; ++i) { out[2 * i] = mi[i]; out[2 * i + 1] = mq[i]; }
        hi_.clear(); hq_.clear();
        return n_in;
    }

    // ── Filter + decimate: evaluate only at output instants ───────────────
    const int n_taps = (int)taps_.size();
    int out_cnt = 0;
    int i = next_out_ - 1;             // index (within this block) of next output
    for (; i < n_in; i += decim_) {
        // Window ends at block sample i → buffer [i, i + n_taps − 1].
        dot2(taps_.data(), hi_.data() + i, hq_.data() + i, n_taps,
             out[2 * out_cnt], out[2 * out_cnt + 1]);
        ++out_cnt;
    }
    next_out_ = i - n_in + 1;

    // Keep the last (n_taps − 1) mixed samples as history.
    std::memmove(hi_.data(), hi_.data() + n_in, hist * sizeof(float));
    std::memmove(hq_.data(), hq_.data() + n_in, hist * sizeof(float));
    hi_.resize(hist);
    hq_.resize(hist);
    return out_cnt;
}

} // namespace sdr

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
