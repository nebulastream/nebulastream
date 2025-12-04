/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Execution/Functions/Function.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <std/cstring.h>
#include <ExecutableFunctionRegistry.hpp>
#include <Common/DataTypes/Float.hpp>

#include <Util/Logger/Logger.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>
#include <cstring>
#include <sstream>
#include <iomanip>

namespace NES::Runtime::Execution::Functions {

struct MelSpecParams {
    int sample_rate   = 16000;
    int n_fft         = 400;
    int win_length    = 400;
    int hop_length    = 160;
    int n_mels        = 40;
    float fmin        = 0.0f;
    float fmax        = 8000.0f;
    int target_frames = 101;
};

static constexpr bool kApplyNormalization = true;

static const float kMelMean[40] = {
    -8.2160,  -7.0743,  -6.4821,  -6.1831,  -6.7281,  -7.0503,  -7.2575,
    -7.3544,  -7.6203,  -7.9106,  -8.1490,  -8.3737,  -8.7019,  -8.8741,
    -9.2064,  -9.4442,  -9.6138,  -9.6957,  -9.7030,  -9.8139,  -9.9993,
    -10.2186, -10.4083, -10.4372, -10.5115, -10.6126, -10.7421, -10.8343,
    -10.9514, -11.1105, -11.3259, -11.5441, -11.7392, -11.9286, -12.1333,
    -12.2867, -12.3593, -12.6297, -13.6289, -15.9821
};
static const float kMelStd[40] = {
    7.7147, 7.5881, 7.7746, 7.9476, 7.8909, 7.8429, 8.0211, 8.1393, 8.1328,
    8.1165, 8.0399, 7.8774, 7.6814, 7.5295, 7.4086, 7.3172, 7.2240, 7.1592,
    7.1269, 7.0714, 6.9739, 6.8711, 6.8227, 6.8479, 6.8793, 6.8432, 6.7742,
    6.7324, 6.6811, 6.6113, 6.5061, 6.4150, 6.3430, 6.2674, 6.2043, 6.1526,
    6.1271, 6.1075, 6.0064, 5.6733
};

// Mel scale helpers. HTK-style; torchaudio uses Slaney by default for mels, but differences are tiny
static inline float hz_to_mel(float f) {
    return 2595.0f * std::log10(1.0f + f / 700.0f);
}
static inline float mel_to_hz(float m) {
    return 700.0f * (std::pow(10.0f, m / 2595.0f) - 1.0f);
}

static void real_dft_power_1frame(const float* frame, int n_fft, std::vector<float>& power_out) {
    const int n_bins = n_fft / 2 + 1;
    power_out.assign(n_bins, 0.0f);

    // Naive O(N^2) DFT; for maybe kissFFT later
    for (int k = 0; k < n_bins; ++k) {
        double re = 0.0, im = 0.0;
        const double ang = -2.0 * M_PI * k / static_cast<double>(n_fft);
        for (int n = 0; n < n_fft; ++n) {
            const double a = ang * n;
            const double c = std::cos(a);
            const double s = std::sin(a);
            const double x = frame[n];
            re += x * c;
            im += x * s;
        }
        power_out[k] = static_cast<float>(re * re + im * im);
    }
}

static std::vector<float> build_mel_filterbank(const MelSpecParams& P) {
    const int n_bins = P.n_fft / 2 + 1;
    std::vector<float> fb(static_cast<size_t>(P.n_mels) * n_bins, 0.0f);

    // FFT bin center frequencies: 0 .. sr/2 inclusive, length n_bins
    std::vector<float> freqs(n_bins);
    const float df = static_cast<float>(P.sample_rate) / static_cast<float>(P.n_fft); // bin spacing
    for (int k = 0; k < n_bins; ++k) freqs[k] = k * df; // [0, df, 2df, ..., sr/2]

    // Mel points (P.n_mels + 2), HTK scale
    const float mel_min = hz_to_mel(P.fmin);
    const float mel_max = hz_to_mel(P.fmax);
    std::vector<float> mel_pts(P.n_mels + 2);
    for (int i = 0; i < P.n_mels + 2; ++i) {
        mel_pts[i] = mel_min + (mel_max - mel_min) * (static_cast<float>(i) / (P.n_mels + 1));
    }

    // mel points to Hz
    std::vector<float> hz_pts(P.n_mels + 2);
    for (int i = 0; i < P.n_mels + 2; ++i) hz_pts[i] = mel_to_hz(mel_pts[i]);

    // for every mel band m, sample triangular window at each FFT bin
    for (int m = 0; m < P.n_mels; ++m) {
        const float f_left   = hz_pts[m];
        const float f_center = hz_pts[m + 1];
        const float f_right  = hz_pts[m + 2];
        if (!(f_left < f_center && f_center < f_right)) continue;

        for (int k = 0; k < n_bins; ++k) {
            const float f = freqs[k];
            float w = 0.0f;
            if (f >= f_left && f < f_center) {
                w = (f - f_left) / (f_center - f_left);
            } else if (f >= f_center && f <= f_right) {
                w = (f_right - f) / (f_right - f_center);
            }
            fb[size_t(m) * n_bins + k] = (w > 0.f) ? w : 0.f;
        }
    }

    return fb;
}

static void ComputeLogMelFromPCM(std::byte* inContent, uint32_t inBytes,
                                 std::byte* outContent, uint32_t outBytes) {
    MelSpecParams P{};
    const int n_bins = P.n_fft / 2 + 1;
    const size_t total_out_floats = size_t(P.n_mels) * size_t(P.target_frames);
    const size_t needed_bytes = total_out_floats * sizeof(float);
    if (outBytes < needed_bytes) return;

    float* out = reinterpret_cast<float*>(outContent);
    std::fill(out, out + total_out_floats, 0.0f);

    int n_samples = int(inBytes / sizeof(float));

    //const float* samples = reinterpret_cast<const float*>(inContent);
    //Hotfix normalization int to float
    float* samples = reinterpret_cast<float*>(inContent);
    for (int i = 0; i < n_samples; ++i) {
        samples[i] /= static_cast<float>(std::numeric_limits<int16_t>::max());
    }
    //Hotfix end

    if (n_samples > 16000) {
        n_samples = 16000;
    } else if (n_samples < 16000) {
        // pad with zeros
        std::vector<float> padded_input(16000, 0.0f);
        std::memcpy(padded_input.data(), samples, n_samples * sizeof(float));
        samples = padded_input.data();
        n_samples = 16000;
    }
    if (n_samples > 0) {
        int to_print = std::min(n_samples, 1000);
        std::ostringstream vals;
        for (int i = 0; i < to_print; ++i) {
            vals << std::fixed << std::setprecision(6) << samples[i] << " ";
        }
        //NES_DEBUG("ComputeLogMelFromPCM: first {} samples = {}", to_print, vals.str());
    }


    //NES_DEBUG("ComputeLogMelFromPCM: received bytes={}, samples={}", inBytes, n_samples);

    // Hann window
    std::vector<float> window(P.win_length);
    for (int i = 0; i < P.win_length; ++i) {
        window[i] = 0.5f - 0.5f * std::cos(2.0f * float(M_PI) * (float)i / float(P.win_length));
    }

    // center=True with pad_mode='reflect'
    const int pad = P.n_fft / 2; // 200
    const size_t total = size_t(n_samples) + 2u * size_t(pad);
    std::vector<float> padded(total, 0.0f);

    if (n_samples > 0) {
        std::memcpy(padded.data() + pad, samples, size_t(n_samples) * sizeof(float));
    }

    //left side: padded[0 .. pad-1] mirrors samples[1 .. pad]
    for (int i = 0; i < pad; ++i) {
        int si = 1 + i;
        if (si >= n_samples) si = n_samples - 1;
        padded[pad - 1 - i] = samples[si];
    }

    //right side: padded[pad+n_samples .. end) mirrors samples[n_samples-2 .. n_samples-2-pad+1]
    for (int i = 0; i < pad; ++i) {
        int si = n_samples - 2 - i;
        if (si < 0) si = 0;
        padded[pad + n_samples + i] = samples[si];
    }

    // framing count
    int n_frames = 0;
    if (int(padded.size()) >= P.win_length) {
        n_frames = 1 + (int(padded.size()) - P.win_length) / P.hop_length;
    }

    const int start_frame = std::max(0, n_frames - P.target_frames);
    const int frames_to_compute = std::min(P.target_frames, n_frames);

    static thread_local std::vector<float> mel_fb = build_mel_filterbank(P);

    std::vector<float> frame(P.n_fft, 0.0f);
    std::vector<float> power;                  // size n_bins
    std::vector<float> mel_vec(P.n_mels, 0.0f);

    auto out_index = [P](int m, int t) -> size_t {
        // (mel, time) flattening
        return size_t(m) * size_t(P.target_frames) + size_t(t);
    };

    for (int tl = 0; tl < frames_to_compute; ++tl) {
        const int t_abs = start_frame + tl;
        const int out_t = P.target_frames - frames_to_compute + tl;
        const int start = t_abs * P.hop_length;

        // windowed frame (length = win_length), zero-pad/truncate to n_fft (n_fft == win_length here)
        std::fill(frame.begin(), frame.end(), 0.0f);
        for (int i = 0; i < P.win_length; ++i) {
            frame[i] = padded[start + i] * window[i];
        }

        real_dft_power_1frame(frame.data(), P.n_fft, power);

        // mel projection + log2
        for (int m = 0; m < P.n_mels; ++m) {
            const float* w = &mel_fb[size_t(m) * n_bins];
            double acc = 0.0;
            for (int k = 0; k < n_bins; ++k) acc += double(power[k]) * double(w[k]);
            const float p = (acc <= 1e-9) ? 1e-9f : static_cast<float>(acc);
            mel_vec[m] = std::log2(p);
        }

        if (kApplyNormalization) {
            for (int m = 0; m < P.n_mels; ++m) {
                mel_vec[m] = (mel_vec[m] - kMelMean[m]) / (kMelStd[m] + 1e-9f);
            }
        }

        for (int m = 0; m < P.n_mels; ++m) {
            out[out_index(m, out_t)] = mel_vec[m];
        }
    }
}

struct MelSpectrogramExecutableFunction : Function {
    explicit MelSpectrogramExecutableFunction(std::unique_ptr<Function> input)
        : inputFn(std::move(input)) {}

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override {
        //NES_DEBUG("MelSpectrogramExecutableFunction::execute (live, torchaudio-compatible)");

        auto inAny = inputFn->execute(record, arena);
        auto inVar = inAny.cast<VariableSizedData>();

        // allocate output buffer: 40 x 101 floats
        static constexpr size_t MEL = 40;
        static constexpr size_t FRAMES = 101;
        static constexpr uint32_t OUT_BYTES = static_cast<uint32_t>(MEL * FRAMES * sizeof(float));
        auto outVar = arena.allocateVariableSizedData(nautilus::val<uint32_t>(OUT_BYTES));

        nautilus::invoke(
            ComputeLogMelFromPCM,
            inVar.getContent(),
            inVar.getContentSize(),
            outVar.getContent(),
            outVar.getContentSize()
        );

        nautilus::invoke(
            +[](std::byte* content) {
                const float* p = reinterpret_cast<const float*>(content);
                //NES_DEBUG("Mel out[0..3] = {:.5f} {:.5f} {:.5f} {:.5f}", p[0], p[1], p[2], p[3]);
        },
    outVar.getContent()
);

        return VarVal(outVar);
    }

    std::unique_ptr<Function> inputFn;

};

} // namespace NES::Runtime::Execution::Functions

namespace NES::Runtime::Execution::Functions::ExecutableFunctionGeneratedRegistrar {
ExecutableFunctionRegistryReturnType RegisterMelSpectrogramExecutableFunction(ExecutableFunctionRegistryArguments args) {
    if (args.childFunctions.size() != 1) {
        throw TypeInferenceException("MelSpectrogram expects 1 argument");
    }
    return std::make_unique<NES::Runtime::Execution::Functions::MelSpectrogramExecutableFunction>(
        std::move(args.childFunctions[0]));
}
} // namespace NES::Runtime::Execution::Functions::ExecutableFunctionGeneratedRegistrar
