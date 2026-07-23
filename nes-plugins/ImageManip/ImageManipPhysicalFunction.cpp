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

#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <numbers>
#include <numeric>
#include <ranges>
#include <span>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <arv.h>
#include <battery/embed.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <netinet/in.h>
#include <opencv2/core.hpp>
#include <opencv2/imgcodecs/imgcodecs.hpp>
#include <opencv2/imgproc/imgproc.hpp>
#include <opencv2/objdetect/objdetect.hpp>
#include <openssl/evp.h>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

struct OpenCVConfig
{
    OpenCVConfig() { cv::setNumThreads(0); }
};

static OpenCVConfig opencvConfig;

namespace NES
{
void registerImageManipLogicalFunctionUnreflectors();

namespace
{
struct __attribute__((packed)) CalRangeDescriptor
{
    float calRangeMinTempC; ///< Min calibrated temperature for range
    float calRangeMaxTempC; ///< Max calibrated temperature for range
    float displayRangeMinTempC; ///< Min displayed temperature (may be less than calMinTemp
    float displayRangeMaxTempC; ///< Max displayed temperature (may be more than calMaxTemp
    float minManualPaletteSpanDegreesC; ///< Min palette resolution for manual palette scaling mode
    float minAutoPaletteSpanDegreesC; ///< Min palette resolution for automatic palette scaling mode
    unsigned int numUInverseCurvesDescriptors;
    float UInverse[11][5]; ///< U-Inverse transfer function coefficients array
};

struct __attribute__((packed)) CalRangesData
{
    unsigned int magic; /* 0x52696d01 */
    unsigned int numEnabledRanges;
    unsigned int enabledRangesMask; /* one bit per range (LSB is rangeA, more important bits are higher
    ranges) */

    union
    {
        unsigned int encoded;

        struct
        {
            unsigned short runNumber : 2;
            unsigned short day : 5;
            unsigned short month : 4;
            unsigned short year : 5;
            unsigned short unused;
        } date;
    } calibrationDate;

    CalRangeDescriptor calRangeDescriptors[3];
    unsigned int checksum;
};

struct LookupTable
{
    std::unordered_map<uint16_t, float> entries;
};

// Build power to temperature lookup table from calibration data
LookupTable buildLookupTable()
{
    CalRangesData calibration{};
    std::ifstream calibrationFile(std::filesystem::path(IMAGE_MANIP_RESOURCE_DIR) / "calibration.bin", std::ios::binary);
    if (not calibrationFile.read(reinterpret_cast<char*>(&calibration), sizeof(calibration)))
    {
        throw std::runtime_error("Could not read image-manip calibration.bin");
    }
    const auto* calRange = &calibration.calRangeDescriptors[0];

    // Allocate lookup table
    LookupTable lookupTable;
    size_t lookupTableSize = 0;

    // Second pass: populate lookup table
    for (unsigned segment = 0; segment < calRange->numUInverseCurvesDescriptors; segment++)
    {
        float startTemp = calRange->UInverse[segment][3];
        float endTemp = calRange->UInverse[segment][4];
        float u0 = calRange->UInverse[segment][0];
        float u1 = calRange->UInverse[segment][1];
        float u2 = calRange->UInverse[segment][2];

        auto tempToPower = [&](float tempCelsius) -> uint16_t
        {
            const float power = (((tempCelsius * u2) + u1) * tempCelsius) + u0;
            const int32_t powerAsInt = static_cast<int32_t>(round(power));
            return static_cast<uint16_t>(powerAsInt);
        };


        auto powerToTemp = [&](uint16_t power) -> float
        {
            float u1mulU1 = u1 * u1;
            float u1Negative = -u1;
            float u2mul2 = u2 * 2;
            float u2mul4 = 4.0f * u2;
            float U1mulU1_minus_U2mul4mulU0 = u1mulU1 - u2mul4 * u0;
            float temperatureValue = (u1Negative + sqrtf(U1mulU1_minus_U2mul4mulU0 + u2mul4 * static_cast<float>(power))) / u2mul2;
            return temperatureValue;
        };

        const uint16_t startPower = tempToPower(startTemp);
        const uint16_t endPower = tempToPower(endTemp);
        // INVARIANT(startPower <= endPower, "startPower <= endPower");

        for (uint16_t power = startPower; power <= endPower; ++power)
        {
            lookupTable.entries[power] = powerToTemp(power);
            lookupTableSize++;
        }
    }

    return lookupTable;
}

const LookupTable& getLookupTable()
{
    static const LookupTable lookupTable = buildLookupTable();
    return lookupTable;
}

float calculateTemperature(uint16_t power)
{
    const auto& table = getLookupTable();

    if (auto it = table.entries.find(power); it != table.entries.end())
    {
        return it->second;
    }
    return NAN;
}
}

namespace
{
cv::CascadeClassifier& get()
{
    thread_local cv::CascadeClassifier cache = [] -> cv::CascadeClassifier
    {
        const auto cascade = b::embed<"Cascade.xml">();
        const cv::FileStorage storage(
            std::string(cascade.data(), cascade.size()), cv::FileStorage::READ | cv::FileStorage::MEMORY | cv::FileStorage::FORMAT_XML);
        cv::CascadeClassifier classifier;
        if (not storage.isOpened() or not classifier.read(storage.getFirstTopLevelNode()))
        {
            throw std::runtime_error("Could not read embedded image-manip Cascade.xml");
        }
        return classifier;
    }();
    return cache;
}
}

class PhysicalFunctionImageManip final
{
public:
    explicit PhysicalFunctionImageManip(std::string functionName, std::vector<PhysicalFunction> childFunctions);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    std::string functionName;
    std::vector<PhysicalFunction> childFunctions;
};

static constexpr size_t DEFAULT_IMAGE_WIDTH = 640;
static constexpr size_t DEFAULT_IMAGE_HEIGHT = 480;
static constexpr int AUDIO_SAMPLES = 16000;
static constexpr int AUDIO_FRAME_SAMPLES = 400;
static constexpr int AUDIO_FRAME_STRIDE = 160;
static constexpr int AUDIO_FFT_SIZE = 512;
static constexpr int AUDIO_FRAMES = AUDIO_SAMPLES / AUDIO_FRAME_STRIDE + 1;
static constexpr int MFCC_COEFFICIENTS = 64;
static constexpr uint32_t AUDIO_BYTES = AUDIO_SAMPLES * sizeof(float);
static constexpr uint32_t MFCC_BYTES = AUDIO_FRAMES * MFCC_COEFFICIENTS * sizeof(float);

constexpr uint64_t argmaxF32(std::span<const float> values)
{
    uint64_t maximumIndex = 0;
    for (uint64_t index = 1; index < values.size(); ++index)
    {
        if (values[index] > values[maximumIndex])
        {
            maximumIndex = index;
        }
    }
    return maximumIndex;
}

constexpr float maxF32(std::span<const float> values)
{
    return values[argmaxF32(values)];
}

constexpr float maxAbsF32(std::span<const float> values)
{
    float maximum = 0;
    for (const float value : values)
    {
        const float absolute = value < 0 ? -value : value;
        maximum = absolute > maximum ? absolute : maximum;
    }
    return maximum;
}

static_assert(
    []
    {
        constexpr std::array values{1.0F, 3.0F, 3.0F, 2.0F};
        constexpr std::array signedValues{-1.0F, 0.5F, -4.0F, 3.0F};
        return argmaxF32(values) == 1 && maxF32(values) == 3.0F && maxAbsF32(signedValues) == 4.0F;
    }());

template <float (*Function)(std::span<const float>)>
nautilus::val<float> reduceF32(const VariableSizedData& input)
{
    return nautilus::invoke(
        +[](const int8_t* data, const uint32_t size) -> float
        {
            if (size == 0 || size % sizeof(float) != 0)
            {
                throw InferenceRuntimeFailure("Expected a non-empty FLOAT32 payload, but received {} bytes", size);
            }
            return Function(std::span{reinterpret_cast<const float*>(data), size / sizeof(float)});
        },
        input.getContent(),
        input.getSize());
}

nautilus::val<uint64_t> argmaxF32(const VariableSizedData& input)
{
    return nautilus::invoke(
        +[](const int8_t* data, const uint32_t size) -> uint64_t
        {
            if (size == 0 || size % sizeof(float) != 0)
            {
                throw InferenceRuntimeFailure("Argmax expected a non-empty FLOAT32 payload, but received {} bytes", size);
            }
            return argmaxF32(std::span{reinterpret_cast<const float*>(data), size / sizeof(float)});
        },
        input.getContent(),
        input.getSize());
}

/// Transforms one second of 16 kHz audio into a frame-major [101, 64] MFCC tensor.
void audioToMFCC(const float* input, float* output)
{
    constexpr int frequencyBins = AUDIO_FFT_SIZE / 2 + 1;
    constexpr int fftPadding = (AUDIO_FFT_SIZE - AUDIO_FRAME_SAMPLES) / 2;

    if (input == nullptr || output == nullptr)
    {
        throw std::invalid_argument("audioToMFCC requires non-null input and output pointers");
    }

    static const cv::Mat hannWindow = []
    {
        cv::Mat window = cv::Mat::zeros(1, AUDIO_FFT_SIZE, CV_32F);
        for (int sample = 0; sample < AUDIO_FRAME_SAMPLES; ++sample)
        {
            window.at<float>(fftPadding + sample) = 0.5F
                - 0.5F * std::cos(2.0F * std::numbers::pi_v<float> * static_cast<float>(sample) / static_cast<float>(AUDIO_FRAME_SAMPLES));
        }
        return window;
    }();

    static const cv::Mat melFilterbank = []
    {
        constexpr int melPoints = MFCC_COEFFICIENTS + 2;
        std::array<float, melPoints> frequencies{};
        const auto maxMel = 2595.0F * std::log10(1.0F + 8000.0F / 700.0F);
        for (int point = 0; point < melPoints; ++point)
        {
            const auto mel = maxMel * static_cast<float>(point) / static_cast<float>(melPoints - 1);
            frequencies[point] = 700.0F * (std::pow(10.0F, mel / 2595.0F) - 1.0F);
        }

        cv::Mat filterbank(frequencyBins, MFCC_COEFFICIENTS, CV_32F);
        for (int bin = 0; bin < frequencyBins; ++bin)
        {
            const auto frequency = 8000.0F * static_cast<float>(bin) / static_cast<float>(frequencyBins - 1);
            for (int band = 0; band < MFCC_COEFFICIENTS; ++band)
            {
                const auto rising = (frequency - frequencies[band]) / (frequencies[band + 1] - frequencies[band]);
                const auto falling = (frequencies[band + 2] - frequency) / (frequencies[band + 2] - frequencies[band + 1]);
                filterbank.at<float>(bin, band) = std::max(0.0F, std::min(rising, falling));
            }
        }
        return filterbank;
    }();

    const cv::Mat samples(1, AUDIO_SAMPLES, CV_32F, const_cast<float*>(input));
    cv::Mat padded;
    cv::copyMakeBorder(samples, padded, 0, 0, AUDIO_FFT_SIZE / 2, AUDIO_FFT_SIZE / 2, cv::BORDER_REFLECT_101);

    cv::Mat outputView(AUDIO_FRAMES, MFCC_COEFFICIENTS, CV_32F, output);
    for (int frame = 0; frame < AUDIO_FRAMES; ++frame)
    {
        cv::Mat windowed;
        cv::multiply(padded.colRange(frame * AUDIO_FRAME_STRIDE, frame * AUDIO_FRAME_STRIDE + AUDIO_FFT_SIZE), hannWindow, windowed);

        cv::Mat spectrum;
        cv::dft(windowed, spectrum, cv::DFT_COMPLEX_OUTPUT);
        cv::Mat powerSpectrum;
        cv::mulSpectrums(spectrum, spectrum, powerSpectrum, 0, true);
        cv::Mat power;
        cv::extractChannel(powerSpectrum.colRange(0, frequencyBins), power, 0);

        cv::Mat logMel;
        cv::gemm(power, melFilterbank, 1.0, cv::noArray(), 0.0, logMel);
        logMel += 1e-6F;
        cv::log(logMel, logMel);

        cv::Mat mfcc;
        cv::dct(logMel, mfcc);
        mfcc.copyTo(outputView.row(frame));
    }
}

VariableSizedData audioToMFCC(const VariableSizedData& input, ArenaRef& arena)
{
    auto output = arena.allocateVariableSizedData(MFCC_BYTES);
    nautilus::invoke(
        +[](int8_t* inputData, uint32_t inputSize, int8_t* outputData, uint32_t outputSize)
        {
            if (inputSize != AUDIO_BYTES)
            {
                throw InferenceRuntimeFailure(
                    "Audio-to-MFCC expected {} float32 samples ({} bytes), but received {} bytes", AUDIO_SAMPLES, AUDIO_BYTES, inputSize);
            }
            if (outputSize != MFCC_BYTES)
            {
                throw InferenceRuntimeFailure(
                    "Audio-to-MFCC expected a {}-byte output buffer, but received {} bytes", MFCC_BYTES, outputSize);
            }
            audioToMFCC(reinterpret_cast<const float*>(inputData), reinterpret_cast<float*>(outputData));
        },
        input.getContent(),
        input.getSize(),
        output.getContent(),
        output.getSize());
    return output;
}

VariableSizedData toBase64(const VariableSizedData& input, ArenaRef& arena)
{
    auto length = input.getSize();
    const auto pl = 4 * ((length + 2) / 3);
    auto output = arena.allocateVariableSizedData(pl + 1); ///+1 for the terminating null that EVP_EncodeBlock adds on
    nautilus::invoke(
        +[](int8_t* input, uint32_t size, int8_t* output, uint32_t outputSize)
        {
            const auto bytesEncoded
                = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(output), reinterpret_cast<const unsigned char*>(input), size);
            if (bytesEncoded <= 0)
            {
                throw InferenceRuntimeFailure(
                    "Base64 encoding failed for an input of {} bytes: EVP_EncodeBlock returned {}", size, bytesEncoded);
            }
            if (static_cast<uint32_t>(bytesEncoded) != outputSize - 1)
            {
                throw InferenceRuntimeFailure(
                    "Base64 encoding produced {} bytes for an input of {} bytes; expected {} bytes", bytesEncoded, size, outputSize - 1);
            }
        },
        input.getContent(),
        input.getSize(),
        output.getContent(),
        output.getSize());
    return output.withSize(pl);
}

VariableSizedData fromBase64(const VariableSizedData& input, ArenaRef& arena)
{
    const auto pl = 3 * input.getSize() / 4;
    auto output = arena.allocateVariableSizedData(pl);
    auto length = nautilus::invoke(
        +[](int8_t* input, uint32_t size, int8_t* output, uint32_t outputSize)
        {
            const auto lengthOfDecoded
                = EVP_DecodeBlock(reinterpret_cast<unsigned char*>(output), reinterpret_cast<const unsigned char*>(input), size);
            if (lengthOfDecoded <= 0)
            {
                throw InferenceRuntimeFailure(
                    "Base64 decoding failed for an input of {} bytes: EVP_DecodeBlock returned {}", size, lengthOfDecoded);
            }
            if (static_cast<uint32_t>(lengthOfDecoded) > outputSize)
            {
                throw InferenceRuntimeFailure(
                    "Base64 decoding produced {} bytes for an input of {} bytes, exceeding the {}-byte output buffer",
                    lengthOfDecoded,
                    size,
                    outputSize);
            }
            return lengthOfDecoded;
        },
        input.getContent(),
        input.getSize(),
        output.getContent(),
        output.getSize());

    return output.withSize(pl - length);
}

struct YUYV
{
};

void set_pixel_color(std::span<int8_t> image, uint32_t x, uint32_t y, std::array<uint8_t, 3> yuv)
{
    auto sane_x = std::min(static_cast<uint32_t>(DEFAULT_IMAGE_WIDTH), x);
    auto sane_y = std::min(static_cast<uint32_t>(DEFAULT_IMAGE_HEIGHT), y);
    auto pixel_index = (sane_y * DEFAULT_IMAGE_WIDTH + sane_x) * 2;

    image[pixel_index] = yuv[0];
    image[pixel_index + 1] = pixel_index % 2 == 0 ? yuv[1] : yuv[2];
}

nautilus::val<uint16_t> mono16MAX(const VariableSizedData& input)
{
    nautilus::val<uint64_t> max = 0;
    auto pixels = input.getSize() / 2;

    for (nautilus::val<size_t> i = 0; i < pixels; ++i)
    {
        nautilus::val<uint16_t> value = *static_cast<nautilus::val<uint16_t*>>(input.getContent() + (nautilus::val<size_t>(2) * i));
        if (value > max)
        {
            max = value;
        }
    }
    return max;
}

nautilus::val<uint16_t> mono16MIN(const VariableSizedData& input)
{
    nautilus::val<uint64_t> min = std::numeric_limits<uint16_t>::max();
    auto pixels = input.getSize() / 2;

    for (nautilus::val<size_t> i = 0; i < pixels; ++i)
    {
        const nautilus::val<uint16_t> value = *static_cast<nautilus::val<uint16_t*>>(input.getContent() + (nautilus::val<size_t>(2) * i));
        if (value < min)
        {
            min = value;
        }
    }
    return min;
}

nautilus::val<uint16_t> mono16AVG(const VariableSizedData& input)
{
    nautilus::val<uint64_t> sum = 0;
    auto pixels = input.getSize() / 2;

    if (pixels == 0)
    {
        return nautilus::val<uint16_t>(0);
    }

    for (nautilus::val<size_t> i = 0; i < pixels; ++i)
    {
        nautilus::val<uint16_t> value = *static_cast<nautilus::val<uint16_t*>>(input.getContent() + (nautilus::val<size_t>(2) * i));
        sum = sum + value;
    }
    return sum / pixels;
}

nautilus::val<float> mono16ToCelsius(nautilus::val<uint16_t> power)
{
    return nautilus::invoke(+[](uint16_t power) { return calculateTemperature(power); }, power);
}

struct Rectangle
{
    struct Unpacked
    {
        uint16_t x;
        uint16_t y;
        uint16_t width;
        uint16_t height;
    };

    union
    {
        uint64_t packed;
        Unpacked unpacked;
    };

    Rectangle(uint64_t value) : packed(value) { }
};

VariableSizedData mono16ROI(
    const VariableSizedData& input,
    const nautilus::val<uint32_t>& width,
    const nautilus::val<uint32_t>& height,
    const nautilus::val<uint64_t>& rectangle,
    ArenaRef& arena)
{
    auto roiWidth = nautilus::invoke(+[](uint64_t rectangle) { return Rectangle(rectangle).unpacked.width; }, rectangle);
    auto roiHeight = nautilus::invoke(+[](uint64_t rectangle) { return Rectangle(rectangle).unpacked.height; }, rectangle);
    VariableSizedData roi = arena.allocateVariableSizedData(roiWidth * roiHeight * 2);
    nautilus::invoke(
        +[](int8_t* input, uint32_t inputWidth, uint32_t inputHeight, uint64_t rectangle, int8_t* destination)
        {
            const auto [x, y, width, height] = Rectangle(rectangle).unpacked;
            cv::Mat inputImg(inputHeight, inputWidth, CV_16UC1, input);
            cv::Mat roiImg(height, width, CV_16UC1, destination);
            cv::Rect roiRect(x, y, width, height);
            inputImg(roiRect).copyTo(roiImg);
        },
        input.getContent(),
        width,
        height,
        rectangle,
        roi.getContent());

    return roi;
}

VariableSizedData drawRectangle(const VariableSizedData& input, const nautilus::val<uint64_t>& rectangle, ArenaRef& arena)
{
    VariableSizedData imageWithRectangle = arena.allocateVariableSizedData(input.getSize());

    nautilus::invoke(
        +[](uint32_t input_length, int8_t* data, uint64 rectangle, int8_t* destination)
        {
            const auto [x, y, width, height] = Rectangle(rectangle).unpacked;
            auto input = std::span{reinterpret_cast<int8_t*>(data), input_length};
            auto image = std::span{destination, input_length};
            std::ranges::copy(input, image.data());

            /// Upper and Lower
            for (uint16_t i = 0; i < width; i++)
            {
                set_pixel_color(image, x + i, y, {200, 50, 0});
                set_pixel_color(image, x + i, y + 1, {200, 50, 0});

                set_pixel_color(image, x + i, y + height - 1, {200, 50, 0});
                set_pixel_color(image, x + i, y + height, {200, 50, 0});
            }

            /// Left and right
            for (uint16_t i = 0; i < height; i++)
            {
                set_pixel_color(image, x, y + i, {200, 50, 0});
                set_pixel_color(image, x + 1, y + i, {200, 50, 0});

                set_pixel_color(image, x + width - 1, y + i, {200, 50, 0});
                set_pixel_color(image, x + width, y + i, {200, 50, 0});
            }
        },
        input.getSize(),
        input.getContent(),
        rectangle,
        imageWithRectangle.getContent());

    return imageWithRectangle;
}

const static std::vector JPEG_COMPRESSION_PARAMETER = {cv::IMWRITE_JPEG_QUALITY, 90};

thread_local std::vector<uint8_t> jpegBuffer;

VariableSizedData
yuyvToJPG(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto imageSize = nautilus::invoke(
        +[](int8_t* data, uint64_t width, uint64_t height)
        {
            jpegBuffer.clear();
            const cv::Mat input(height, width, CV_8UC2, data);

            cv::Mat bgr;
            cv::cvtColor(input, bgr, cv::COLOR_YUV2BGR_YUYV);

            cv::imencode(".jpg", bgr, jpegBuffer, JPEG_COMPRESSION_PARAMETER);
            if (jpegBuffer.size() < sizeof(uint16_t))
            {
                throw InferenceRuntimeFailure(
                    "YUYV-to-JPEG encoding produced only {} bytes for a {}x{} image", jpegBuffer.size(), width, height);
            }
            const auto signature = *std::bit_cast<uint16_t*>(jpegBuffer.data());
            if (signature != 0xD8FF)
            {
                throw InferenceRuntimeFailure(
                    "YUYV-to-JPEG encoding produced an invalid JPEG signature 0x{:x} for a {}x{} image", signature, width, height);
            }
            return jpegBuffer.size();
        },
        input.getContent(),
        width,
        height);

    auto jpgVarsizedBuffer = arena.allocateVariableSizedData(imageSize);
    nautilus::invoke(
        +[](uint8_t* data, uint32_t size) { std::ranges::copy_n(jpegBuffer.begin(), size, data); },
        jpgVarsizedBuffer.getContent(),
        jpgVarsizedBuffer.getSize());

    return jpgVarsizedBuffer;
}

VariableSizedData
jpgToYUYV(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto yuyvVarsized = arena.allocateVariableSizedData(width * height * 2);
    nautilus::invoke(
        +[](int8_t* inputData, uint32_t inputSize, int8_t* dest, uint64_t width, uint64_t height)
        {
            const cv::Mat input(1, inputSize, CV_8UC1, inputData);
            cv::Mat output(height, width, CV_8UC2, dest);
            // Decode JPEG to BGR (temporary Mat)
            cv::Mat bgr = cv::imdecode(input, cv::IMREAD_COLOR);
            // Convert BGR to YUYV into your buffer
            cv::cvtColor(bgr, output, cv::COLOR_BGR2YUV_YUYV);
        },
        input.getContent(),
        input.getSize(),
        yuyvVarsized.getContent(),
        width,
        height);

    return yuyvVarsized;
}

VariableSizedData
png16ToMono16(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto mono16Varsized = arena.allocateVariableSizedData(width * height * 2);
    nautilus::invoke(
        +[](int8_t* inputData, uint32_t inputSize, int8_t* dest, const uint64_t width, const uint64_t height)
        {
            const cv::Mat input(1, inputSize, CV_8UC1, inputData);
            cv::Mat output(height, width, CV_16UC1, dest);
            cv::imdecode(input, cv::IMREAD_UNCHANGED, &output);
        },
        input.getContent(),
        input.getSize(),
        mono16Varsized.getContent(),
        width,
        height);

    return mono16Varsized;
}

thread_local std::vector<uint8_t> pngBuffer;
thread_local std::vector<uint8_t> mono16ToJpegBuffer;

void applyMono16ColorMap(const cv::Mat& input, cv::Mat& color)
{
    cv::Mat mono8;
    cv::normalize(input, mono8, 0, 255, cv::NORM_MINMAX, CV_8U);
    cv::applyColorMap(mono8, color, cv::COLORMAP_INFERNO);
}

VariableSizedData
mono16ToPNG16(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto imageSize = nautilus::invoke(
        +[](int8_t* data, size_t inputSize, uint64_t width, uint64_t height)
        {
            if (const auto expectedSize = width * height * 2; inputSize != expectedSize)
            {
                throw InferenceRuntimeFailure(
                    "Mono16-to-PNG16 expected a {}x{} image with {} bytes, but received {} bytes", width, height, expectedSize, inputSize);
            }
            const cv::Mat input(height, width, CV_16UC1, data);
            pngBuffer.clear();
            cv::imencode(".png", input, pngBuffer);
            return pngBuffer.size();
        },
        input.getContent(),
        input.getSize(),
        width,
        height);

    auto pngVarsizedBuffer = arena.allocateVariableSizedData(imageSize);
    nautilus::invoke(
        +[](uint8_t* data, uint32_t size) { std::ranges::copy_n(pngBuffer.begin(), size, data); },
        pngVarsizedBuffer.getContent(),
        pngVarsizedBuffer.getSize());

    return pngVarsizedBuffer;
}

VariableSizedData
mono16ToJPG(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto imageSize = nautilus::invoke(
        +[](int8_t* data, size_t inputSize, uint64_t width, uint64_t height)
        {
            if (const auto expectedSize = width * height * 2; inputSize != expectedSize)
            {
                throw InferenceRuntimeFailure(
                    "Mono16-to-JPEG expected a {}x{} image with {} bytes, but received {} bytes", width, height, expectedSize, inputSize);
            }
            const cv::Mat input(height, width, CV_16UC1, data);
            cv::Mat color;
            applyMono16ColorMap(input, color);
            mono16ToJpegBuffer.clear();
            cv::imencode(".jpg", color, mono16ToJpegBuffer, JPEG_COMPRESSION_PARAMETER);
            return mono16ToJpegBuffer.size();
        },
        input.getContent(),
        input.getSize(),
        width,
        height);

    auto jpgVarsizedBuffer = arena.allocateVariableSizedData(imageSize);
    nautilus::invoke(
        +[](uint8_t* data, size_t size) { std::ranges::copy_n(mono16ToJpegBuffer.begin(), size, data); },
        jpgVarsizedBuffer.getContent(),
        jpgVarsizedBuffer.getSize());
    return jpgVarsizedBuffer;
}

VariableSizedData
mono16ToYUYV(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto yuyvBuffer = arena.allocateVariableSizedData(width * height * 2);
    nautilus::invoke(
        +[](int8_t* inputData, size_t inputSize, uint64_t width, uint64_t height, int8_t* outputData, size_t outputSize)
        {
            const auto expectedSize = width * height * 2;
            if (inputSize != expectedSize || outputSize != expectedSize)
            {
                throw InferenceRuntimeFailure(
                    "Mono16-to-YUYV expected a {}x{} input and output with {} bytes, but received {} and {} bytes",
                    width,
                    height,
                    expectedSize,
                    inputSize,
                    outputSize);
            }
            const cv::Mat mono16(height, width, CV_16UC1, inputData);
            cv::Mat color;
            applyMono16ColorMap(mono16, color);
            cv::Mat yuyv(height, width, CV_8UC2, outputData);
            cv::cvtColor(color, yuyv, cv::COLOR_BGR2YUV_YUYV);
        },
        input.getContent(),
        input.getSize(),
        width,
        height,
        yuyvBuffer.getContent(),
        yuyvBuffer.getSize());
    return yuyvBuffer;
}

VariableSizedData mono16ToMono8(
    const VariableSizedData& input,
    const nautilus::val<uint64>& width,
    const nautilus::val<uint64>& height,
    const nautilus::val<uint16_t>& min,
    const nautilus::val<uint16_t>& max,
    ArenaRef& arena)
{
    auto mono8VarSized = arena.allocateVariableSizedData(height * width);
    nautilus::invoke(
        +[](uint32_t inputLength,
            int8_t* inputData,
            uint64_t width,
            uint64_t height,
            uint32_t outputLength,
            int8_t* outputData,
            uint16_t mmin,
            uint16_t mmax)
        {
            if (const auto expectedInputLength = width * height * 2; inputLength != expectedInputLength)
            {
                throw InferenceRuntimeFailure(
                    "Mono16-to-Mono8 expected a {}x{} input with {} bytes, but received {} bytes",
                    width,
                    height,
                    expectedInputLength,
                    inputLength);
            }
            if (const auto expectedOutputLength = inputLength / 2; outputLength != expectedOutputLength)
            {
                throw InferenceRuntimeFailure(
                    "Mono16-to-Mono8 expected a {}-byte output buffer for a {}-byte input, but received {} bytes",
                    expectedOutputLength,
                    inputLength,
                    outputLength);
            }
            auto max = std::max(mmin, mmax);
            auto min = std::min(mmin, mmax);
            std::span mono16Bytes(std::bit_cast<uint16_t*>(inputData), inputLength / 2);

            auto mono8Bytes = mono16Bytes
                | std::views::transform(
                                  [&](uint16_t mono16Value)
                                  {
                                      uint32_t clamped = std::clamp<uint32_t>(mono16Value, min, max);
                                      clamped -= min;
                                      clamped *= 255;
                                      clamped /= max - min;
                                      return static_cast<int8_t>(clamped);
                                  });
            std::ranges::copy(mono8Bytes, std::bit_cast<uint8_t*>(outputData));
        },
        input.getSize(),
        input.getContent(),
        width,
        height,
        mono8VarSized.getSize(),
        mono8VarSized.getContent(),
        min,
        max);

    return mono8VarSized;
}

thread_local std::vector<uint8_t> monoToJpegBuffer;

VariableSizedData
mono8ToJPG(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto imageSize = nautilus::invoke(
        +[](uint32_t input_length, int8_t* data, uint64_t width, uint64_t height)
        {
            if (const auto expectedSize = width * height; input_length != expectedSize)
            {
                throw InferenceRuntimeFailure(
                    "Mono8-to-JPEG expected a {}x{} image with {} bytes, but received {} bytes", width, height, expectedSize, input_length);
            }
            cv::Mat gray_input(height, width, CV_8UC1, data);

            cv::Mat img_color;
            cv::applyColorMap(gray_input, img_color, cv::COLORMAP_INFERNO);
            cv::imencode(".jpg", img_color, monoToJpegBuffer, JPEG_COMPRESSION_PARAMETER);
            return monoToJpegBuffer.size();
        },
        input.getSize(),
        input.getContent(),
        width,
        height);

    auto jpgVarsizedBuffer = arena.allocateVariableSizedData(imageSize);
    nautilus::invoke(
        +[](uint8_t* data, size_t size) { std::ranges::copy_n(monoToJpegBuffer.begin(), size, data); },
        jpgVarsizedBuffer.getContent(),
        jpgVarsizedBuffer.getSize());

    return jpgVarsizedBuffer;
}

VarVal faceDetection(const VariableSizedData& input, const nautilus::val<uint64_t>& width, const nautilus::val<uint64_t>& height, ArenaRef&)
{
    auto result = nautilus::invoke(
        +[](uint8_t* data, uint32_t size, uint64_t width, uint64_t height)
        {
            Rectangle r(0);
            if (const auto expectedSize = width * height * 2; size != expectedSize)
            {
                throw InferenceRuntimeFailure(
                    "Face detection expected a {}x{} YUYV image with {} bytes, but received {} bytes", width, height, expectedSize, size);
            }
            const cv::Mat input(height, width, CV_8UC2, data);
            cv::Mat gray;
            cv::cvtColor(input, gray, cv::COLOR_YUV2GRAY_YUYV);
            std::vector<cv::Rect> faces;
            get().detectMultiScale(gray, faces);

            if (!faces.empty())
            {
                r.unpacked.x = faces[0].x;
                r.unpacked.y = faces[0].y;
                r.unpacked.width = faces[0].width;
                r.unpacked.height = faces[0].height;
            }
            else
            {
                r.unpacked.x = 0;
                r.unpacked.y = 0;
                r.unpacked.width = 0;
                r.unpacked.height = 0;
            }
            return r.packed;
        },
        input.getContent(),
        input.getSize(),
        width,
        height);

    if (result == nautilus::val<uint64_t>(0))
    {
        return VarVal(nautilus::val<uint64_t>(0), true, nautilus::val<bool>(true));
    }

    return VarVal(result, true, nautilus::val<bool>(false));
}

thread_local std::vector<uint8_t> monoToBGR;

VariableSizedData
mono8ToYUYV(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto yuyvBuffer = arena.allocateVariableSizedData(width * height * 2);
    nautilus::invoke(
        +[](uint32_t input_length, int8_t* data, uint64_t width, uint64_t height, uint32_t output_length, int8_t* output)
        {
            if (const auto expectedInputLength = width * height; input_length != expectedInputLength)
            {
                throw InferenceRuntimeFailure(
                    "Mono8-to-YUYV expected a {}x{} input with {} bytes, but received {} bytes",
                    width,
                    height,
                    expectedInputLength,
                    input_length);
            }
            if (const auto expectedOutputLength = width * height * 2; output_length != expectedOutputLength)
            {
                throw InferenceRuntimeFailure(
                    "Mono8-to-YUYV expected a {}-byte output buffer for a {}x{} image, but received {} bytes",
                    expectedOutputLength,
                    width,
                    height,
                    output_length);
            }
            cv::Mat gray_input(height, width, CV_8UC1, data);
            monoToBGR.resize(height * width * 3);
            cv::Mat img_color(height, width, CV_8UC3, monoToBGR.data());
            cv::Mat output_mat(height, width, CV_8UC2, output);

            cv::applyColorMap(gray_input, img_color, cv::COLORMAP_INFERNO);
            cv::cvtColor(img_color, output_mat, cv::COLOR_BGR2YUV_YUYV);
        },
        input.getSize(),
        input.getContent(),
        width,
        height,
        yuyvBuffer.getSize(),
        yuyvBuffer.getContent());

    return yuyvBuffer;
}

VarVal PhysicalFunctionImageManip::execute(const Record& record, ArenaRef& arena) const
{
    const auto child = [&]<typename T>(size_t index) { return childFunctions[index].execute(record, arena).getRawValueAs<T>(); };

    if (functionName == "Mono16ToMono8")
    {
        return mono16ToMono8(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint64_t>>(1),
            child.template operator()<nautilus::val<uint64_t>>(2),
            child.template operator()<nautilus::val<uint16_t>>(3),
            child.template operator()<nautilus::val<uint16_t>>(4),
            arena);
    }
    else if (functionName == "AudioToMFCC")
    {
        return audioToMFCC(child.template operator()<VariableSizedData>(0), arena);
    }
    else if (functionName == "ArgmaxF32")
    {
        return argmaxF32(child.template operator()<VariableSizedData>(0));
    }
    else if (functionName == "MaxF32")
    {
        return reduceF32<maxF32>(child.template operator()<VariableSizedData>(0));
    }
    else if (functionName == "MaxAbsF32")
    {
        return reduceF32<maxAbsF32>(child.template operator()<VariableSizedData>(0));
    }
    else if (functionName == "Mono8ToJPG")
    {
        return mono8ToJPG(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint64_t>>(1),
            child.template operator()<nautilus::val<uint64_t>>(2),
            arena);
    }
    else if (functionName == "YUYVToJPG")
    {
        return yuyvToJPG(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint64_t>>(1),
            child.template operator()<nautilus::val<uint64_t>>(2),
            arena);
    }
    else if (functionName == "Mono16ToPNG16")
    {
        return mono16ToPNG16(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint64_t>>(1),
            child.template operator()<nautilus::val<uint64_t>>(2),
            arena);
    }
    else if (functionName == "Mono16ToJPG")
    {
        return mono16ToJPG(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint64_t>>(1),
            child.template operator()<nautilus::val<uint64_t>>(2),
            arena);
    }
    else if (functionName == "Mono16ToYUYV")
    {
        return mono16ToYUYV(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint64_t>>(1),
            child.template operator()<nautilus::val<uint64_t>>(2),
            arena);
    }
    else if (functionName == "Serialize")
    {
        auto image = child.template operator()<VariableSizedData>(0);
        auto width = child.template operator()<nautilus::val<uint64_t>>(1);
        auto height = child.template operator()<nautilus::val<uint64_t>>(2);
        auto pixelFormat = child.template operator()<nautilus::val<uint64_t>>(3);

        if (pixelFormat == nautilus::val<uint64_t>(ARV_PIXEL_FORMAT_MONO_16))
        {
            auto encoded = toBase64(mono16ToPNG16(image, width, height, arena), arena);
            nautilus::invoke(
                +[](int8_t* data, uint32_t size)
                {
                    if (size <= 14)
                    {
                        throw InferenceRuntimeFailure("PNG serialization produced only {} Base64 bytes; expected more than 14", size);
                    }
                    const std::string_view prefix(std::bit_cast<char*>(data), 11);
                    if (prefix != "iVBORw0KGgo")
                    {
                        throw InferenceRuntimeFailure("PNG serialization produced an invalid Base64 prefix '{}'", prefix);
                    }
                },
                encoded.getContent(),
                encoded.getSize());
            return encoded;
        }

        if (pixelFormat == nautilus::val<uint64_t>(ARV_PIXEL_FORMAT_YUV_422_YUYV_PACKED))
        {
            auto encoded = toBase64(yuyvToJPG(image, width, height, arena), arena);
            nautilus::invoke(
                +[](int8_t* data, uint32_t size)
                {
                    if (size <= 14)
                    {
                        throw InferenceRuntimeFailure("JPEG serialization produced only {} Base64 bytes; expected more than 14", size);
                    }
                    const std::string_view prefix(std::bit_cast<char*>(data), 14);
                    if (prefix != "/9j/4AAQSkZJRg")
                    {
                        throw InferenceRuntimeFailure("JPEG serialization produced an invalid Base64 prefix '{}'", prefix);
                    }
                },
                encoded.getContent(),
                encoded.getSize());
            return encoded;
        }

        nautilus::invoke(+[]() { throw std::runtime_error("Unsupported pixel format"); });

        return image;
    }
    else if (functionName == "Deserialize")
    {
        auto image = child.template operator()<VariableSizedData>(0);
        auto width = child.template operator()<nautilus::val<uint64_t>>(1);
        auto height = child.template operator()<nautilus::val<uint64_t>>(2);
        auto pixelFormat = child.template operator()<nautilus::val<uint64_t>>(3);

        if (pixelFormat == nautilus::val<uint64_t>(ARV_PIXEL_FORMAT_MONO_16))
        {
            return png16ToMono16(fromBase64(image, arena), width, height, arena);
        }
        else
        {
            return jpgToYUYV(fromBase64(image, arena), width, height, arena);
        }
    }
    else if (functionName == "Mono8ToYUYV")
    {
        return mono8ToYUYV(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint64_t>>(1),
            child.template operator()<nautilus::val<uint64_t>>(2),
            arena);
    }
    else if (functionName == "FaceDetection")
    {
        return faceDetection(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint64_t>>(1),
            child.template operator()<nautilus::val<uint64_t>>(2),
            arena);
    }
    else if (functionName == "Mono16ROI")
    {
        return mono16ROI(
            child.template operator()<VariableSizedData>(0),
            child.template operator()<nautilus::val<uint32_t>>(1),
            child.template operator()<nautilus::val<uint32_t>>(2),
            child.template operator()<nautilus::val<uint64_t>>(3),
            arena);
    }
    else if (functionName == "Mono16AVG")
    {
        return mono16AVG(child.template operator()<VariableSizedData>(0));
    }
    else if (functionName == "Mono16ToCelsius")
    {
        return mono16ToCelsius(child.template operator()<nautilus::val<uint16_t>>(0));
    }
    else if (functionName == "Mono16MAX")
    {
        return mono16MAX(child.template operator()<VariableSizedData>(0));
    }
    else if (functionName == "Mono16MIN")
    {
        return mono16MIN(child.template operator()<VariableSizedData>(0));
    }
    else if (functionName == "Rectangle")
    {
        return child.template operator()<nautilus::val<uint64_t>>(0) << 48 | child.template operator()<nautilus::val<uint64_t>>(1) << 32
            | child.template operator()<nautilus::val<uint64_t>>(2) << 16 | child.template operator()<nautilus::val<uint64_t>>(3);
    }
    else if (functionName == "DrawRectangle")
    {
        return drawRectangle(child.template operator()<VariableSizedData>(0), child.template operator()<nautilus::val<uint64_t>>(1), arena);
    }
    throw NotImplemented("ImageManip{} is not implemented!", functionName);
}

PhysicalFunctionImageManip::PhysicalFunctionImageManip(std::string functionName, std::vector<PhysicalFunction> childFunction)
    : functionName(std::move(functionName)), childFunctions(std::move(childFunction))
{
}

#define ImageManipFunction(name, operation) \
    PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::Register##name##PhysicalFunction( \
        PhysicalFunctionRegistryArguments arguments) \
    { \
        registerImageManipLogicalFunctionUnreflectors(); \
        return PhysicalFunction(PhysicalFunctionImageManip(operation, std::move(arguments.childFunctions))); \
    }

ImageManipFunction(IMAGE_MANIP_FACE_DETECTION, "FaceDetection");
ImageManipFunction(IMAGE_MANIP_MONO8_TO_JPG, "Mono8ToJPG");
ImageManipFunction(IMAGE_MANIP_MONO16_TO_PNG16, "Mono16ToPNG16");
ImageManipFunction(IMAGE_MANIP_MONO16_TO_JPG, "Mono16ToJPG");
ImageManipFunction(IMAGE_MANIP_MONO16_TO_YUYV, "Mono16ToYUYV");
ImageManipFunction(IMAGE_MANIP_SERIALIZE, "Serialize");
ImageManipFunction(IMAGE_MANIP_DESERIALIZE, "Deserialize");
ImageManipFunction(IMAGE_MANIP_MONO16_TO_MONO8, "Mono16ToMono8");
ImageManipFunction(IMAGE_MANIP_DRAW_RECTANGLE, "DrawRectangle");
ImageManipFunction(IMAGE_MANIP_YUYV_TO_JPG, "YUYVToJPG");
ImageManipFunction(IMAGE_MANIP_MONO16_ROI, "Mono16ROI");
ImageManipFunction(IMAGE_MANIP_MONO16_AVG, "Mono16AVG");
ImageManipFunction(IMAGE_MANIP_MONO16_MAX, "Mono16MAX");
ImageManipFunction(IMAGE_MANIP_MONO8_TO_YUYV, "Mono8ToYUYV");
ImageManipFunction(IMAGE_MANIP_MONO16_MIN, "Mono16MIN");
ImageManipFunction(IMAGE_MANIP_MONO16_TO_CELSIUS, "Mono16ToCelsius");
ImageManipFunction(IMAGE_MANIP_RECTANGLE, "Rectangle");
ImageManipFunction(IMAGE_MANIP_AUDIO_TO_MFCC, "AudioToMFCC");
ImageManipFunction(IMAGE_MANIP_ARGMAX_F32, "ArgmaxF32");
ImageManipFunction(IMAGE_MANIP_MAX_F32, "MaxF32");
ImageManipFunction(IMAGE_MANIP_MAX_ABS_F32, "MaxAbsF32");
}
