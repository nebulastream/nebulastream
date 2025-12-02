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

#include <cstdint>
#include <numeric>
#include <ranges>
#include <arv.h>
#include <Functions/PhysicalFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <battery/embed.hpp>
#include <netinet/in.h>
#include <opencv2/core/mat.hpp>
#include <opencv2/imgcodecs/imgcodecs.hpp>
#include <opencv2/imgproc/imgproc.hpp>
#include <opencv2/objdetect/objdetect.hpp>
#include <openssl/evp.h>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{
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
    auto* calRangeData = std::bit_cast<CalRangesData*>(b::embed<"calibration.bin">().data());
    auto* calRange = &calRangeData->calRangeDescriptors[0];

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
        cv::CascadeClassifier b;
        auto content = b::embed<"Cascade.xml">().str();
        cv::FileStorage fs(content, cv::FileStorage::READ | cv::FileStorage::MEMORY | cv::FileStorage::FORMAT_XML);
        b.read(fs.getFirstTopLevelNode());
        return b;
    }();
    return cache;
}
}

class PhysicalFunctionImageManip final : public PhysicalFunctionConcept
{
public:
    explicit PhysicalFunctionImageManip(std::string functionName, std::vector<PhysicalFunction> childFunctions);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::string functionName;
    const std::vector<PhysicalFunction> childFunctions;
};

static constexpr size_t DEFAULT_IMAGE_WIDTH = 640;
static constexpr size_t DEFAULT_IMAGE_HEIGHT = 480;

VariableSizedData toBase64(const VariableSizedData& input, ArenaRef& arena)
{
    auto length = input.getContentSize();
    const auto pl = 4 * ((length + 2) / 3);
    auto output = arena.allocateVariableSizedData(pl + 1); ///+1 for the terminating null that EVP_EncodeBlock adds on
    nautilus::invoke(
        +[](int8_t* input, uint32_t size, int8_t* output, USED_IN_DEBUG uint32_t outputSize)
        {
            USED_IN_DEBUG auto bytesEncoded
                = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(output), reinterpret_cast<const unsigned char*>(input), size);
            INVARIANT(bytesEncoded > 0, "bytesEncoded > 0");
            INVARIANT(static_cast<uint32_t>(bytesEncoded) == outputSize - 1, "bytesEncoded == outputSize");
        },
        input.getContent(),
        input.getContentSize(),
        output.getContent(),
        output.getContentSize());
    output.shrink(1); /// we don't care about the null terminator
    return output;
}

VariableSizedData fromBase64(const VariableSizedData& input, ArenaRef& arena)
{
    const auto pl = 3 * input.getContentSize() / 4;
    auto output = arena.allocateVariableSizedData(pl);
    auto length = nautilus::invoke(
        +[](int8_t* input, uint32_t size, int8_t* output, USED_IN_DEBUG uint32_t outputSize)
        {
            const auto lengthOfDecoded
                = EVP_DecodeBlock(reinterpret_cast<unsigned char*>(output), reinterpret_cast<const unsigned char*>(input), size);
            INVARIANT(lengthOfDecoded > 0, "bytesEncoded == size");
            INVARIANT(static_cast<uint32_t>(lengthOfDecoded) <= outputSize, "bytesDecoded < outputSize");
            return lengthOfDecoded;
        },
        input.getContent(),
        input.getContentSize(),
        output.getContent(),
        output.getContentSize());

    output.shrink(pl - length);
    return output;
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
    auto pixels = input.getContentSize() / 2;

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
    auto pixels = input.getContentSize() / 2;

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
    auto pixels = input.getContentSize() / 2;

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
    VariableSizedData imageWithRectangle = arena.allocateVariableSizedData(input.getContentSize());

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
        input.getContentSize(),
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
            INVARIANT(
                *std::bit_cast<uint16_t*>(jpegBuffer.data()) == 0xD8FF, "Invalid JPG. {:x}", *std::bit_cast<uint16_t*>(jpegBuffer.data()));
            return jpegBuffer.size();
        },
        input.getContent(),
        width,
        height);

    auto jpgVarsizedBuffer = arena.allocateVariableSizedData(imageSize);
    nautilus::invoke(
        +[](uint8_t* data, uint32_t size) { std::ranges::copy_n(jpegBuffer.begin(), size, data); },
        jpgVarsizedBuffer.getContent(),
        jpgVarsizedBuffer.getContentSize());

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
        input.getContentSize(),
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
        input.getContentSize(),
        mono16Varsized.getContent(),
        width,
        height);

    return mono16Varsized;
}

VariableSizedData
fromBase64ToTensor(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto imageTensor = arena.allocateVariableSizedData(width * height * 3);
    nautilus::invoke(
        +[](int8_t* inputData, uint32_t inputSize, int8_t* dest, const uint64_t width, const uint64_t height)
        {
            const cv::Mat input(1, inputSize, CV_8UC1, inputData);
            cv::Mat output(height, width, CV_8UC1, dest);
            cv::imdecode(input, cv::IMREAD_UNCHANGED, &output);
        },
        input.getContent(),
        input.getContentSize(),
        imageTensor.getContent(),
        width,
        height);

    return imageTensor;
}

thread_local std::vector<uint8_t> pngBuffer;

VariableSizedData
mono16ToPNG16(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto imageSize = nautilus::invoke(
        +[](int8_t* data, USED_IN_DEBUG size_t inputSize, uint64_t width, uint64_t height)
        {
            INVARIANT(inputSize == width * height * 2, "Image size is not correct");
            const cv::Mat input(height, width, CV_16UC1, data);
            pngBuffer.clear();
            cv::imencode(".png", input, pngBuffer);
            return pngBuffer.size();
        },
        input.getContent(),
        input.getContentSize(),
        width,
        height);

    auto pngVarsizedBuffer = arena.allocateVariableSizedData(imageSize);
    nautilus::invoke(
        +[](uint8_t* data, uint32_t size) { std::ranges::copy_n(pngBuffer.begin(), size, data); },
        pngVarsizedBuffer.getContent(),
        pngVarsizedBuffer.getContentSize());

    return pngVarsizedBuffer;
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
            USED_IN_DEBUG uint64_t width,
            USED_IN_DEBUG uint64_t height,
            USED_IN_DEBUG uint32_t outputLength,
            int8_t* outputData,
            uint16_t mmin,
            uint16_t mmax)
        {
            INVARIANT(inputLength == width * height * 2, "Image size is not correct");
            INVARIANT(outputLength == inputLength / 2, "Image size is not correct");
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
        input.getContentSize(),
        input.getContent(),
        width,
        height,
        mono8VarSized.getContentSize(),
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
        +[](USED_IN_DEBUG uint32_t input_length, int8_t* data, uint64_t width, uint64_t height)
        {
            INVARIANT(input_length == width * height, "Image size is not correct");
            cv::Mat gray_input(height, width, CV_8UC1, data);

            cv::Mat img_color;
            cv::applyColorMap(gray_input, img_color, cv::COLORMAP_INFERNO);
            cv::imencode(".jpg", img_color, monoToJpegBuffer, JPEG_COMPRESSION_PARAMETER);
            return monoToJpegBuffer.size();
        },
        input.getContentSize(),
        input.getContent(),
        width,
        height);

    auto jpgVarsizedBuffer = arena.allocateVariableSizedData(imageSize);
    nautilus::invoke(
        +[](uint8_t* data, size_t size) { std::ranges::copy_n(monoToJpegBuffer.begin(), size, data); },
        jpgVarsizedBuffer.getContent(),
        jpgVarsizedBuffer.getContentSize());

    return jpgVarsizedBuffer;
}

nautilus::val<uint64>
faceDetection(const VariableSizedData& input, const nautilus::val<uint64_t>& width, const nautilus::val<uint64_t>& height, ArenaRef&)
{
    return nautilus::invoke(
        +[](uint8_t* data, USED_IN_DEBUG uint32_t size, uint64_t width, uint64_t height)
        {
            Rectangle r(0);
            INVARIANT(size == width * height * 2, "Image size {}, is not correct", size);
            const cv::Mat input(height, width, CV_8UC2, data);
            cv::Mat gray;
            cv::cvtColor(input, gray, cv::COLOR_YUV2GRAY_YUYV);
            std::vector<cv::Rect> faces;
            get().detectMultiScale(gray, faces);

            if (!faces.empty())
            {
                NES_INFO("Face at ({},{}) Width: {} Height: {}", faces[0].x, faces[0].y, faces[0].width, faces[0].height);
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
        input.getContentSize(),
        width,
        height);
}

thread_local std::vector<uint8_t> monoToBGR;

VariableSizedData
mono8ToYUYV(const VariableSizedData& input, const nautilus::val<uint64>& width, const nautilus::val<uint64>& height, ArenaRef& arena)
{
    auto yuyvBuffer = arena.allocateVariableSizedData(width * height * 2);
    nautilus::invoke(
        +[](USED_IN_DEBUG uint32_t input_length,
            int8_t* data,
            uint64_t width,
            uint64_t height,
            USED_IN_DEBUG uint32_t output_length,
            int8_t* output)
        {
            INVARIANT(input_length == width * height, "Image size is not correct");
            INVARIANT(output_length == width * height * 2, "Image size is not correct");
            cv::Mat gray_input(height, width, CV_8UC1, data);
            monoToBGR.resize(height * width * 3);
            cv::Mat img_color(height, width, CV_8UC3, monoToBGR.data());
            cv::Mat output_mat(height, width, CV_8UC2, output);

            cv::applyColorMap(gray_input, img_color, cv::COLORMAP_INFERNO);
            cv::cvtColor(img_color, output_mat, cv::COLOR_BGR2YUV_YUYV);
        },
        input.getContentSize(),
        input.getContent(),
        width,
        height,
        yuyvBuffer.getContentSize(),
        yuyvBuffer.getContent());

    return yuyvBuffer;
}

VarVal PhysicalFunctionImageManip::execute(const Record& record, ArenaRef& arena) const
{
    if (functionName == "ToBase64")
    {
        return toBase64(childFunctions[0].execute(record, arena).cast<VariableSizedData>(), arena);
    }
    else if (functionName == "FromBase64")
    {
        return fromBase64(childFunctions[0].execute(record, arena).cast<VariableSizedData>(), arena);
    }
    else if (functionName == "FromBase64ToTensor")
    {
        auto image = childFunctions[0].execute(record, arena).cast<VariableSizedData>();
        auto width = childFunctions[1].execute(record, arena).cast<nautilus::val<uint64>>();
        auto height = childFunctions[2].execute(record, arena).cast<nautilus::val<uint64>>();
        return fromBase64ToTensor(fromBase64(image, arena), width, height, arena);
    }
    else if (functionName == "Mono16ToMono8")
    {
        return mono16ToMono8(
            childFunctions[0].execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1].execute(record, arena).cast<nautilus::val<uint64>>(),
            childFunctions[2].execute(record, arena).cast<nautilus::val<uint64>>(),
            childFunctions[3].execute(record, arena).cast<nautilus::val<uint16_t>>(),
            childFunctions[4].execute(record, arena).cast<nautilus::val<uint16_t>>(),
            arena);
    }
    else if (functionName == "Mono8ToJPG")
    {
        return mono8ToJPG(
            childFunctions[0].execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1].execute(record, arena).cast<nautilus::val<uint64>>(),
            childFunctions[2].execute(record, arena).cast<nautilus::val<uint64>>(),
            arena);
    }
    else if (functionName == "YUYVToJPG")
    {
        return yuyvToJPG(
            childFunctions[0].execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1].execute(record, arena).cast<nautilus::val<uint64>>(),
            childFunctions[2].execute(record, arena).cast<nautilus::val<uint64>>(),
            arena);
    }
    else if (functionName == "Mono16ToPNG16")
    {
        return mono16ToPNG16(
            childFunctions[0].execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1].execute(record, arena).cast<nautilus::val<uint64>>(),
            childFunctions[2].execute(record, arena).cast<nautilus::val<uint64>>(),
            arena);
    }
    else if (functionName == "Serialize")
    {
        auto image = childFunctions[0].execute(record, arena).cast<VariableSizedData>();
        auto width = childFunctions[1].execute(record, arena).cast<nautilus::val<uint64>>();
        auto height = childFunctions[2].execute(record, arena).cast<nautilus::val<uint64>>();
        auto pixelFormat = childFunctions[3].execute(record, arena).cast<nautilus::val<uint64>>();

        if (pixelFormat == nautilus::val<uint64_t>(ARV_PIXEL_FORMAT_MONO_16))
        {
            auto encoded = toBase64(mono16ToPNG16(image, width, height, arena), arena);
#ifndef NDEBUG
            nautilus::invoke(
                +[](int8_t* data, uint32_t size)
                {
                    INVARIANT(size > 14, "The encoded image is invalid");
                    INVARIANT(
                        std::string_view(std::bit_cast<char*>(data), 11) == "iVBORw0KGgo",
                        "The encoded image is invalid png: {}",
                        std::string_view(std::bit_cast<char*>(data), 11));
                },
                encoded.getContent(),
                encoded.getContentSize());
#endif
            return encoded;
        }

        if (pixelFormat == nautilus::val<uint64_t>(ARV_PIXEL_FORMAT_YUV_422_YUYV_PACKED))
        {
            auto encoded = toBase64(yuyvToJPG(image, width, height, arena), arena);
#ifndef NDEBUG
            nautilus::invoke(
                +[](int8_t* data, uint32_t size)
                {
                    INVARIANT(size > 14, "The encoded image is invalid");
                    INVARIANT(
                        std::string_view(std::bit_cast<char*>(data), 14) == "/9j/4AAQSkZJRg",
                        "The encoded image is invalid jpeg: {}",
                        std::string_view(std::bit_cast<char*>(data), 14));
                },
                encoded.getContent(),
                encoded.getContentSize());
#endif
            return encoded;
        }

        nautilus::invoke(+[]() { throw std::runtime_error("Unsupported pixel format"); });

        return image;
    }
    else if (functionName == "Deserialize")
    {
        auto image = childFunctions[0].execute(record, arena).cast<VariableSizedData>();
        auto width = childFunctions[1].execute(record, arena).cast<nautilus::val<uint64>>();
        auto height = childFunctions[2].execute(record, arena).cast<nautilus::val<uint64>>();
        auto pixelFormat = childFunctions[3].execute(record, arena).cast<nautilus::val<uint64>>();

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
            childFunctions[0].execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1].execute(record, arena).cast<nautilus::val<uint64>>(),
            childFunctions[2].execute(record, arena).cast<nautilus::val<uint64>>(),
            arena);
    }
    else if (functionName == "FaceDetection")
    {
        return faceDetection(
            childFunctions[0].execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1].execute(record, arena).cast<nautilus::val<uint64_t>>(),
            childFunctions[2].execute(record, arena).cast<nautilus::val<uint64_t>>(),
            arena);
    }
    else if (functionName == "Mono16ROI")
    {
        return mono16ROI(
            childFunctions[0].execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1].execute(record, arena).cast<nautilus::val<uint32_t>>(),
            childFunctions[2].execute(record, arena).cast<nautilus::val<uint32_t>>(),
            childFunctions[3].execute(record, arena).cast<nautilus::val<uint64_t>>(),
            arena);
    }
    else if (functionName == "Mono16AVG")
    {
        return mono16AVG(childFunctions[0].execute(record, arena).cast<VariableSizedData>());
    }
    else if (functionName == "Mono16ToCelsius")
    {
        return mono16ToCelsius(childFunctions[0].execute(record, arena).cast<nautilus::val<uint16_t>>());
    }
    else if (functionName == "Mono16MAX")
    {
        return mono16MAX(childFunctions[0].execute(record, arena).cast<VariableSizedData>());
    }
    else if (functionName == "Mono16MIN")
    {
        return mono16MIN(childFunctions[0].execute(record, arena).cast<VariableSizedData>());
    }
    else if (functionName == "Rectangle")
    {
        return childFunctions[0].execute(record, arena).cast<nautilus::val<uint16_t>>() << 48
            | childFunctions[1].execute(record, arena).cast<nautilus::val<uint16_t>>() << 32
            | childFunctions[2].execute(record, arena).cast<nautilus::val<uint16_t>>() << 16
            | childFunctions[3].execute(record, arena).cast<nautilus::val<uint16_t>>();
    }
    else if (functionName == "DrawRectangle")
    {
        return drawRectangle(
            childFunctions[0].execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1].execute(record, arena).cast<nautilus::val<uint64_t>>(),
            arena);
    }
    throw NotImplemented("ImageManip{} is not implemented!", functionName);
}

PhysicalFunctionImageManip::PhysicalFunctionImageManip(std::string functionName, std::vector<PhysicalFunction> childFunction)
    : functionName(std::move(functionName)), childFunctions(std::move(childFunction))
{
}

#define ImageManipFunction(name) \
    PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterImageManip##name##PhysicalFunction( \
        PhysicalFunctionRegistryArguments arguments) \
    { \
        return PhysicalFunction(PhysicalFunctionImageManip(#name, std::move(arguments.childFunctions))); \
    }

ImageManipFunction(ToBase64);
ImageManipFunction(FromBase64);
ImageManipFunction(FromBase64ToTensor);
ImageManipFunction(FaceDetection);
ImageManipFunction(Mono8ToJPG);
ImageManipFunction(Mono16ToPNG16);
ImageManipFunction(Serialize);
ImageManipFunction(Deserialize);
ImageManipFunction(Mono16ToMono8);
ImageManipFunction(DrawRectangle);
ImageManipFunction(YUYVToJPG);
ImageManipFunction(Mono16ROI);
ImageManipFunction(Mono16AVG);
ImageManipFunction(Mono16MAX);
ImageManipFunction(Mono8ToYUYV);
ImageManipFunction(Mono16MIN);
ImageManipFunction(Mono16ToCelsius);
ImageManipFunction(Rectangle);
}
