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

#include <Functions/FtPreprocessAllSlotsPhysicalFunction.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <span>
#include <utility>
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <openssl/evp.h>
#include <scope_guard.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

/// Single-header JPEG (and friends) decoder, public domain. Vendored again rather than shared with
/// FtPreprocessSlot -- plugins in this codebase are self-contained (see that plugin's CMakeLists
/// comment on stb_image over a formal libjpeg-turbo dependency). STB_IMAGE_STATIC gives this
/// translation unit's copy of the implementation internal linkage, since both plugin libraries end
/// up in the same final binary and external linkage would collide with FtPreprocessSlot's own copy.
#define STB_IMAGE_STATIC
#define STB_IMAGE_IMPLEMENTATION
#define STBI_ONLY_JPEG
#define STBI_NO_STDIO
#include <stb_image.h>

namespace NES
{

FtPreprocessAllSlotsPhysicalFunction::FtPreprocessAllSlotsPhysicalFunction(PhysicalFunction image) : image(std::move(image))
{
}

namespace
{
constexpr uint32_t modelExtent = 64;
constexpr uint32_t modelChannels = 3;
constexpr uint32_t slotCount = 9;
constexpr uint64_t perSlotTensorBytes = uint64_t{1} * modelChannels * modelExtent * modelExtent * sizeof(float);
constexpr uint64_t outputTensorBytes = uint64_t{slotCount} * perSlotTensorBytes;

struct SlotRoi
{
    uint32_t x;
    uint32_t y;
    uint32_t w;
    uint32_t h;
};

/// The 9 fixed HBW slot ROIs, same pixel rects and order (A1..C3) as the 9 FT_PREPROCESS_SLOT
/// calls in ssc-slot-anomaly.sql. This order is also the row order BUCKET_SLOT_CNN_9's batched
/// input expects and the column order its output is unpacked into (p_absent_A1..p_absent_C3 in
/// the gated-batched YAML/SQL) -- change all three together or slot labels silently swap.
constexpr std::array<SlotRoi, slotCount> slotRois{
    {{86, 10, 55, 51}, /// A1
     {155, 20, 57, 50}, /// A2
     {240, 30, 76, 55}, /// A3
     {88, 63, 56, 40}, /// B1
     {149, 73, 69, 52}, /// B2
     {237, 88, 78, 62}, /// B3
     {92, 107, 51, 51}, /// C1
     {150, 128, 72, 50}, /// C2
     {226, 153, 88, 53}}}; /// C3

/// HWC-interleaved, 8-bit-per-channel RGB image, decoded or cropped.
struct RgbImage
{
    uint32_t width;
    uint32_t height;
    std::vector<uint8_t> pixels;
};

struct ResampleContribution
{
    std::vector<size_t> indices;
    std::vector<float> weights;
};

/// See FtPreprocessSlotPhysicalFunction.cpp's stripDataUriPrefix for the data-URI rationale.
std::span<const std::byte> stripDataUriPrefix(std::span<const std::byte> field)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-char to search for the data-URI ',' separator
    const auto* commaPtr = static_cast<const std::byte*>(std::memchr(field.data(), ',', field.size()));
    if (commaPtr == nullptr)
    {
        throw FormattingError("FT_PREPROCESS_ALL_SLOTS expected a data-URI (\"data:<type>;base64,<...>\") but found no ',' separator");
    }
    const auto prefixLength = static_cast<size_t>(commaPtr - field.data()) + 1;
    return field.subspan(prefixLength);
}

std::vector<uint8_t> decodeBase64(std::span<const std::byte> base64Bytes)
{
    if (base64Bytes.empty())
    {
        return {};
    }
    std::vector<uint8_t> decoded(base64Bytes.size() / 4 * 3);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) OpenSSL's EVP API takes unsigned char*
    const auto decodedLen = EVP_DecodeBlock(
        decoded.data(), reinterpret_cast<const unsigned char*>(base64Bytes.data()), static_cast<int>(base64Bytes.size()));
    if (decodedLen < 0)
    {
        throw FormattingError("FT_PREPROCESS_ALL_SLOTS could not base64-decode the data-URI payload");
    }

    auto actualLen = static_cast<size_t>(decodedLen);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) index into the base64 buffer to check padding characters
    if (base64Bytes.size() >= 1 && base64Bytes[base64Bytes.size() - 1] == std::byte{'='})
    {
        --actualLen;
    }
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) index into the base64 buffer to check padding characters
    if (base64Bytes.size() >= 2 && base64Bytes[base64Bytes.size() - 2] == std::byte{'='})
    {
        --actualLen;
    }
    decoded.resize(actualLen);
    return decoded;
}

RgbImage decodeJpegToRgb(std::span<const std::byte> jpegBytes)
{
    int width = 0;
    int height = 0;
    int channelsInFile = 0;
    unsigned char* decoded = stbi_load_from_memory(
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        reinterpret_cast<const unsigned char*>(jpegBytes.data()),
        static_cast<int>(jpegBytes.size()),
        &width,
        &height,
        &channelsInFile,
        modelChannels);
    if (decoded == nullptr)
    {
        throw FormattingError("FT_PREPROCESS_ALL_SLOTS could not decode JPEG payload: {}", stbi_failure_reason());
    }
    SCOPE_EXIT
    {
        stbi_image_free(decoded);
    };

    const auto pixelCount = static_cast<size_t>(width) * static_cast<size_t>(height) * modelChannels;
    return {.width = static_cast<uint32_t>(width), .height = static_cast<uint32_t>(height), .pixels = {decoded, decoded + pixelCount}};
}

RgbImage cropRgb(const RgbImage& image, uint32_t x, uint32_t y, uint32_t w, uint32_t h)
{
    if (w == 0 or h == 0 or x + w > image.width or y + h > image.height)
    {
        throw FormattingError(
            "FT_PREPROCESS_ALL_SLOTS crop rect ({}, {}, {}, {}) is out of bounds for a {}x{} frame",
            x,
            y,
            w,
            h,
            image.width,
            image.height);
    }

    std::vector<uint8_t> cropped(static_cast<size_t>(w) * h * modelChannels);
    for (uint32_t row = 0; row < h; ++row)
    {
        const auto* srcRow = &image.pixels[(static_cast<size_t>(y + row) * image.width + x) * modelChannels];
        auto* dstRow = &cropped[static_cast<size_t>(row) * w * modelChannels];
        std::memcpy(dstRow, srcRow, static_cast<size_t>(w) * modelChannels);
    }
    return {.width = w, .height = h, .pixels = std::move(cropped)};
}

/// See FtPreprocessSlotPhysicalFunction.cpp's computeTriangleContributions for the PIL-parity rationale.
std::vector<ResampleContribution> computeTriangleContributions(const size_t inputExtent, const uint32_t outputExtent)
{
    constexpr float coordinateShift = 0.01F;
    const auto scale = static_cast<float>(inputExtent) / static_cast<float>(outputExtent);
    const auto filterScale = std::max(scale, 1.0F);
    const auto support = filterScale;

    std::vector<ResampleContribution> contributions(outputExtent);
    for (uint32_t outputIndex = 0; outputIndex < outputExtent; ++outputIndex)
    {
        const auto center = (static_cast<float>(outputIndex) + 0.5F) * scale + coordinateShift;
        const auto windowStart = static_cast<int>(center - support + 0.5F);
        const auto windowEnd = static_cast<int>(center + support + 0.5F);

        auto& contribution = contributions[outputIndex];
        for (int sample = windowStart; sample < windowEnd; ++sample)
        {
            const auto clampedSample = static_cast<size_t>(std::clamp(sample, 0, static_cast<int>(inputExtent) - 1));
            const auto distance = (static_cast<float>(sample) + 0.5F - center) / filterScale;
            const auto weight = std::max(0.0F, 1.0F - std::abs(distance));
            if (weight > 0.0F)
            {
                contribution.indices.push_back(clampedSample);
                contribution.weights.push_back(weight);
            }
        }

        const auto weightSum = std::accumulate(contribution.weights.begin(), contribution.weights.end(), 0.0F);
        for (auto& weight : contribution.weights)
        {
            weight /= weightSum;
        }
    }

    return contributions;
}

void resizeRgbLikePillow(const RgbImage& image, std::vector<uint8_t>& resizedPixels)
{
    if (image.width == modelExtent and image.height == modelExtent)
    {
        resizedPixels = image.pixels;
        return;
    }

    const auto horizontalContributions = computeTriangleContributions(image.width, modelExtent);
    const auto verticalContributions = computeTriangleContributions(image.height, modelExtent);

    std::vector<float> horizontallyResized(static_cast<size_t>(image.height) * modelExtent * modelChannels, 0.0F);
    for (uint32_t y = 0; y < image.height; ++y)
    {
        for (uint32_t outputX = 0; outputX < modelExtent; ++outputX)
        {
            const auto& contribution = horizontalContributions[outputX];
            for (size_t channel = 0; channel < modelChannels; ++channel)
            {
                auto value = 0.0F;
                for (size_t i = 0; i < contribution.indices.size(); ++i)
                {
                    const auto inputX = contribution.indices[i];
                    const auto inputOffset = (static_cast<size_t>(y) * image.width + inputX) * modelChannels + channel;
                    value += static_cast<float>(image.pixels[inputOffset]) * contribution.weights[i];
                }
                const auto outputOffset = (static_cast<size_t>(y) * modelExtent + outputX) * modelChannels + channel;
                horizontallyResized[outputOffset] = value;
            }
        }
    }

    resizedPixels.resize(static_cast<size_t>(modelExtent) * modelExtent * modelChannels);
    for (uint32_t outputY = 0; outputY < modelExtent; ++outputY)
    {
        const auto& contribution = verticalContributions[outputY];
        for (uint32_t outputX = 0; outputX < modelExtent; ++outputX)
        {
            for (size_t channel = 0; channel < modelChannels; ++channel)
            {
                auto value = 0.0F;
                for (size_t i = 0; i < contribution.indices.size(); ++i)
                {
                    const auto inputY = contribution.indices[i];
                    const auto inputOffset = (inputY * modelExtent + outputX) * modelChannels + channel;
                    value += horizontallyResized[inputOffset] * contribution.weights[i];
                }
                const auto clampedValue = static_cast<uint8_t>(std::clamp(std::floor(value), 0.0F, 255.0F));
                const auto outputOffset = (static_cast<size_t>(outputY) * modelExtent + outputX) * modelChannels + channel;
                resizedPixels[outputOffset] = clampedValue;
            }
        }
    }
}

/// Crops+resizes+normalizes one already-decoded frame into the CHW slice for slot `slotIndex`,
/// starting at `outputPtr + slotIndex * perSlotTensorBytes`.
void writeSlotTensor(const RgbImage& decoded, const SlotRoi& roi, int8_t* outputPtr, uint32_t slotIndex)
{
    const auto cropped = cropRgb(decoded, roi.x, roi.y, roi.w, roi.h);

    std::vector<uint8_t> resizedPixels;
    resizeRgbLikePillow(cropped, resizedPixels);

    auto* slotOutputPtr = outputPtr + static_cast<size_t>(slotIndex) * perSlotTensorBytes; /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    for (uint32_t pixelY = 0; pixelY < modelExtent; ++pixelY)
    {
        for (uint32_t pixelX = 0; pixelX < modelExtent; ++pixelX)
        {
            const auto baseIndex = static_cast<size_t>(pixelY) * modelExtent + pixelX;
            for (size_t channel = 0; channel < modelChannels; ++channel)
            {
                const auto pixelOffset = baseIndex * modelChannels + channel;
                const auto value = static_cast<float>(resizedPixels[pixelOffset]) / 255.0F;
                const auto tensorIndex = channel * modelExtent * modelExtent + baseIndex;
                std::memcpy(
                    slotOutputPtr + tensorIndex * sizeof(float), &value, sizeof(value)); /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            }
        }
    }
}

/// Native function invoked (via nautilus::invoke) from the traced physical function. Decodes the
/// JPEG once, then crops/resizes/packs all 9 fixed slot ROIs into one [9,3,64,64] tensor -- the
/// single biggest win over calling FT_PREPROCESS_SLOT 9 times, which each independently receive
/// and decode their own copy of the same frame.
void preprocessAllSlotsToTensor(int8_t* fieldPtr, uint64_t fieldSize, int8_t* outputPtr)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    const auto field = std::span<const std::byte>(reinterpret_cast<const std::byte*>(fieldPtr), fieldSize);
    const auto base64Payload = stripDataUriPrefix(field);
    const auto jpegBytes = decodeBase64(base64Payload);
    const auto decoded = decodeJpegToRgb(std::span<const std::byte>(reinterpret_cast<const std::byte*>(jpegBytes.data()), jpegBytes.size()));

    for (uint32_t slotIndex = 0; slotIndex < slotCount; ++slotIndex)
    {
        writeSlotTensor(decoded, slotRois.at(slotIndex), outputPtr, slotIndex);
    }
}
}

VarVal FtPreprocessAllSlotsPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto imageValue = image.execute(record, arena).getRawValueAs<VariableSizedData>();

    auto output = arena.allocateVariableSizedData(nautilus::val<uint64_t>(outputTensorBytes));
    nautilus::invoke(preprocessAllSlotsToTensor, imageValue.getContent(), imageValue.getSize(), output.getContent());
    return VariableSizedData(output.getContent(), nautilus::val<uint64_t>(outputTensorBytes));
}

/// NOLINTNEXTLINE(readability-identifier-naming)
PhysicalFunctionRegistryReturnType FtPreprocessAllSlotsPhysicalFunction::createFT_PREPROCESS_ALL_SLOTS(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1, "FT_PREPROCESS_ALL_SLOTS must have exactly 1 child function (image)");
    return FtPreprocessAllSlotsPhysicalFunction(arguments.childFunctions[0]);
}
}
