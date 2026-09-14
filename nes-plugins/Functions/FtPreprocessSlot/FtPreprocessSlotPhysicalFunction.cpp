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

#include <Functions/FtPreprocessSlotPhysicalFunction.hpp>

#include <algorithm>
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

/// Single-header JPEG (and friends) decoder, public domain. Only the JPEG path is exercised here
/// (i/cam's frames are baseline JPEG), so every other format is compiled in but unused.
#define STB_IMAGE_IMPLEMENTATION
#define STBI_ONLY_JPEG
#define STBI_NO_STDIO
#include <stb_image.h>

namespace NES
{

FtPreprocessSlotPhysicalFunction::FtPreprocessSlotPhysicalFunction(
    PhysicalFunction image, PhysicalFunction x, PhysicalFunction y, PhysicalFunction w, PhysicalFunction h)
    : image(std::move(image)), x(std::move(x)), y(std::move(y)), w(std::move(w)), h(std::move(h))
{
}

namespace
{
/// Matches the training pipeline's IMG_SIZE=64, 3-channel RGB convention (see preprocess.py /
/// train_model.py in the Dropbox "SSC model" folder) and the AutoVI demo's tensor layout, so the
/// rest of the MODEL_INFERENCE plumbing (tensor shape matching, IREE compile) needs no changes.
constexpr uint32_t modelExtent = 64;
constexpr uint32_t modelChannels = 3;
constexpr uint64_t outputTensorBytes = uint64_t{1} * modelChannels * modelExtent * modelExtent * sizeof(float);

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

/// i/cam's "data" field is a full data-URI (`data:image/jpeg;base64,<...>`), not a bare base64
/// string -- the leading `data:image/jpeg;base64,` text is not itself valid base64 (':' and ';'
/// aren't in the base64 alphabet), so decoding the whole field with FROM_BASE64 corrupts the
/// image bytes. Confirmed empirically: stb_image reported "unknown image type" on real i/cam
/// frames until this prefix was stripped first. Strip up to and including the first ',' (the
/// data-URI separator, per RFC 2397) rather than hardcoding the prefix's length, since the media
/// type could in principle vary.
std::span<const std::byte> stripDataUriPrefix(std::span<const std::byte> field)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-char to search for the data-URI ',' separator
    const auto* commaPtr = static_cast<const std::byte*>(std::memchr(field.data(), ',', field.size()));
    if (commaPtr == nullptr)
    {
        throw FormattingError("FT_PREPROCESS_SLOT expected a data-URI (\"data:<type>;base64,<...>\") but found no ',' separator");
    }
    const auto prefixLength = static_cast<size_t>(commaPtr - field.data()) + 1;
    return field.subspan(prefixLength);
}

/// Base64-decodes the data-URI payload (post prefix-strip) via OpenSSL, same approach as
/// FromBase64PhysicalFunction -- duplicated rather than reused because that function operates on
/// the whole field, ignorant of the data-URI prefix, and its runtime piece is tightly coupled to
/// FromBase64LogicalFunction's own registry-plumbed FROM_BASE64 name.
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
        throw FormattingError("FT_PREPROCESS_SLOT could not base64-decode the data-URI payload");
    }

    auto actualLen = static_cast<size_t>(decodedLen);
    /// EVP_DecodeBlock doesn't account for padding -- subtract padding bytes, same as
    /// FromBase64PhysicalFunction's native decode helper.
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
        throw FormattingError("FT_PREPROCESS_SLOT could not decode JPEG payload: {}", stbi_failure_reason());
    }
    SCOPE_EXIT
    {
        stbi_image_free(decoded);
    };

    const auto pixelCount = static_cast<size_t>(width) * static_cast<size_t>(height) * modelChannels;
    return {.width = static_cast<uint32_t>(width), .height = static_cast<uint32_t>(height), .pixels = {decoded, decoded + pixelCount}};
}

/// Extracts one HBW slot's fixed pixel rect out of the full i/cam frame, before resizing. Kept as
/// its own step (rather than folded into the resize) so training-time crop == query-time crop by
/// construction (see preprocess.py's "crop to slot ROI" step).
RgbImage cropRgb(const RgbImage& image, uint32_t x, uint32_t y, uint32_t w, uint32_t h)
{
    if (w == 0 or h == 0 or x + w > image.width or y + h > image.height)
    {
        throw FormattingError(
            "FT_PREPROCESS_SLOT crop rect ({}, {}, {}, {}) is out of bounds for a {}x{} frame", x, y, w, h, image.width, image.height);
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

/// Precomputes a separable bilinear resize's per-output-pixel source indices/weights, replicating
/// PIL's `Image.BILINEAR` (a triangle filter, not a naive 2x2 sample) so training (Python/PIL) and
/// query time (here) produce numerically matching tensors. Ported from
/// nebulastream/nes-demo-process-intelligence's AUTOVI_PREPROCESS_IMAGE, which solved this same
/// train/query parity problem for a different (PNG, whole-frame) source.
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

/// Native function invoked (via nautilus::invoke) from the traced physical function. Everything
/// here -- data-URI strip, base64 decode, JPEG decode, crop, resize, normalize, HWC->CHW
/// transpose -- runs as plain C++, not traced Nautilus IR; only the pointers/sizes crossing the
/// boundary are `nautilus::val`s. `fieldPtr`/`fieldSize` is the RAW MQTT field value (e.g. i/cam's
/// full "data:image/jpeg;base64,<...>" string) -- do not wrap the query's argument in
/// FROM_BASE64, this function does its own data-URI-aware base64 decode.
void preprocessSlotToTensor(int8_t* fieldPtr, uint64_t fieldSize, uint32_t x, uint32_t y, uint32_t w, uint32_t h, int8_t* outputPtr)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    const auto field = std::span<const std::byte>(reinterpret_cast<const std::byte*>(fieldPtr), fieldSize);
    const auto base64Payload = stripDataUriPrefix(field);
    const auto jpegBytes = decodeBase64(base64Payload);
    const auto decoded = decodeJpegToRgb(std::span<const std::byte>(reinterpret_cast<const std::byte*>(jpegBytes.data()), jpegBytes.size()));
    const auto cropped = cropRgb(decoded, x, y, w, h);

    std::vector<uint8_t> resizedPixels;
    resizeRgbLikePillow(cropped, resizedPixels);

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
                std::memcpy(outputPtr + tensorIndex * sizeof(float), &value, sizeof(value)); /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            }
        }
    }
}
}

VarVal FtPreprocessSlotPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto imageValue = image.execute(record, arena).getRawValueAs<VariableSizedData>();
    const auto xValue = x.execute(record, arena).getRawValueAs<nautilus::val<uint32_t>>();
    const auto yValue = y.execute(record, arena).getRawValueAs<nautilus::val<uint32_t>>();
    const auto wValue = w.execute(record, arena).getRawValueAs<nautilus::val<uint32_t>>();
    const auto hValue = h.execute(record, arena).getRawValueAs<nautilus::val<uint32_t>>();

    auto output = arena.allocateVariableSizedData(nautilus::val<uint64_t>(outputTensorBytes));
    nautilus::invoke(
        preprocessSlotToTensor, imageValue.getContent(), imageValue.getSize(), xValue, yValue, wValue, hValue, output.getContent());
    return VariableSizedData(output.getContent(), nautilus::val<uint64_t>(outputTensorBytes));
}

/// NOLINTNEXTLINE(readability-identifier-naming)
PhysicalFunctionRegistryReturnType FtPreprocessSlotPhysicalFunction::createFT_PREPROCESS_SLOT(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5, "FT_PREPROCESS_SLOT must have exactly 5 child functions (image, x, y, w, h)");
    return FtPreprocessSlotPhysicalFunction(
        arguments.childFunctions[0], arguments.childFunctions[1], arguments.childFunctions[2], arguments.childFunctions[3], arguments.childFunctions[4]);
}
}
