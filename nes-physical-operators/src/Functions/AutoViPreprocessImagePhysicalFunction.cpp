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

#include <Functions/AutoViPreprocessImagePhysicalFunction.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <span>
#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <png.h>
#include <scope_guard.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{

AutoViPreprocessImagePhysicalFunction::AutoViPreprocessImagePhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
}

namespace
{
constexpr uint32_t modelExtent = 64;
constexpr uint32_t modelChannels = 3;
constexpr uint64_t outputTensorBytes = uint64_t{1} * modelChannels * modelExtent * modelExtent * sizeof(float);

struct DecodedRgbImage
{
    png_uint_32 width;
    png_uint_32 height;
    std::vector<uint8_t> pixels;
};

struct ResampleContribution
{
    std::vector<size_t> indices;
    std::vector<float> weights;
};

DecodedRgbImage decodePngToRgb(std::span<const std::byte> pngBytes)
{
    png_image image{};
    image.version = PNG_IMAGE_VERSION;
    if (!png_image_begin_read_from_memory(&image, pngBytes.data(), pngBytes.size()))
    {
        throw FormattingError("AUTOVI_PREPROCESS_IMAGE could not read PNG header: {}", image.message);
    }
    SCOPE_EXIT
    {
        png_image_free(&image);
    };

    image.format = PNG_FORMAT_RGB;
    std::vector<uint8_t> pixels(PNG_IMAGE_SIZE(image));
    if (!png_image_finish_read(&image, nullptr, pixels.data(), 0, nullptr))
    {
        throw FormattingError("AUTOVI_PREPROCESS_IMAGE could not decode PNG payload: {}", image.message);
    }

    return {.width = image.width, .height = image.height, .pixels = std::move(pixels)};
}

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

void resizeRgbLikePillow(const DecodedRgbImage& image, std::vector<uint8_t>& resizedPixels)
{
    if (image.width == modelExtent && image.height == modelExtent)
    {
        resizedPixels = image.pixels;
        return;
    }

    const auto horizontalContributions = computeTriangleContributions(image.width, modelExtent);
    const auto verticalContributions = computeTriangleContributions(image.height, modelExtent);

    std::vector<float> horizontallyResized(static_cast<size_t>(image.height) * modelExtent * modelChannels, 0.0F);
    for (png_uint_32 y = 0; y < image.height; ++y)
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

void preprocessPngToAutoViTensor(int8_t* inputPtr, uint64_t inputSize, int8_t* outputPtr)
{
    const auto image = decodePngToRgb(std::span<const std::byte>(reinterpret_cast<const std::byte*>(inputPtr), inputSize));
    std::vector<uint8_t> resizedPixels;
    resizeRgbLikePillow(image, resizedPixels);

    for (uint32_t y = 0; y < modelExtent; ++y)
    {
        for (uint32_t x = 0; x < modelExtent; ++x)
        {
            const auto baseIndex = static_cast<size_t>(y) * modelExtent + x;

            for (size_t channel = 0; channel < modelChannels; ++channel)
            {
                const auto pixelOffset = baseIndex * modelChannels + channel;
                const auto value = static_cast<float>(resizedPixels[pixelOffset]) / 255.0F;
                const auto tensorIndex = channel * modelExtent * modelExtent + baseIndex;
                std::memcpy(outputPtr + tensorIndex * sizeof(float), &value, sizeof(value));
            }
        }
    }
}
}

VarVal AutoViPreprocessImagePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto inputValue = childPhysicalFunction.execute(record, arena).getRawValueAs<VariableSizedData>();
    auto output = arena.allocateVariableSizedData(nautilus::val<uint64_t>(outputTensorBytes));
    nautilus::invoke(preprocessPngToAutoViTensor, inputValue.getContent(), inputValue.getSize(), output.getContent());
    return VariableSizedData(output.getContent(), nautilus::val<uint64_t>(outputTensorBytes));
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterAUTOVI_PREPROCESS_IMAGEPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "AUTOVI_PREPROCESS_IMAGE must have exactly one child function");
    return AutoViPreprocessImagePhysicalFunction(args.childFunctions[0]);
}
}
