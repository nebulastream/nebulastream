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

#include "ExecutableFunctionImageManip.hpp"

#include <cstdint>
#include <numeric>
#include <ranges>
#include <Execution/Functions/ExecutableFunctionConstantValue.hpp>
#include <Util/Ranges.hpp>
#include <netinet/in.h>
#include <ExecutableFunctionRegistry.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Functions
{
static constexpr size_t DEFAULT_IMAGE_WIDTH = 640;
static constexpr size_t DEFAULT_IMAGE_HEIGHT = 480;


VariableSizedData toBase64(VariableSizedData input, ArenaRef& arena)
{
    VariableSizedData newBuffer = arena.allocateVariableSizedData((input.getContentSize() + 2) / 3 * 4);

    nautilus::invoke(
        +[](uint32_t input_length, int8_t* data, int8_t* encoded)
        {
            static constexpr char encodingTable[]
                = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
                   'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                   's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

            size_t encodedIndex = 0;
            for (size_t i = 0; i < input_length; i += 3)
            {
                // Cast to unsigned before bit operations to avoid sign extension issues
                const uint32_t octet_a = i < input_length ? static_cast<uint8_t>(data[i]) : 0;
                uint32_t octet_b = i + 1 < input_length ? static_cast<uint8_t>(data[i + 1]) : 0;
                uint32_t octet_c = i + 2 < input_length ? static_cast<uint8_t>(data[i + 2]) : 0;

                uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;

                encoded[encodedIndex++] = encodingTable[(triple >> 18) & 0x3F];
                encoded[encodedIndex++] = encodingTable[(triple >> 12) & 0x3F];
                encoded[encodedIndex++] = i + 1 < input_length ? encodingTable[(triple >> 6) & 0x3F] : '=';
                encoded[encodedIndex++] = i + 2 < input_length ? encodingTable[triple & 0x3F] : '=';
            }
        },
        input.getContentSize(),
        input.getContent(),
        newBuffer.getContent());

    return newBuffer;
}

size_t getDecodedSize(uint32_t encodedLength, const int8_t* encoded)
{
    if (encodedLength == 0)
    {
        return 0;
    }

    // Basic formula: 3 bytes for every 4 Base64 characters
    size_t maxDecodedSize = (encodedLength * 3) / 4;

    // Adjust for padding characters
    if (encodedLength >= 1 && encoded[encodedLength - 1] == '=')
    {
        maxDecodedSize--; // One padding character

        if (encodedLength >= 2 && encoded[encodedLength - 2] == '=')
        {
            maxDecodedSize--; // Two padding characters
        }
    }

    return maxDecodedSize;
}

VariableSizedData fromBase64(VariableSizedData input, ArenaRef& arena)
{
    auto decodedSize = nautilus::invoke(
        +[](size_t size, int8_t* encoded) { return getDecodedSize(size, encoded); }, input.getContentSize(), input.getContent());
    VariableSizedData newBuffer = arena.allocateVariableSizedData(decodedSize);
    nautilus::invoke(
        +[](uint32_t input_length, int8_t* encoded, int8_t* decoded)
        {
            static constexpr int8_t decodingTable[256]
                = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, // '+' is 62, '/' is 63
                   52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, // '0'-'9' are 52-61
                   -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, // 'A'-'O' are 0-14
                   15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, // 'P'-'Z' are 15-25
                   -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, // 'a'-'o' are 26-40
                   41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1, // 'p'-'z' are 41-51
                   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};

            size_t decodedIndex = 0;

            // Process the input in blocks of 4 Base64 characters
            for (size_t i = 0; i < input_length; i += 4)
            {
                // Get values for each Base64 character in the group
                uint32_t sextet_a = i < input_length && encoded[i] != '=' ? decodingTable[static_cast<uint8_t>(encoded[i])] : 0;
                uint32_t sextet_b = i + 1 < input_length && encoded[i + 1] != '=' ? decodingTable[static_cast<uint8_t>(encoded[i + 1])] : 0;
                uint32_t sextet_c = i + 2 < input_length && encoded[i + 2] != '=' ? decodingTable[static_cast<uint8_t>(encoded[i + 2])] : 0;
                uint32_t sextet_d = i + 3 < input_length && encoded[i + 3] != '=' ? decodingTable[static_cast<uint8_t>(encoded[i + 3])] : 0;

                // Combine the 6-bit values into the original bytes
                uint32_t triple = (sextet_a << 18) | (sextet_b << 12) | (sextet_c << 6) | sextet_d;

                // Output the original data bytes
                if (decodedIndex < input_length * 3 / 4)
                {
                    decoded[decodedIndex++] = static_cast<int8_t>((triple >> 16) & 0xFF);
                }

                // Only output the second byte if we don't have a padding character in the third position
                if (decodedIndex < input_length * 3 / 4 && i + 2 < input_length && encoded[i + 2] != '=')
                {
                    decoded[decodedIndex++] = static_cast<int8_t>((triple >> 8) & 0xFF);
                }

                // Only output the third byte if we don't have a padding character in the fourth position
                if (decodedIndex < input_length * 3 / 4 && i + 3 < input_length && encoded[i + 3] != '=')
                {
                    decoded[decodedIndex++] = static_cast<int8_t>(triple & 0xFF);
                }
            }
        },
        input.getContentSize(),
        input.getContent(),
        newBuffer.getContent());

    return newBuffer;
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

VariableSizedData drawRectangle(
    VariableSizedData input,
    nautilus::val<uint32_t> x,
    nautilus::val<uint32_t> y,
    nautilus::val<uint32_t> width,
    nautilus::val<uint32_t> height,
    ArenaRef& arena)
{
    VariableSizedData imageWithRectangle = arena.allocateVariableSizedData(input.getContentSize());

    nautilus::invoke(
        +[](uint32_t input_length, int8_t* data, uint32_t x, uint32_t y, uint32_t height, uint32_t width, int8_t* destination)
        {
            auto input = std::span{reinterpret_cast<int8_t*>(data), input_length};
            auto image = std::span{destination, input_length};
            std::ranges::copy(input, image.data());

            /// Upper and Lower
            for (size_t i = 0; i < width; i++)
            {
                set_pixel_color(image, x + i, y, {200, 50, 0});
                set_pixel_color(image, x + i, y + 1, {200, 50, 0});

                set_pixel_color(image, x + i, y + height - 1, {200, 50, 0});
                set_pixel_color(image, x + i, y + height, {200, 50, 0});
            }

            /// Left and right
            for (size_t i = 0; i < height; i++)
            {
                set_pixel_color(image, x, y + i, {200, 50, 0});
                set_pixel_color(image, x + 1, y + i, {200, 50, 0});

                set_pixel_color(image, x + width - 1, y + i, {200, 50, 0});
                set_pixel_color(image, x + width, y + i, {200, 50, 0});
            }
        },
        input.getContentSize(),
        input.getContent(),
        x,
        y,
        height,
        width,
        imageWithRectangle.getContent());

    return imageWithRectangle;
}

VariableSizedData mono16ToGray(VariableSizedData input, ArenaRef& arena)
{
    VariableSizedData grayScaleImage = arena.allocateVariableSizedData(input.getContentSize());

    nautilus::invoke(
        +[](uint32_t input_length, int8_t* data, int8_t* dest)
        {
            std::span mono16{data, input_length};
            std::ranges::copy(std::views::transform(mono16 | std::views::chunk(2), [](auto p) { return p[0]; }), dest);
        },
        input.getContentSize(),
        input.getContent(),
        grayScaleImage.getContent());

    return grayScaleImage;
}

VarVal ExecutableFunctionImageManip::execute(const Record& record, ArenaRef& arena) const
{
    if (functionName == "ToBase64")
    {
        return toBase64(childFunctions[0]->execute(record, arena).cast<VariableSizedData>(), arena);
    }
    else if (functionName == "FromBase64")
    {
        return fromBase64(childFunctions[0]->execute(record, arena).cast<VariableSizedData>(), arena);
    }
    else if (functionName == "Mono16ToGray")
    {
        return mono16ToGray(childFunctions[0]->execute(record, arena).cast<VariableSizedData>(), arena);
    }
    else if (functionName == "DrawRectangle")
    {
        return drawRectangle(
            childFunctions[0]->execute(record, arena).cast<VariableSizedData>(),
            childFunctions[1]->execute(record, arena).cast<nautilus::val<uint32_t>>(),
            childFunctions[2]->execute(record, arena).cast<nautilus::val<uint32_t>>(),
            childFunctions[3]->execute(record, arena).cast<nautilus::val<uint32_t>>(),
            childFunctions[4]->execute(record, arena).cast<nautilus::val<uint32_t>>(),
            arena);
    }
    throw NotImplemented("ImageManip{} is not implemented!", functionName);
}

ExecutableFunctionImageManip::ExecutableFunctionImageManip(std::string functionName, std::vector<std::unique_ptr<Function>> childFunction)
    : functionName(std::move(functionName)), childFunctions(std::move(childFunction))
{
}


ExecutableFunctionRegistryReturnType
ExecutableFunctionGeneratedRegistrar::RegisterImageManipToBase64ExecutableFunction(ExecutableFunctionRegistryArguments arguments)
{
    std::vector<std::unique_ptr<Functions::Function>> children;
    children.push_back(std::move(arguments.childFunctions[1]));
    return std::make_unique<ExecutableFunctionImageManip>("ToBase64", std::move(children));
}


ExecutableFunctionRegistryReturnType
ExecutableFunctionGeneratedRegistrar::RegisterImageManipFromBase64ExecutableFunction(ExecutableFunctionRegistryArguments arguments)
{
    std::vector<std::unique_ptr<Functions::Function>> children;
    children.push_back(std::move(arguments.childFunctions[1]));
    return std::make_unique<ExecutableFunctionImageManip>("FromBase64", std::move(children));
}

ExecutableFunctionRegistryReturnType
ExecutableFunctionGeneratedRegistrar::RegisterImageManipMono16ToGrayExecutableFunction(ExecutableFunctionRegistryArguments arguments)
{
    std::vector<std::unique_ptr<Functions::Function>> children;
    children.push_back(std::move(arguments.childFunctions[1]));
    return std::make_unique<ExecutableFunctionImageManip>("Mono16ToGray", std::move(children));
}

ExecutableFunctionRegistryReturnType
ExecutableFunctionGeneratedRegistrar::RegisterImageManipDrawRectangleExecutableFunction(ExecutableFunctionRegistryArguments arguments)
{
    std::vector<std::unique_ptr<Functions::Function>> children;
    children.push_back(std::move(arguments.childFunctions[1]));
    children.push_back(std::move(arguments.childFunctions[2]));
    children.push_back(std::move(arguments.childFunctions[3]));
    children.push_back(std::move(arguments.childFunctions[4]));
    children.push_back(std::move(arguments.childFunctions[5]));
    return std::make_unique<ExecutableFunctionImageManip>("DrawRectangle", std::move(children));
}

}
