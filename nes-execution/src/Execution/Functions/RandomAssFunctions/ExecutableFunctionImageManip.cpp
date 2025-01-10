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
#include <ranges>
#include <Execution/Functions/ExecutableFunctionConstantValue.hpp>
#include <Execution/Functions/RandomAssFunctions/ExecutableFunctionImageManip.hpp>
#include <Util/Ranges.hpp>
#include <netinet/in.h>

#include <ExecutableFunctionRegistry.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Functions
{
static constexpr size_t DEFAULT_IMAGE_WIDTH = 640;
static constexpr size_t DEFAULT_IMAGE_HEIGHT = 480;


VariableSizedData toBase64(VariableSizedData input, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
{
    auto encoded = nautilus::invoke(
        +[](uint32_t input_length, int8_t* data, Memory::AbstractBufferProvider* bufferProvider)
        {
            static const char encodingTable[]
                = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
                   'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                   's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};


            auto buffer = bufferProvider->getUnpooledBuffer((input_length + 2) / 3 * 4 + 4).value();
            *buffer.getBuffer<uint32_t>() = ((input_length + 2) / 3) * 4;
            auto encoded = buffer.getBuffer<uint8_t>() + sizeof(uint32_t);

            size_t encodedIndex = 0;
            for (size_t i = 0; i < input_length; i += 3)
            {
                uint32_t octet_a = i < input_length ? data[i] : 0;
                uint32_t octet_b = i + 1 < input_length ? data[i + 1] : 0;
                uint32_t octet_c = i + 2 < input_length ? data[i + 2] : 0;

                uint32_t triple = (octet_a << 16) + (octet_b << 8) + octet_c;

                encoded[encodedIndex++] = encodingTable[(triple >> 18) & 0x3F];
                encoded[encodedIndex++] = encodingTable[(triple >> 12) & 0x3F];
                encoded[encodedIndex++] = i + 1 < input_length ? encodingTable[(triple >> 6) & 0x3F] : '=';
                encoded[encodedIndex++] = i + 2 < input_length ? encodingTable[triple & 0x3F] : '=';
            }

            buffer.retain();
            return buffer.getBuffer<uint8_t>();
        },
        input.getSize(),
        input.getContent(),
        bufferProvider);

    return VariableSizedData(encoded);
}

struct YUYV
{
};

void set_pixel_color(std::span<uint8_t> image, uint32_t x, uint32_t y, std::array<uint8_t, 3> yuv)
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
    nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
{
    auto imageWithRectangle = nautilus::invoke(
        +[](uint32_t input_length,
            int8_t* data,
            uint32_t x,
            uint32_t y,
            uint32_t height,
            uint32_t width,
            Memory::AbstractBufferProvider* bufferProvider)
        {
            auto buffer = bufferProvider->getUnpooledBuffer(input_length + 4).value();
            buffer.getBuffer<uint32_t>()[0] = input_length;
            auto input = std::span{reinterpret_cast<uint8_t*>(data), input_length};
            auto image = std::span{buffer.getBuffer<uint8_t>() + sizeof(uint32_t), input_length};
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

            buffer.retain();
            return buffer.getBuffer<uint8_t>();
        },
        input.getSize(),
        input.getContent(),
        x,
        y,
        height,
        width,
        bufferProvider);

    return VariableSizedData(imageWithRectangle);
}

VariableSizedData mono16ToGray(VariableSizedData input, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
{
    auto encoded = nautilus::invoke(
        +[](uint32_t input_length, int8_t* data, Memory::AbstractBufferProvider* bufferProvider)
        {
            std::span mono16{data, input_length};
            auto buffer = bufferProvider->getUnpooledBuffer(4 + input_length / 2).value();
            *buffer.getBuffer<uint32_t>() = input_length / 2;
            auto grayData = buffer.getBuffer<uint8_t>() + sizeof(uint32_t);
            std::ranges::copy(std::views::transform(mono16 | std::views::chunk(2), [](auto p) { return p[0]; }), grayData);
            buffer.retain();
            return buffer.getBuffer<uint8_t>();
        },
        input.getSize(),
        input.getContent(),
        bufferProvider);

    return VariableSizedData(encoded);
}

VarVal ExecutableFunctionImageManip::execute(const Record& record, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) const
{
    if (functionName == "ToBase64")
    {
        return toBase64(childFunctions[0]->execute(record, bufferProvider).cast<VariableSizedData>(), bufferProvider);
    }
    else if (functionName == "Mono16ToGray")
    {
        return mono16ToGray(childFunctions[0]->execute(record, bufferProvider).cast<VariableSizedData>(), bufferProvider);
    }
    else if (functionName == "DrawRectangle")
    {
        return drawRectangle(
            childFunctions[0]->execute(record, bufferProvider).cast<VariableSizedData>(),
            childFunctions[1]->execute(record, bufferProvider).cast<nautilus::val<uint32_t>>(),
            childFunctions[2]->execute(record, bufferProvider).cast<nautilus::val<uint32_t>>(),
            childFunctions[3]->execute(record, bufferProvider).cast<nautilus::val<uint32_t>>(),
            childFunctions[4]->execute(record, bufferProvider).cast<nautilus::val<uint32_t>>(),
            bufferProvider);
    }
    throw NotImplemented("ImageManip{} is not implemented!", functionName);
}

ExecutableFunctionImageManip::ExecutableFunctionImageManip(std::string functionName, std::vector<std::unique_ptr<Function>> childFunction)
    : functionName(std::move(functionName)), childFunctions(std::move(childFunction))
{
}


std::unique_ptr<ExecutableFunctionRegistryReturnType>
ExecutableFunctionGeneratedRegistrar::RegisterImageManipToBase64ExecutableFunction(ExecutableFunctionRegistryArguments arguments)
{
    std::vector<std::unique_ptr<Functions::Function>> children;
    children.push_back(std::move(arguments.childFunctions[1]));
    return std::make_unique<ExecutableFunctionImageManip>("ToBase64", std::move(children));
}

std::unique_ptr<ExecutableFunctionRegistryReturnType>
ExecutableFunctionGeneratedRegistrar::RegisterImageManipMono16ToGrayExecutableFunction(ExecutableFunctionRegistryArguments arguments)
{
    std::vector<std::unique_ptr<Functions::Function>> children;
    children.push_back(std::move(arguments.childFunctions[1]));
    return std::make_unique<ExecutableFunctionImageManip>("Mono16ToGray", std::move(children));
}

std::unique_ptr<ExecutableFunctionRegistryReturnType>
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
