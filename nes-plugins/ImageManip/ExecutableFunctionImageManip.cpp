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

#include <Functions/PhysicalFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <openssl/evp.h>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

class PhysicalFunctionImageManip final : public PhysicalFunctionConcept
{
public:
    explicit PhysicalFunctionImageManip(std::string functionName, std::vector<PhysicalFunction> childFunctions);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::string functionName;
    const std::vector<PhysicalFunction> childFunctions;
};

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
}
