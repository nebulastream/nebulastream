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

#include <XJBOutputParser.hpp>

#include <cstdint>
#include <ftoa.h>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <nautilus/inline.hpp>
#include <OutputParserRegistry.hpp>

namespace NES
{
/// xjb_ftoa returns a pointer to the terminating '\0'; the written digit count is (end - buf).
/// The buffer requirement is 21 bytes (float) / 34 bytes (double) -- see xjb ftoa.h.
NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseF32XJB(
    const float value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    constexpr uint64_t kFloatBuf = 21;
    if (remainingSpace >= kFloatBuf)
    {
        char* out = reinterpret_cast<char*>(bufferStartingAddress);
        return static_cast<uint64_t>(xjb_ftoa(value, out) - out);
    }
    char tmp[kFloatBuf];
    const auto size = static_cast<uint64_t>(xjb_ftoa(value, tmp) - tmp);
    return writeBytesToBuffer(tmp, size, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseF64XJB(
    const double value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    constexpr uint64_t kDoubleBuf = 34;
    if (remainingSpace >= kDoubleBuf)
    {
        char* out = reinterpret_cast<char*>(bufferStartingAddress);
        return static_cast<uint64_t>(xjb_ftoa(value, out) - out);
    }
    char tmp[kDoubleBuf];
    const auto size = static_cast<uint64_t>(xjb_ftoa(value, tmp) - tmp);
    return writeBytesToBuffer(tmp, size, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

nautilus::val<uint64_t> XJBF32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseF32XJB, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> XJBF64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseF64XJB, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterXJBF32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<XJBF32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterXJBF64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<XJBF64OutputParser>();
}
}
