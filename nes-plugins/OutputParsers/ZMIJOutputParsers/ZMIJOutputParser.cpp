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

#include <ZMIJOutputParser.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <zmij.h>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <Util/Strings.hpp>
#include <nautilus/inline.hpp>
#include <OutputParserRegistry.hpp>

namespace NES
{
NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseF32ZMIJ(
    const float value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    /// Fast path: the widest float is zmij::float_buffer_size, so if that fits in the remaining main-buffer
    /// space we let zmij::write serialize straight into the output buffer -- no per-value std::string heap
    /// allocation and no strlen (zmij::write returns the byte count).
    if (remainingSpace >= zmij::float_buffer_size)
    {
        return zmij::write(reinterpret_cast<char*>(bufferStartingAddress), remainingSpace, value);
    }
    /// Boundary: not enough contiguous main-buffer room for the worst case -- serialize into a stack buffer
    /// and let writeBytesToBuffer spill the overflow into a child buffer.
    char tmp[zmij::float_buffer_size + 1];
    const auto size = zmij::write(tmp, sizeof(tmp), value);
    return writeBytesToBuffer(tmp, size, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseF64ZMIJ(
    const double value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    /// Fast path: zmij::write straight into the output buffer when the worst-case width fits -- no per-value
    /// std::string heap allocation, no strlen.
    if (remainingSpace >= zmij::double_buffer_size)
    {
        return zmij::write(reinterpret_cast<char*>(bufferStartingAddress), remainingSpace, value);
    }
    /// Boundary: serialize into a stack buffer and let writeBytesToBuffer spill into a child buffer.
    char tmp[zmij::double_buffer_size + 1];
    const auto size = zmij::write(tmp, sizeof(tmp), value);
    return writeBytesToBuffer(tmp, size, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

nautilus::val<uint64_t> ZMIJF32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseF32ZMIJ, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> ZMIJF64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseF64ZMIJ, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterZMIJF32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<ZMIJF32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterZMIJF64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<ZMIJF64OutputParser>();
}
}
