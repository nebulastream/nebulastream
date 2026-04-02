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
#include <Util/InvokeMacro.hpp>
#include <Util/Strings.hpp>
#include <OutputParserRegistry.hpp>
#include <nautilus/inline.hpp>
#include <Util/InlineTagMacro.hpp>

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
    std::string parsedValue(zmij::float_buffer_size + 1, '\0');
    const auto size = zmij::write(parsedValue.data(), parsedValue.size(), value);
    parsedValue.resize(size);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseF64ZMIJ(
    const double value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string parsedValue(zmij::double_buffer_size + 1, '\0');
    const auto size = zmij::write(parsedValue.data(), parsedValue.size(), value);
    parsedValue.resize(size);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}


nautilus::val<uint64_t> ZMIJF32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return NAUTILUS_TAGGED_INVOKE("parse_to_string", parseF32ZMIJ, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> ZMIJF64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return NAUTILUS_TAGGED_INVOKE("parse_to_string", parseF64ZMIJ, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
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
