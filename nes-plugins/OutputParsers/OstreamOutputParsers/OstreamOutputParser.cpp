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

#include <OstreamOutputParser.hpp>

#include <cstdint>
#include <sstream>
#include <string>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/InvokeMacro.hpp>
#include <OutputParserRegistry.hpp>

namespace NES
{
template <typename T>
uint64_t parseWithOstream(
    const T value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::ostringstream oss;
    oss << value;
    const std::string str = oss.str();
    return writeBytesToBuffer(str.data(), str.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

nautilus::val<uint64_t> OstreamF32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseWithOstream<float>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> OstreamF64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string",
        parseWithOstream<double>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterOstreamF32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<OstreamF32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterOstreamF64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<OstreamF64OutputParser>();
}
}
