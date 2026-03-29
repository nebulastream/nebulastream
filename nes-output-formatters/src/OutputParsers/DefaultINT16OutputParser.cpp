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

#include <OutputParsers/DefaultINT16OutputParser.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Util/Strings.hpp>
#include <OutputParserRegistry.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES
{
namespace
{
uint64_t parse(
    int32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

nautilus::val<uint64_t> DefaultINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(parse, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT16OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT16OutputParser>();
}
}
