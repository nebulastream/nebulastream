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

#include <OutputParsers/DefaultOutputParsers.hpp>

#include <cstdint>
#include <memory>
#include <string>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <nautilus/inline.hpp>
#include <OutputParserRegistry.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseChar(
    const char value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue{value};
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseF32(
    const float value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = formatFloat(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseF64(
    const double value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = formatFloat(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseInt8(
    const int32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseInt16(
    const int32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseInt32(
    const int32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseInt64(
    const int64_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseBool(
    const bool value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(static_cast<int>(value));
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseUint8(
    const uint8_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseUint16(
    const uint16_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseUint32(
    const uint32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)
uint64_t parseUint64(
    const uint64_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

nautilus::val<uint64_t> DefaultCHAROutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<char>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseChar, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultF32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseF32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultF64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseF64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT8OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseInt8, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseInt16, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseInt32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int64_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseInt64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultBOOLOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<bool>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseBool, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT8OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint8_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseUint8, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint16_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseUint16, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint32_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseUint32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint64_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseUint64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultCHAROutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultCHAROutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultF32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultF32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultF64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultF64OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT8OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT8OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT16OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT16OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT64OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultBOOLOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultBOOLOutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUINT8OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT8OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUINT16OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT16OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUINT32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUINT64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT64OutputParser>();
}
}
