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
#include <OutputParserRegistry.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::DefaultOutputParser
{
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

uint64_t parseBool(
    const bool value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = value ? "true" : "false";
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

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

uint64_t parseVarsized(
    const int8_t* valueAddress,
    const uint64_t valueSize,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const bool quoted,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string stringFormattedValue{reinterpret_cast<const char*>(valueAddress), valueSize};
    if (quoted)
    {
        /// Replace all " instances in the string with ""
        std::string stringWithDoubledQuotes;
        for (const char character : stringFormattedValue)
        {
            if (character == '"')
            {
                stringWithDoubledQuotes.append("\"\"");
            }
            else
            {
                stringWithDoubledQuotes += character;
            }
        }
        stringFormattedValue = "\"" + stringWithDoubledQuotes + "\"";
    }
    return writeValueToBuffer(stringFormattedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

namespace NES
{
nautilus::val<uint64_t> DefaultCHAROutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<char>>();
    return nautilus::invoke(
        DefaultOutputParser::parseChar, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultF32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return nautilus::invoke(
        DefaultOutputParser::parseF32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultF64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return nautilus::invoke(
        DefaultOutputParser::parseF64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT8OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseInt8, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseInt16, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseInt32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int64_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseInt64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultBOOLOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<bool>>();
    return nautilus::invoke(
        DefaultOutputParser::parseBool, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT8OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint8_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseUint8, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint16_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseUint16, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint32_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseUint32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint64_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseUint64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultVARSIZEDOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<VariableSizedData>();
    return nautilus::invoke(
        DefaultOutputParser::parseVarsized,
        castedVal.getContent(),
        castedVal.getSize(),
        startingAddress,
        remainingSize,
        nautilus::val<bool>{false},
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> QuotedVARSIZEDOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto castedVal = value.getRawValueAs<VariableSizedData>();
    return nautilus::invoke(
        DefaultOutputParser::parseVarsized,
        castedVal.getContent(),
        castedVal.getSize(),
        startingAddress,
        remainingSize,
        nautilus::val<bool>{true},
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultFIXEDSIZEDOutputParser::parseAndWrite(
    const VarVal&,
    const nautilus::val<uint64_t>&,
    const RecordBuffer&,
    const nautilus::val<AbstractBufferProvider*>&,
    const nautilus::val<int8_t*>&,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    throw NotImplemented("FIXEDSIZED does not have a default output parser implementation.");
}

nautilus::val<uint64_t> DefaultSTRUCTOutputParser::parseAndWrite(
    const VarVal&,
    const nautilus::val<uint64_t>&,
    const RecordBuffer&,
    const nautilus::val<AbstractBufferProvider*>&,
    const nautilus::val<int8_t*>&,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    throw NotImplemented("STRUCT does not have a default output parser implementation.");
}

nautilus::val<uint64_t> DefaultUNDEFINEDOutputParser::parseAndWrite(
    const VarVal&,
    const nautilus::val<uint64_t>&,
    const RecordBuffer&,
    const nautilus::val<AbstractBufferProvider*>&,
    const nautilus::val<int8_t*>&,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    throw UnknownDataType("UNDEFINED does not have a default output parser implementation.");
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

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultVARSIZEDOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultVARSIZEDOutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterQuotedVARSIZEDOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<QuotedVARSIZEDOutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultFIXEDSIZEDOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultFIXEDSIZEDOutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultSTRUCTOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultSTRUCTOutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUNDEFINEDOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUNDEFINEDOutputParser>();
}
}
