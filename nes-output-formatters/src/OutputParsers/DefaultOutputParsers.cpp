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
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <OutputParserRegistry.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::DefaultOutputParser
{

template <typename T>
T parseNumeric(
    const T value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = std::to_string(value);
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

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

template <typename T>
uint64_t parseFloat(
    const T value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = formatFloat(value);
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

template <bool Quoted>
uint64_t parseVarsized(
    const int8_t* valueAddress,
    const uint64_t valueSize,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string stringFormattedValue{reinterpret_cast<const char*>(valueAddress), valueSize};
    if constexpr (Quoted)
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
    const nautilus::val<int8_t*>& startingAddress) const
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
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return nautilus::invoke(
        DefaultOutputParser::parseFloat<float>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultF64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return nautilus::invoke(
        DefaultOutputParser::parseFloat<double>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT8OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseNumeric<int8_t>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseNumeric<int16_t>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseNumeric<int32_t>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int64_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseNumeric<int64_t>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultBOOLOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
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
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint8_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseNumeric<uint8_t>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint16_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseNumeric<uint16_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint32_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseNumeric<uint32_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint64_t>>();
    return nautilus::invoke(
        DefaultOutputParser::parseNumeric<uint64_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultVARSIZEDOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<VariableSizedData>();
    return nautilus::invoke(
        DefaultOutputParser::parseVarsized<false>,
        castedVal.getContent(),
        castedVal.getSize(),
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> QuotedVARSIZEDOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<VariableSizedData>();
    return nautilus::invoke(
        DefaultOutputParser::parseVarsized<true>,
        castedVal.getContent(),
        castedVal.getSize(),
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
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
}
