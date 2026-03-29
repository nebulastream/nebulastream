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

#include <JSONOutputParsers.hpp>

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

namespace NES::JSONOutputParser
{

std::string escapeAsJsonString(std::string_view input)
{
    std::string out;
    out.reserve(input.size() + 2);
    out.push_back('"');
    for (const char raw : input)
    {
        const auto byte = static_cast<unsigned char>(raw);
        switch (raw)
        {
            case '"':
                out.append("\\\"");
                break;
            case '\\':
                out.append("\\\\");
                break;
            case '\b':
                out.append("\\b");
                break;
            case '\f':
                out.append("\\f");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                if (byte < 0x20)
                {
                    fmt::format_to(std::back_inserter(out), "\\u{:04x}", byte);
                }
                else
                {
                    out.push_back(raw);
                }
                break;
        }
    }
    out.push_back('"');
    return out;
}

uint64_t parseChar(
    const char value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = escapeAsJsonString(std::string(1, value));
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

uint64_t parseVarsized(
    const int8_t* valueAddress,
    const uint64_t valueSize,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = escapeAsJsonString(std::string(reinterpret_cast<const char*>(valueAddress), valueSize));
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

namespace NES
{
nautilus::val<uint64_t> JSONCHAROutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<char>>();
    return nautilus::invoke(
        JSONOutputParser::parseChar, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> JSONVARSIZEDOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<VariableSizedData>();
    return nautilus::invoke(
        JSONOutputParser::parseVarsized,
        castedVal.getContent(),
        castedVal.getSize(),
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterJSONCHAROutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<JSONCHAROutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterJSONVARSIZEDOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<JSONVARSIZEDOutputParser>();
}
}
