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

#include <RawValueParser.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

ParseFunctionSignature getQuotedStringParseFunction()
{
    return [](const std::string_view inputString,
              const size_t writeOffsetInBytes,
              AbstractBufferProvider& bufferProvider,
              TupleBuffer& tupleBufferFormatted)
    {
        INVARIANT(inputString.length() >= 2, "Input string must be at least 2 characters long.");
        const auto inputStringWithoutQuotes = inputString.substr(1, inputString.length() - 2);
        const auto variableSizedAccess = MemoryLayout::writeVarSized<MemoryLayout::PREPEND_LENGTH_AS_UINT32>(
            tupleBufferFormatted, bufferProvider, std::as_bytes(std::span{inputStringWithoutQuotes}));
        const auto combinedIdxOffset = variableSizedAccess.getCombinedIdxOffset();
        const auto parsedValueBytes = std::as_bytes(std::span{&combinedIdxOffset, 1});
        std::ranges::copy(parsedValueBytes, tupleBufferFormatted.getAvailableMemoryArea().begin() + writeOffsetInBytes);
    };
}

ParseFunctionSignature getBasicStringParseFunction()
{
    return [](const std::string_view inputString,
              const size_t writeOffsetInBytes,
              AbstractBufferProvider& bufferProvider,
              TupleBuffer& tupleBufferFormatted)
    {
        const auto variableSizedAccess = MemoryLayout::writeVarSized<MemoryLayout::PREPEND_LENGTH_AS_UINT32>(
            tupleBufferFormatted, bufferProvider, std::as_bytes(std::span{inputString}));
        const auto combinedIdxOffset = variableSizedAccess.getCombinedIdxOffset();
        const auto parsedValueBytes = std::as_bytes(std::span{&combinedIdxOffset, 1});
        std::ranges::copy(parsedValueBytes, tupleBufferFormatted.getAvailableMemoryArea().begin() + writeOffsetInBytes);
    };
}

ParseFunctionSignature getStringParseFunction(const QuotationType quotationType)
{
    switch (quotationType)
    {
        case QuotationType::NONE: {
            return getBasicStringParseFunction();
        }
        case QuotationType::DOUBLE_QUOTE: {
            return getQuotedStringParseFunction();
        }
    }
    std::unreachable();
}

ParseFunctionSignature getBasicTypeParseFunction(const DataType::Type physicalType, const QuotationType quotationType)
{
    switch (physicalType)
    {
        case DataType::Type::INT8: {
            return parseFieldString<int8_t>();
        }
        case DataType::Type::INT16: {
            return parseFieldString<int16_t>();
        }
        case DataType::Type::INT32: {
            return parseFieldString<int32_t>();
        }
        case DataType::Type::INT64: {
            return parseFieldString<int64_t>();
        }
        case DataType::Type::UINT8: {
            return parseFieldString<uint8_t>();
        }
        case DataType::Type::UINT16: {
            return parseFieldString<uint16_t>();
        }
        case DataType::Type::UINT32: {
            return parseFieldString<uint32_t>();
        }
        case DataType::Type::UINT64: {
            return parseFieldString<uint64_t>();
        }
        case DataType::Type::FLOAT32: {
            return parseFieldString<float>();
        }
        case DataType::Type::FLOAT64: {
            return parseFieldString<double>();
        }
        case DataType::Type::CHAR: {
            return (quotationType == QuotationType::NONE) ? parseFieldString<char>() : parseQuotedFieldString<char>();
        }
        case DataType::Type::BOOLEAN: {
            return parseFieldString<bool>();
        }
        case DataType::Type::VARSIZED: {
            return getBasicStringParseFunction();
        }
        case DataType::Type::VARSIZED_POINTER_REP: {
            throw NotImplemented("Cannot parse VARSIZED_POINTER_REP type.");
        }
        case DataType::Type::UNDEFINED: {
            throw NotImplemented("Cannot parse undefined type.");
        }
    }
    return nullptr;
}

ParseFunctionSignature getParseFunction(const DataType::Type physicalType, const QuotationType quotationType)
{
    if (physicalType == DataType::Type::VARSIZED)
    {
        return getStringParseFunction(quotationType);
    }
    return getBasicTypeParseFunction(physicalType, quotationType);
}
}
