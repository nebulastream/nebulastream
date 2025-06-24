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

#include <cstdint>
#include <cstring>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::InputFormatters::RawValueParser
{

ParseFunctionSignature getQuotedStringParseFunction()
{
    return [](const std::string_view)
    {
        // INVARIANT(inputString.length() >= 2, "Input string must be at least 2 characters long.");
        // const auto inputStringWithoutQuotes = inputString.substr(1, inputString.length() - 2);
        // const auto valueLength = inputStringWithoutQuotes.length();
        // auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
        // if (not childBuffer.has_value())
        // {
        //     throw CannotAllocateBuffer("Could not store string, because we cannot allocate a child buffer.");
        // }
        //
        // auto& childBufferVal = childBuffer.value();
        // *childBufferVal.getBuffer<uint32_t>() = valueLength;
        // std::memcpy(
        //     childBufferVal.getBuffer<char>() + sizeof(uint32_t), ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        //     inputStringWithoutQuotes.data(),
        //     valueLength);
        // const auto indexToChildBuffer = tupleBufferFormatted.storeChildBuffer(childBufferVal);
        // auto* childBufferIndexPointer = reinterpret_cast<uint32_t*>( ///NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        //     tupleBufferFormatted.getBuffer() + writeOffsetInBytes); ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        // *childBufferIndexPointer = indexToChildBuffer;
    };
}


ParseFunctionSignature getBasicStringParseFunction()
{
    return [](const std::string_view)
    {
        // const auto valueLength = inputString.length();
        // auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
        // if (not childBuffer.has_value())
        // {
        //     throw CannotAllocateBuffer("Could not store string, because we cannot allocate a child buffer.");
        // }
        //
        // auto& childBufferVal = childBuffer.value();
        // *childBufferVal.getBuffer<uint32_t>() = valueLength;
        // std::memcpy(
        //     childBufferVal.getBuffer<char>() + sizeof(uint32_t), ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        //     inputString.data(),
        //     valueLength);
        // const auto indexToChildBuffer = tupleBufferFormatted.storeChildBuffer(childBufferVal);
        // auto* childBufferIndexPointer = reinterpret_cast<uint32_t*>( ///NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        //     tupleBufferFormatted.getBuffer() + writeOffsetInBytes); ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        // *childBufferIndexPointer = indexToChildBuffer;
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
        case DataType::Type::VARSIZED:
            return getBasicStringParseFunction();
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
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
