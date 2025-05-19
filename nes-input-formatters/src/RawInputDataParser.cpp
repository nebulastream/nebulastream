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

#include <cstdint>
#include <cstring>
#include <string_view>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <RawInputDataParser.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::InputFormatters::RawInputDataParser
{

template <bool Nullable>
struct BasicStringParser
{
    Sources::ParserConfig config;

    void operator()(
        const std::string_view value,
        int8_t* writeAddress,
        Memory::AbstractBufferProvider& bufferProvider,
        const Memory::TupleBuffer& bufferFormatted) const
    {
        if constexpr (Nullable)
        {
            if (value == config.nullRepresentation)
            {
                *(writeAddress + sizeof(uint32_t)) = 1;
                return;
            }
            *(writeAddress + sizeof(uint32_t)) = 0;
        }

        const auto valueLength = value.length();
        auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
        if (not childBuffer.has_value())
        {
            throw CannotAllocateBuffer("Could not store string, because we cannot allocate a child buffer.");
        }

        auto& childBufferVal = childBuffer.value();
        *childBufferVal.getBuffer<uint32_t>() = valueLength;
        std::memcpy(childBufferVal.getBuffer<char>() + sizeof(uint32_t), value.data(), valueLength);
        const auto indexToChildBuffer = bufferFormatted.storeChildBuffer(childBufferVal);
        auto* childBufferIndexPointer = reinterpret_cast<uint32_t*>(writeAddress);
        *childBufferIndexPointer = indexToChildBuffer;
    }
};

ParseFunctionSignature getBasicStringParseFunction(const Sources::ParserConfig& parserConfig, const bool nullable)
{
    if (nullable)
    {
        return BasicStringParser<true>{parserConfig};
    }
    return BasicStringParser<false>{parserConfig};
}

namespace
{
template <typename T>
ParseFunctionSignature dispatchNullable(const Sources::ParserConfig& config, const bool nullable)
{
    if (nullable)
    {
        return parseFieldString<T, true>(config);
    }
    return parseFieldString<T, false>(config);
}
}

ParseFunctionSignature
getBasicTypeParseFunction(const BasicPhysicalType& basicPhysicalType, const Sources::ParserConfig& config, const bool nullable)
{
    switch (basicPhysicalType.nativeType)
    {
        case BasicPhysicalType::NativeType::INT_8:
            return dispatchNullable<int8_t>(config, nullable);
        case BasicPhysicalType::NativeType::INT_16:
            return dispatchNullable<int16_t>(config, nullable);
        case BasicPhysicalType::NativeType::INT_32:
            return dispatchNullable<int32_t>(config, nullable);
        case BasicPhysicalType::NativeType::INT_64:
            return dispatchNullable<int64_t>(config, nullable);
        case BasicPhysicalType::NativeType::UINT_8:
            return dispatchNullable<uint8_t>(config, nullable);
        case BasicPhysicalType::NativeType::UINT_16:
            return dispatchNullable<uint16_t>(config, nullable);
        case BasicPhysicalType::NativeType::UINT_32:
            return dispatchNullable<uint32_t>(config, nullable);
        case BasicPhysicalType::NativeType::UINT_64:
            return dispatchNullable<uint64_t>(config, nullable);
        case BasicPhysicalType::NativeType::FLOAT:
            return dispatchNullable<float>(config, nullable);
        case BasicPhysicalType::NativeType::DOUBLE:
            return dispatchNullable<double>(config, nullable);
        case BasicPhysicalType::NativeType::CHAR:
            return dispatchNullable<char>(config, nullable);
        case BasicPhysicalType::NativeType::BOOLEAN:
            return dispatchNullable<bool>(config, nullable);
        case BasicPhysicalType::NativeType::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    throw NotImplemented("Unhandled BasicPhysicalType."); // optional fallback
}
}
