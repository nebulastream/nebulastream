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
#include <ErrorHandling.hpp>
#include <RawInputDataParser.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::InputFormatters::RawInputDataParser
{

ParseFunctionSignature getBasicStringParseFunction()
{
    return [](const std::string_view inputString,
              const size_t writeOffsetInBytes,
              Memory::AbstractBufferProvider& bufferProvider,
              Memory::TupleBuffer& tupleBufferFormatted)
    {
        const auto valueLength = inputString.length();
        auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
        if (not childBuffer.has_value())
        {
            throw CannotAllocateBuffer("Could not store string, because we cannot allocate a child buffer.");
        }

        auto& childBufferVal = childBuffer.value();
        *childBufferVal.getBuffer<uint32_t>() = valueLength;
        std::memcpy(
            childBufferVal.getBuffer<char>() + sizeof(uint32_t), ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            inputString.data(),
            valueLength);
        const auto indexToChildBuffer = tupleBufferFormatted.storeChildBuffer(childBufferVal);
        auto* childBufferIndexPointer = reinterpret_cast<uint32_t*>(
            tupleBufferFormatted.getBuffer() + writeOffsetInBytes); ///NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        *childBufferIndexPointer = indexToChildBuffer;
    };
}

ParseFunctionSignature getBasicTypeParseFunction(const BasicPhysicalType& basicPhysicalType)
{
    switch (basicPhysicalType.nativeType)
    {
        case BasicPhysicalType::NativeType::INT_8: {
            return parseFieldString<int8_t>();
        }
        case BasicPhysicalType::NativeType::INT_16: {
            return parseFieldString<int16_t>();
        }
        case BasicPhysicalType::NativeType::INT_32: {
            return parseFieldString<int32_t>();
        }
        case BasicPhysicalType::NativeType::INT_64: {
            return parseFieldString<int64_t>();
        }
        case BasicPhysicalType::NativeType::UINT_8: {
            return parseFieldString<uint8_t>();
        }
        case BasicPhysicalType::NativeType::UINT_16: {
            return parseFieldString<uint16_t>();
        }
        case BasicPhysicalType::NativeType::UINT_32: {
            return parseFieldString<uint32_t>();
        }
        case BasicPhysicalType::NativeType::UINT_64: {
            return parseFieldString<uint64_t>();
        }
        case BasicPhysicalType::NativeType::FLOAT: {
            return parseFieldString<float>();
        }
        case BasicPhysicalType::NativeType::DOUBLE: {
            return parseFieldString<double>();
        }
        case BasicPhysicalType::NativeType::CHAR: {
            return parseFieldString<char>();
        }
        case BasicPhysicalType::NativeType::BOOLEAN: {
            return parseFieldString<bool>();
        }
        case BasicPhysicalType::NativeType::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    return nullptr;
}
}
