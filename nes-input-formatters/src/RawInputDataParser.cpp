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
#include <string_view>
#include <vector>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <RawInputDataParser.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::InputFormatters::RawInputDataParser
{

void addBasicStringParseFunction(std::vector<InputFormatterTask::ParseFunctionSignature>& fieldParseFunctions)
{
    fieldParseFunctions.emplace_back(
        [](std::string_view inputString,
           int8_t* fieldPointer,
           Memory::AbstractBufferProvider& bufferProvider,
           const NES::Memory::TupleBuffer& tupleBufferFormatted)
        {
            const auto valueLength = inputString.length();
            auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
            if(not childBuffer.has_value())
            {
                throw CannotAllocateBuffer("Could not store string, because we cannot allocate a child buffer.");
            }

            auto& childBufferVal = childBuffer.value();
            *childBufferVal.getBuffer<uint32_t>() = valueLength;
            std::memcpy(childBufferVal.getBuffer<char>() + sizeof(uint32_t), inputString.data(), valueLength);
            const auto indexToChildBuffer = tupleBufferFormatted.storeChildBuffer(childBufferVal);
            auto* childBufferIndexPointer = reinterpret_cast<uint32_t*>(fieldPointer);
            *childBufferIndexPointer = indexToChildBuffer;
        });
}

void addBasicTypeParseFunction(
    const BasicPhysicalType& basicPhysicalType, std::vector<InputFormatterTask::ParseFunctionSignature>& fieldParseFunctions)
{
    switch (basicPhysicalType.nativeType)
    {
        case NES::BasicPhysicalType::NativeType::INT_8: {
            fieldParseFunctions.emplace_back(parseFieldString<int8_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::INT_16: {
            fieldParseFunctions.emplace_back(parseFieldString<int16_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::INT_32: {
            fieldParseFunctions.emplace_back(parseFieldString<int32_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::INT_64: {
            fieldParseFunctions.emplace_back(parseFieldString<int64_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UINT_8: {
            fieldParseFunctions.emplace_back(parseFieldString<uint8_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UINT_16: {
            fieldParseFunctions.emplace_back(parseFieldString<uint16_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UINT_32: {
            fieldParseFunctions.emplace_back(parseFieldString<uint32_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UINT_64: {
            fieldParseFunctions.emplace_back(parseFieldString<uint64_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::FLOAT: {
            fieldParseFunctions.emplace_back(parseFieldString<float>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::DOUBLE: {
            fieldParseFunctions.emplace_back(parseFieldString<double>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::CHAR: {
            fieldParseFunctions.emplace_back(parseFieldString<char>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::BOOLEAN: {
            fieldParseFunctions.emplace_back(parseFieldString<bool>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UNDEFINED:
            NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
    }
}
}
