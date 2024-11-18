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

#include <cstring>
#include <string>
#include <utility>
#include <API/AttributeField.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Parsers/Parser.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::Sources
{

void Parser::writeBasicTypeToTupleBuffer(
    std::string inputString,
    NES::Memory::MemoryLayouts::DynamicField& testTupleBufferDynamicField,
    const BasicPhysicalType& basicPhysicalType)
{
    if (inputString.empty())
    {
        throw Exceptions::RuntimeException("Input string for parsing is empty");
    }
    /// TODO replace with csv parsing library
    try
    {
        switch (basicPhysicalType.nativeType)
        {
            case NES::BasicPhysicalType::NativeType::INT_8: {
                auto value = static_cast<int8_t>(std::stoi(inputString));
                testTupleBufferDynamicField.write<int8_t>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_16: {
                auto value = static_cast<int16_t>(std::stol(inputString));
                testTupleBufferDynamicField.write<int16_t>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_32: {
                auto value = static_cast<int32_t>(std::stol(inputString));
                testTupleBufferDynamicField.write<int32_t>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_64: {
                auto value = static_cast<int64_t>(std::stoll(inputString));
                testTupleBufferDynamicField.write<int64_t>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_8: {
                auto value = static_cast<uint8_t>(std::stoi(inputString));
                testTupleBufferDynamicField.write<uint8_t>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_16: {
                auto value = static_cast<uint16_t>(std::stoul(inputString));
                testTupleBufferDynamicField.write<uint16_t>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_32: {
                auto value = static_cast<uint32_t>(std::stoul(inputString));
                testTupleBufferDynamicField.write<uint32_t>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_64: {
                auto value = static_cast<uint64_t>(std::stoull(inputString));
                testTupleBufferDynamicField.write<uint64_t>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::FLOAT: {
                NES::Util::findAndReplaceAll(inputString, ",", ".");
                auto value = static_cast<float>(std::stof(inputString));
                testTupleBufferDynamicField.write<float>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::DOUBLE: {
                auto value = static_cast<double>(std::stod(inputString));
                testTupleBufferDynamicField.write<double>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::CHAR: {
                ///verify that only a single char was transmitted
                if (inputString.size() > 1)
                {
                    NES_FATAL_ERROR("SourceFormatIterator::mqttMessageToNESBuffer: Received non char Value for CHAR Field {}", inputString);
                    throw std::invalid_argument("Value " + inputString + " is not a char");
                }
                char value = inputString.at(0);
                testTupleBufferDynamicField.write<char>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                ///verify that a valid bool was transmitted (valid{true,false,0,1})
                bool value = !strcasecmp(inputString.c_str(), "true") || !strcasecmp(inputString.c_str(), "1");
                if (!value)
                {
                    if (strcasecmp(inputString.c_str(), "false") && strcasecmp(inputString.c_str(), "0"))
                    {
                        NES_FATAL_ERROR(
                            "Parser::writeFieldValueToTupleBuffer: Received non boolean value for BOOLEAN field: {}", inputString.c_str());
                        throw std::invalid_argument("Value " + inputString + " is not a boolean");
                    }
                }
                testTupleBufferDynamicField.write<bool>(value);
                break;
            }
            case NES::BasicPhysicalType::NativeType::UNDEFINED:
                NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
        }
    }
    catch (const std::exception& e)
    {
        NES_ERROR("Failed to convert inputString to desired NES data type. Error: {} for inputString {}", e.what(), inputString);
    }
}

}
