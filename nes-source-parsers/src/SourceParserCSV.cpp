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

#include <memory>
#include <string>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <SourceParsers/SourceParser.hpp>
#include <SourceParsers/SourceParserCSV.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::SourceParsers
{
using namespace std::string_literals;

SourceParserCSV::SourceParserCSV(SchemaPtr schema, std::vector<NES::PhysicalTypePtr> physicalTypes, std::string delimiter)
    : schema(std::move(schema))
    , numberOfSchemaFields(this->schema->getSize())
    , physicalTypes(std::move(physicalTypes))
    , delimiter(std::move(delimiter))
{
}

void writeBasicTypeToTupleBuffer(std::string inputString, int8_t* fieldPointer, const BasicPhysicalType& basicPhysicalType)
{
    if (inputString.empty())
    {
        throw Exceptions::RuntimeException("Input string for parsing is empty");
    }

    try
    {
        switch (basicPhysicalType.nativeType)
        {
            case NES::BasicPhysicalType::NativeType::INT_8: {
                const auto value = static_cast<int8_t>(std::stoi(inputString));
                *fieldPointer = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_16: {
                const auto value = static_cast<int16_t>(std::stol(inputString));
                auto int16Ptr = reinterpret_cast<int16_t*>(fieldPointer);
                *int16Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_32: {
                const auto value = static_cast<int32_t>(std::stol(inputString));
                auto int32Ptr = reinterpret_cast<int32_t*>(fieldPointer);
                *int32Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_64: {
                const auto value = static_cast<int64_t>(std::stoll(inputString));
                auto int64Ptr = reinterpret_cast<int64_t*>(fieldPointer);
                *int64Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_8: {
                const auto value = static_cast<uint8_t>(std::stoi(inputString));
                auto uint8Ptr = reinterpret_cast<uint8_t*>(fieldPointer);
                *uint8Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_16: {
                const auto value = static_cast<uint16_t>(std::stoul(inputString));
                auto uint16Ptr = reinterpret_cast<uint16_t*>(fieldPointer);
                *uint16Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_32: {
                auto value = static_cast<uint32_t>(std::stoul(inputString));
                auto uint32Ptr = reinterpret_cast<uint32_t*>(fieldPointer);
                *uint32Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_64: {
                auto value = static_cast<uint64_t>(std::stoull(inputString));
                auto uint64Ptr = reinterpret_cast<uint64_t*>(fieldPointer);
                *uint64Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::FLOAT: {
                NES::Util::findAndReplaceAll(inputString, ",", ".");
                auto value = static_cast<float>(std::stof(inputString));
                auto floatPtr = reinterpret_cast<float*>(fieldPointer);
                *floatPtr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::DOUBLE: {
                auto value = static_cast<double>(std::stod(inputString));
                auto doublePtr = reinterpret_cast<double*>(fieldPointer);
                *doublePtr = value;
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
                auto charPtr = reinterpret_cast<char*>(fieldPointer);
                *charPtr = value;
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
                auto boolPtr = reinterpret_cast<bool*>(fieldPointer);
                *boolPtr = value;
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

bool SourceParserCSV::writeInputTupleToTupleBuffer(
    std::string_view csvInputLine,
    uint64_t tupleCount,
    NES::Memory::TupleBuffer& tupleBuffer,
    NES::Memory::AbstractBufferProvider& bufferProvider) const
{
    NES_TRACE("Current TupleCount:  {}", tupleCount);

    std::vector<std::string> values;
    try
    {
        values = NES::Util::splitWithStringDelimiter<std::string>(csvInputLine, delimiter);
    }
    catch (std::exception e)
    {
        throw CSVParsingError(fmt::format(
            "An error occurred while splitting delimiter. ERROR: {}", strerror(errno)));
    }

    if (values.size() != schema->getSize())
    {
        throw CSVParsingError(fmt::format(
            "The input line does not contain the right number of delimited fields. Fields in schema: {}"
            " Fields in line: {}"
            " Schema: {} Line: {}",
            std::to_string(schema->getSize()),
            std::to_string(values.size()),
            schema->toString(),
            csvInputLine));
    }
    /// iterate over fields of schema and cast string values to correct type
    size_t currentOffset = tupleCount * schema->getSchemaSizeInBytes();
    for (uint64_t j = 0; j < numberOfSchemaFields; j++)
    {
        auto field = physicalTypes[j];
        std::string_view currentVal = values[j];
        NES_TRACE("Current value is:  {}", currentVal);

        const auto dataType = schema->fields[j]->getDataType();
        const auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
        int8_t* fieldPointer = tupleBuffer.getBuffer() + currentOffset;
        if (const auto basicPhysicalType = std::dynamic_pointer_cast<const BasicPhysicalType>(physicalType))
        {
            writeBasicTypeToTupleBuffer(currentVal.data(), fieldPointer, *basicPhysicalType);
        }
        else
        {
            NES_TRACE(
                "Parser::writeFieldValueToTupleBuffer(): trying to write the variable length input string: {}"
                "to tuple buffer",
                currentVal);
            const auto valueLength = currentVal.length();
            auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
            if (childBuffer.has_value())
            {
                auto& childBufferVal = childBuffer.value();
                *childBufferVal.getBuffer<uint32_t>() = valueLength;
                std::memcpy(childBufferVal.getBuffer<char>() + sizeof(uint32_t), currentVal.data(), valueLength);
                const auto index = tupleBuffer.storeChildBuffer(childBufferVal);
                auto childBufferIndexPointer = reinterpret_cast<uint32_t*>(fieldPointer);
                *childBufferIndexPointer = index;
            }
            else
            {
                NES_ERROR("Could not store string {}", currentVal);
            }
        }
        currentOffset += field->size();
    }
    return true;
}
size_t ParserCSV::getSizeOfSchemaInBytes() const
{
    return schema->getSchemaSizeInBytes();
}

}
