/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Sources/Parsers/Parser.hpp>
#include <string>
#include <Util/Logger.hpp>
#include <utility>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <cstring>


Parser::Parser(std::vector<NES::PhysicalTypePtr> physicalTypes): physicalTypes(std::move(physicalTypes)){}

void Parser::writeFieldValueToTupleBuffer(std::basic_string<char> inputString, uint64_t schemaFieldIndex,
                                               NES::Runtime::TupleBuffer& tupleBuffer, uint64_t offset, bool json) {
    NES_ASSERT2_FMT(!inputString.empty(), "Field cannot be empty if basic type");
    uint64_t fieldSize = physicalTypes[schemaFieldIndex]->size();
    if (physicalTypes[schemaFieldIndex]->isBasicType()) {
        auto basicPhysicalField = std::dynamic_pointer_cast<NES::BasicPhysicalType>(physicalTypes[schemaFieldIndex]);
        switch (basicPhysicalField->nativeType) {
            case NES::BasicPhysicalType::INT_8:
            {
                auto value = static_cast<int8_t>(std::stoi(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::INT_16:
            {
                auto value = static_cast<int16_t>(std::stol(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::INT_32:
            {
                auto value = static_cast<int32_t>(std::stol(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::INT_64:
            {
                auto value = static_cast<int64_t>(std::stoll(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::UINT_8:
            {
                auto value = static_cast<uint8_t>(std::stoi(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::UINT_16:
            {
                auto value = static_cast<uint16_t>(std::stoul(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::UINT_32:
            {
                auto value = static_cast<uint32_t>(std::stoul(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::UINT_64:
            {
                auto value = static_cast<uint64_t>(std::stoull(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::FLOAT:
            {
                auto value = static_cast<float>(std::stof(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::DOUBLE:
            {
                auto value = static_cast<double>(std::stod(inputString));
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::CHAR:
            {
                //verify that only a single char was transmitted
                if(inputString.size() > 1) {
                    NES_FATAL_ERROR("SourceFormatIterator::mqttMessageToNESBuffer: Received non char Value for CHAR Field " << inputString.c_str());
                    throw std::invalid_argument("Value " + inputString + " is not a char");
                }
                char value = inputString.at(0);
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
            case NES::BasicPhysicalType::BOOLEAN:
            {
                //verify that a valid bool was transmitted (valid{true,false,0,1})
                bool value = !strcasecmp(inputString.c_str(), "true") || !strcasecmp(inputString.c_str(), "1");
                if(!value) {
                    if(strcasecmp(inputString.c_str(), "false") && strcasecmp(inputString.c_str(), "0")) {
                        NES_FATAL_ERROR("SourceFormatIterator::mqttMessageToNESBuffer: Received non boolean value for BOOLEAN field: " << inputString.c_str());
                        throw std::invalid_argument("Value " + inputString + " is not a boolean");
                    }
                }
                memcpy(tupleBuffer.getBuffer<char>() + offset, &value, fieldSize);
                break;
            }
        }
    } else { // char array(string) case
        std::string value;
        // remove quotation marks from start and end of value (ASSUMES QUOTATIONMARKS AROUND STRINGS)
        // improve behavior with json library
        value = (json) ? inputString.substr(1, value.size()-2) : inputString.c_str();
        memcpy(tupleBuffer.getBuffer<char>() + offset, value.c_str(), fieldSize);
}
}
Parser::~Parser() {
    NES_DEBUG("Parser::Parsertroyed Parser");
}
