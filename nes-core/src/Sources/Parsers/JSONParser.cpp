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

#include "API/AttributeField.hpp"
#include "Common/DataTypes/DataType.hpp"
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <simdjson.h>
#include <string>
#include <utility>

namespace NES {

JSONParser::JSONParser(uint64_t numberOfSchemaFields,
                       std::vector<std::string> schemaKeys,
                       std::vector<NES::PhysicalTypePtr> physicalTypes)
    : Parser(physicalTypes), numberOfSchemaFields(numberOfSchemaFields), schemaKeys(std::move(schemaKeys)),
      physicalTypes(std::move(physicalTypes)) {}

JSONParser::JSONParser(std::vector<PhysicalTypePtr> physical_types) : Parser(physical_types){};

bool JSONParser::writeInputTupleToTupleBuffer(const std::string& jsonTuple,
                                              uint64_t tupleCount,
                                              Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                              const SchemaPtr& schema) {
    NES_TRACE("JSONParser::parseJSONMessage: Current TupleCount: " << tupleCount);

    std::vector<std::string> helperToken;

    // extract values as strings from MQTT message - should be improved with JSON library
    auto parsedJSONObject = web::json::value::parse(jsonTuple);

    // iterate over fields of schema and cast string values to correct type
    std::basic_string<char> jsonValue;
    for (uint64_t fieldIndex = 0; fieldIndex < numberOfSchemaFields; fieldIndex++) {
        auto field = physicalTypes[fieldIndex];
        try {
            //serialize() is called to get the web::json::value as a string. This is done for 2 reasons:
            // 1. to keep 'Parser.cpp' independent of cpprest (no need to deal with 'web::json::value' object)
            // 2. to have a single place for NESBasicPhysicalType conversion (could change this)
            jsonValue = parsedJSONObject.at(schemaKeys[fieldIndex]).serialize();
        } catch (web::json::json_exception& jsonException) {
            NES_ERROR("JSONParser::writeInputTupleToTupleBuffer: Error when parsing jsonTuple: " << jsonException.what());
            return false;
        }

        Parser::writeFieldValueToTupleBuffer(jsonValue, fieldIndex, tupleBuffer, true, schema, tupleCount);
    }
    return true;
}

void JSONParser::writeFieldValueToTupleBuffer(uint64_t tupleIndex,
                                              uint64_t fieldIndex,
                                              const SchemaPtr& schema,
                                              simdjson::simdjson_result<simdjson::ondemand::document_reference> valueResult,
                                              Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer) {
    // TODO void
    DataTypePtr dataType = schema->fields[fieldIndex]->getDataType();
    std::string jsonKey = schema->fields[fieldIndex]->getName();
    PhysicalTypePtr physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
    if (physicalType->isBasicType()) {
        BasicPhysicalTypePtr basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
        BasicPhysicalType::NativeType nativeType = basicPhysicalType->nativeType;
        switch (nativeType) {
            case BasicPhysicalType::UINT_8: break;
            case BasicPhysicalType::UINT_16: break;
            case BasicPhysicalType::UINT_32: break;
            case BasicPhysicalType::UINT_64: {
                uint64_t value = valueResult[jsonKey];
                tupleBuffer[tupleIndex][fieldIndex].write<uint64_t>(value);
                break;
            }
            case BasicPhysicalType::INT_8: break;
            case BasicPhysicalType::INT_16: {
                int64_t value64 = valueResult[jsonKey];
                int16_t value16 = static_cast<int16_t>(value64);// TODO safe?
                tupleBuffer[tupleIndex][fieldIndex].write<int16_t>(value16);
                break;
            }
            case BasicPhysicalType::INT_32: {
                int64_t value64 = valueResult[jsonKey];
                int32_t value32 = static_cast<int32_t>(value64);// TODO safe?
                tupleBuffer[tupleIndex][fieldIndex].write<int32_t>(value32);
                break;
            }
            case BasicPhysicalType::INT_64: {
                int64_t value = valueResult[jsonKey];
                tupleBuffer[tupleIndex][fieldIndex].write<int64_t>(value);
                break;
            }
            case BasicPhysicalType::FLOAT: break;
            case BasicPhysicalType::DOUBLE: {
                double value = valueResult[jsonKey];
                tupleBuffer[tupleIndex][fieldIndex].write<double>(value);
                break;
            }
            case BasicPhysicalType::CHAR: {
                std::string_view value = valueResult[jsonKey];
                std::string str = {value.begin(), value.end()};
                char c = str.at(0);
                tupleBuffer[tupleIndex][fieldIndex].write<char>(c);
                break;
            }
            case BasicPhysicalType::BOOLEAN: {
                bool value = valueResult[jsonKey];
                tupleBuffer[tupleIndex][fieldIndex].write<bool>(value);
                break;
            }
        }
    } else {
        if (dataType->isCharArray()) {
            std::string_view value = valueResult[jsonKey];
            std::string str = {value.begin(), value.end()};
            const char* c_str = str.c_str();
            char* valueRead = tupleBuffer[tupleIndex][fieldIndex].read<char*>();
            strcpy(valueRead, c_str);
        } else if (dataType->isUndefined()) {
            NES_ERROR("Invalid data type")
            throw std::invalid_argument("Data type is undefined");
        } else {
            NES_ERROR("Invalid DataType: " << dataType);
            throw std::invalid_argument("Invalid DataType: " + dataType->toString());
        }
    }
}

}// namespace NES