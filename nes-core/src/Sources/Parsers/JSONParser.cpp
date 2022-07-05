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

bool JSONParser::writeFieldValueToTupleBuffer(uint64_t tupleIndex,
                                              uint64_t fieldIndex,
                                              DataTypePtr dataType,
                                              std::string jsonKey,
                                              simdjson::simdjson_result<simdjson::ondemand::document_reference> valueResult,
                                              Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer) {
    if (dataType->isBoolean()) {
        bool value = valueResult[jsonKey];
        tupleBuffer[tupleIndex][fieldIndex].write<bool>(value);
    } else if (dataType->isChar()) {
        std::string_view value = valueResult[jsonKey];
        std::string str = {value.begin(), value.end()};
        char c = str.at(0);
        tupleBuffer[tupleIndex][fieldIndex].write<char>(c);
    } else if (dataType->isCharArray()) {
        // TODO one cast to rule them all?
        std::string_view value = valueResult[jsonKey];
        const char* p = std::string(value).c_str();
        std::cout << "char*: " << p << std::endl;
        size_t len = strlen(p); // TODO must be as specified in schema
        std::cout << "len: " << len << std::endl;
        char charArray[len];
        strcpy(charArray, p);
        std::cout << "char[]: " << charArray << std::endl;
        tupleBuffer[tupleIndex][fieldIndex].write<const char*>(charArray);// TODO @Dimitrios
        //tupleBuffer[tupleIndex][fieldIndex].write<char []>(charArray);
        //NES_ERROR("Invalid DataType: " << dataType);
        //throw std::logic_error("DataType string/char array not supported");
    } else if (dataType->isFloat()) {// TODO safe?
        double value = valueResult[jsonKey];
        tupleBuffer[tupleIndex][fieldIndex].write<double>(value);
    } else if (dataType->isInteger()) {
        int64_t value = valueResult[jsonKey];
        tupleBuffer[tupleIndex][fieldIndex].write<int64_t>(value);
    } else if (dataType->isNumeric()) {
        simdjson::ondemand::number number = valueResult.get_number();
        simdjson::ondemand::number_type numberType = number.get_number_type();
        if (numberType == simdjson::ondemand::number_type::signed_integer) {
            int64_t value = valueResult[jsonKey];
            tupleBuffer[tupleIndex][fieldIndex].write<int64_t>(value);
        } else if (numberType == simdjson::ondemand::number_type::unsigned_integer) {
            uint64_t value = valueResult[jsonKey];
            tupleBuffer[tupleIndex][fieldIndex].write<uint64_t>(value);
        } else if (numberType == simdjson::ondemand::number_type::floating_point_number) {
            double value = valueResult[jsonKey];
            tupleBuffer[tupleIndex][fieldIndex].write<double>(value);
        }
    } else if (dataType->isUndefined()) {
        NES_ERROR("Invalid data type")
        throw std::invalid_argument("Data type is undefined");
    } else {
        NES_ERROR("Invalid DataType: " << dataType);
        throw std::invalid_argument("Invalid DataType: " + dataType->toString());
    }
    return true;
}

}// namespace NES