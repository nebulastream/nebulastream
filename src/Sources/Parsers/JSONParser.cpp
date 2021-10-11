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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>
#include <utility>

namespace NES {

JSONParser::JSONParser(uint64_t tupleSize, uint64_t numberOfSchemaFields, std::vector<std::string> schemaKeys, std::vector<NES::PhysicalTypePtr> physicalTypes)
: Parser(physicalTypes), tupleSize(tupleSize), numberOfSchemaFields(numberOfSchemaFields), schemaKeys(std::move(schemaKeys)),
      physicalTypes(std::move(physicalTypes)) {}

bool JSONParser::writeInputTupleToTupleBuffer(std::string jsonTuple,
                                              uint64_t tupleCount,
                                              NES::Runtime::TupleBuffer& tupleBuffer) {
    NES_DEBUG("JSONParser::parseJSONMessage: Current TupleCount: " << tupleCount);
    uint64_t offset = 0;

    std::vector<std::string> helperToken;

    // extract values as strings from MQTT message - should be improved with JSON library
    auto parsedJSONObject = web::json::value::parse(jsonTuple);

    // iterate over fields of schema and cast string values to correct type
    std::basic_string<char> jsonValue;
    #pragma unroll
    for (uint64_t fieldIndex = 0; fieldIndex < numberOfSchemaFields; fieldIndex++) {
        auto field = physicalTypes[fieldIndex];
        uint64_t fieldSize = field->size();
        try {
            NES_ASSERT2_FMT(fieldSize + offset + tupleCount * tupleSize < tupleBuffer.getBufferSize(),
                            "Overflow detected: buffer size = " << tupleBuffer.getBufferSize() << " position = "
                            << (offset + tupleCount * tupleSize) << " field size " << fieldSize);
            //serialize() is called to get the web::json::value as a string. This is done for 2 reasons:
            // 1. to keep 'Parser.cpp' independent of cpprest (no need to deal with 'web::json::value' object)
            // 2. to have a single place for NESBasicPhysicalType conversion (could change this)
            jsonValue = parsedJSONObject.at(schemaKeys[fieldIndex]).serialize();
        } catch (web::json::json_exception &exception) {
            NES_ERROR("JSONParser::writeInputTupleToTupleBuffer: Error when parsing jsonTuple: " << exception.what());
            return false;
        }
        writeFieldValueToTupleBuffer(jsonValue, fieldIndex, tupleBuffer, offset + tupleCount * tupleSize, true);
        offset += fieldSize;
    }
    return true;
}
}// namespace NES