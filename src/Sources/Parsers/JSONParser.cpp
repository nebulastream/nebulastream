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

JSONParser::JSONParser(uint64_t tupleSize, uint64_t numberOfSchemaFields, std::vector<NES::PhysicalTypePtr> physicalTypes)
    : Parser(physicalTypes), tupleSize(tupleSize), numberOfSchemaFields(numberOfSchemaFields),
      physicalTypes(std::move(physicalTypes)) {}

void JSONParser::writeInputTupleToTupleBuffer(std::string jsonInput,
                                              uint64_t tupleCount,
                                              NES::Runtime::TupleBuffer& tupleBuffer) {
    NES_DEBUG("JSONParser::parseJSONMessage: Current TupleCount: " << tupleCount);
    uint64_t offset = 0;

    std::vector<std::string> helperToken;
    std::vector<std::string> values;

    // extract values as strings from MQTT message - should be improved with JSON library
    std::vector<std::string> keyValuePairs = NES::Util::splitWithStringDelimiter<std::string>(jsonInput, ",");
    for (auto& keyValuePair : keyValuePairs) {
        values.push_back(NES::Util::splitWithStringDelimiter<std::string>(keyValuePair, ":")[1]);
    }

    // iterate over fields of schema and cast string values to correct type
    for (uint64_t j = 0; j < numberOfSchemaFields; j++) {
        auto field = physicalTypes[j];
        uint64_t fieldSize = field->size();

        NES_ASSERT2_FMT(fieldSize + offset + tupleCount * tupleSize < tupleBuffer.getBufferSize(),
                        "Overflow detected: buffer size = " << tupleBuffer.getBufferSize() << " position = "
                                                            << (offset + tupleCount * tupleSize) << " field size " << fieldSize);
        writeFieldValueToTupleBuffer(values[j], j, tupleBuffer, offset + tupleCount * tupleSize, true);
        offset += fieldSize;
    }
}
}// namespace NES