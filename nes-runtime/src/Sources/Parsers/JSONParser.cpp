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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <absl/numeric/bits.h>
#include <absl/types/span.h>
#include <nlohmann/json.hpp>
#include <string>
#include <utility>

namespace NES {

NESParser::NESParser() : Parser({}) {}
bool NESParser::writeInputTupleToTupleBuffer(std::string_view nesBuffer,
                                             uint64_t,
                                             Runtime::MemoryLayouts::DynamicTupleBuffer& dynamicBuffer,
                                             const SchemaPtr& schema,
                                             const Runtime::BufferManagerPtr&) {
    NES_ASSERT(nesBuffer.size() % schema->getSchemaSizeInBytes() == 0, "Buffer size does not match expected buffer size");
    NES_ASSERT(nesBuffer.size() <= dynamicBuffer.getBuffer().getBufferSize(), "Buffer size missmatch");
    std::memcpy(dynamicBuffer.getBuffer().getBuffer(), nesBuffer.data(), nesBuffer.size());
    dynamicBuffer.getBuffer().setNumberOfTuples(nesBuffer.size() / schema->getSchemaSizeInBytes());
    return true;
}

JSONParser::JSONParser(uint64_t numberOfSchemaFields,
                       std::vector<std::string> schemaKeys,
                       std::vector<NES::PhysicalTypePtr> physicalTypes)
    : Parser(physicalTypes), numberOfSchemaFields(numberOfSchemaFields), schemaKeys(std::move(schemaKeys)),
      physicalTypes(std::move(physicalTypes)) {}

bool JSONParser::writeInputTupleToTupleBuffer(std::string_view jsonTuple,
                                              uint64_t tupleCount,
                                              Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                              const SchemaPtr& schema,
                                              const Runtime::BufferManagerPtr& bufferManager) {
    NES_TRACE("JSONParser::writeInputTupleToTupleBuffer: Current TupleCount:  {}", tupleCount);
    std::vector<std::string> helperToken;
    // extract values as strings from JSON message - should be improved with JSON library
    nlohmann::json parsedJSONObject;
    try {
        parsedJSONObject = nlohmann::json ::parse(jsonTuple);
    } catch (std::exception e) {
        NES_THROW_RUNTIME_ERROR(
            "JSONParser::writeInputTupleToTupleBuffer: Couldn't parse json tuple. ERROR: " << strerror(errno));
    }
    // iterate over fields of schema and cast string values to correct type
    std::basic_string<char> jsonValue;
    for (uint64_t fieldIndex = 0; fieldIndex < numberOfSchemaFields; fieldIndex++) {
        auto field = physicalTypes[fieldIndex];
        try {
            //serialize() is called to get the web::json::value as a string. This is done for 2 reasons:
            // 1. to keep 'Parser.cpp' independent of cpprest (no need to deal with 'web::json::value' object)
            // 2. to have a single place for NESBasicPhysicalType conversion (could change this)
            NES_TRACE("JSONParser::writeInputTupleToTupleBuffer: Current Field:  {}", schemaKeys[fieldIndex]);
            jsonValue = parsedJSONObject[schemaKeys[fieldIndex]].dump();
            if (jsonValue == "null") {// key doesn't exist in parsedJSONObject, which is not an error itself
                return false;
            }
        } catch (nlohmann::json::exception jsonException) {
            NES_ERROR("JSONParser::writeInputTupleToTupleBuffer: Error when parsing jsonTuple: {}", jsonException.what());
            return false;
        }
        //JSON stings are send with " or '. We do not want to save these chars to our strings, though. Hence, we need to trim
        //the strings. This behavior can be improved with a JSON library in the future.
        jsonValue = NES::Util::trim(jsonValue, '"');
        jsonValue = NES::Util::trim(jsonValue, '\'');
        writeFieldValueToTupleBuffer(jsonValue, fieldIndex, tupleBuffer, schema, tupleCount, bufferManager);
    }
    return true;
}
}// namespace NES