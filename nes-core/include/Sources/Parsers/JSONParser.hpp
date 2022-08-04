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

#ifndef NES_INCLUDE_SOURCES_PARSERS_JSONPARSER_HPP_
#define NES_INCLUDE_SOURCES_PARSERS_JSONPARSER_HPP_

#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <simdjson.h>

namespace NES {
class JSONParser {

  public:
    JSONParser(std::vector<PhysicalTypePtr> physicalTypes);

    void writeFieldValueToTupleBuffer(SchemaPtr& schema,
                                      uint64_t tupleIndex,
                                      simdjson::simdjson_result<simdjson::dom::element> element,
                                      Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer);

  private:
    std::vector<std::string> schemaKeys;
    std::vector<NES::PhysicalTypePtr> physicalTypes;
};
}// namespace NES
#endif// NES_INCLUDE_SOURCES_PARSERS_JSONPARSER_HPP_
