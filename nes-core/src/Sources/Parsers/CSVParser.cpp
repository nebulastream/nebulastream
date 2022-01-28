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
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>
#include <utility>

namespace NES {

CSVParser::CSVParser(uint64_t numberOfSchemaFields, std::vector<NES::PhysicalTypePtr> physicalTypes, std::string delimiter)
    : Parser(physicalTypes), numberOfSchemaFields(numberOfSchemaFields), physicalTypes(std::move(physicalTypes)),
      delimiter(std::move(delimiter)) {}

bool CSVParser::writeInputTupleToTupleBuffer(const std::string& csvInputLine,
                                             uint64_t tupleCount,
                                             Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                             const SchemaPtr& schema) {
    NES_TRACE("CSVParser::parseCSVLine: Current TupleCount: " << tupleCount);

    std::vector<std::string> values = NES::Util::splitWithStringDelimiter<std::string>(csvInputLine, delimiter);

    // iterate over fields of schema and cast string values to correct type
    for (uint64_t j = 0; j < numberOfSchemaFields; j++) {
        auto field = physicalTypes[j];
        writeFieldValueToTupleBuffer(values[j], j, tupleBuffer, false, schema, tupleCount);
    }
    return true;
}
}// namespace NES