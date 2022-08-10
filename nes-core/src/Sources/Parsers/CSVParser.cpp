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

#include <API/Schema.hpp>
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>

using namespace std::string_literals;
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

    if (values.size() != schema->getSize() && !schema->containsTensor()) {
        throw Exceptions::RuntimeException("CSVParser: The input line does not contain the right number of delited fiels."s
                                           + " Fields in schema: " + std::to_string(schema->getSize())
                                           + " Fields in line: " + std::to_string(values.size())
                                           + " Schema: " + schema->toString() + " Line: " + csvInputLine);
    }
    // iterate over fields of schema and cast string values to correct type
    size_t columnIter = 0;
    for (uint64_t j = 0; j < numberOfSchemaFields; j++) {
        auto field = physicalTypes[j];
        if (!field->isTensorType()) {
            writeFieldValueToTupleBuffer(values[columnIter], j, tupleBuffer, false, schema, tupleCount);
            columnIter++;
        } else {
            auto tensorDataType = schema->fields[j]->getDataType();
            auto tensor = tensorDataType->as<TensorType>(tensorDataType);
            NES_TRACE("CSVParser::parseCSVLine: tensor size: "<< tensor->totalSize);
            NES_TRACE("CSVParser::parseCSVLine: current columIter: "<< columnIter);
            NES_TRACE("CSVParser::parseCSVLine: input vector size: "<< values.size());
            std::vector<std::string> tensorValues;
            try {
                tensorValues = {values.begin() + columnIter, values.begin() + columnIter + tensor->totalSize};
            } catch (std::exception& exception) {
                NES_ERROR("CSVParser::parseCSVLine: cannot create tensor from input data. Reason: " << exception.what());
            }
            writeFieldValuesToTupleBuffer(tensorValues, j, tupleBuffer, tensor, tupleCount);
            columnIter += tensor->totalSize;
        }
    }
    return true;
}
}// namespace NES