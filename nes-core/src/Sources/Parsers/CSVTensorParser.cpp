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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Sources/Parsers/CSVTensorParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <fstream>
#include <string>
#include <utility>

namespace NES {

CSVTensorParser::CSVTensorParser(
                                 std::vector<NES::PhysicalTypePtr> physicalTypes,
                                 std::string delimiter)
    : physicalTypes(std::move(physicalTypes)),
      delimiter(std::move(delimiter)) {}

bool CSVTensorParser::writeInputTupleToTensor(std::ifstream input,
                                              uint64_t tupleCount,
                                              Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                              const SchemaPtr& schema,
                                              uint64_t generatedTuplesThisPass) {
    NES_TRACE("CSVTensorParser::parseCSVLines: Current TupleCount: " << tupleCount);

    auto tensorDataType = schema->fields[0]->getDataType();
    auto tensor = tensorDataType->as<TensorType>(tensorDataType);
    auto physicalTensor = DefaultPhysicalTypeFactory().getPhysicalType(tensor->component);
    auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalTensor);

    std::string line;

    while (tupleCount < generatedTuplesThisPass) {
        if (tensor->shape.size() == 1) {
            std::getline(input, line);
            switch (basicPhysicalType->nativeType) {
                case NES::BasicPhysicalType::INT_8: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    int8_t* begin = tupleBuffer[tupleCount][0].read<int8_t*>();
                    begin = reinterpret_cast<int8_t*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::INT_16: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    int16_t* begin = tupleBuffer[tupleCount][0].read<int16_t*>();
                    begin = reinterpret_cast<int16_t*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::INT_32: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    int32_t* begin = tupleBuffer[tupleCount][0].read<int32_t*>();
                    begin = reinterpret_cast<int32_t*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::INT_64: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    int64_t* begin = tupleBuffer[tupleCount][0].read<int64_t*>();
                    begin = reinterpret_cast<int64_t*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::UINT_8: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    uint8_t* begin = tupleBuffer[tupleCount][0].read<uint8_t*>();
                    begin = reinterpret_cast<uint8_t*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::UINT_16: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    uint16_t* begin = tupleBuffer[tupleCount][0].read<uint16_t*>();
                    begin = reinterpret_cast<uint16_t*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::UINT_32: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    uint32_t* begin = tupleBuffer[tupleCount][0].read<uint32_t*>();
                    begin = reinterpret_cast<uint32_t*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::UINT_64: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    uint64_t* begin = tupleBuffer[tupleCount][0].read<uint64_t*>();
                    begin = reinterpret_cast<uint64_t*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::FLOAT: {
                    std::vector<float> vals = NES::Util::splitWithStringDelimiter<float>(line, delimiter);
                    float* begin = tupleBuffer[tupleCount][0].read<float*>();
                    begin = &vals[0];
                    break;
                }
                case NES::BasicPhysicalType::DOUBLE: {
                    std::vector<int> vals = NES::Util::splitWithStringDelimiter<int>(line, delimiter);
                    double* begin = tupleBuffer[tupleCount][0].read<double*>();
                    begin = reinterpret_cast<double*>(vals[0]);
                    break;
                }
                case NES::BasicPhysicalType::CHAR: {
                    //verify that only a single char was transmitted
                    NES_ERROR("Cannot read CHAR into Tensor");
                    break;
                }
                case NES::BasicPhysicalType::BOOLEAN: {
                    //verify that a valid bool was transmitted (valid{true,false,0,1})
                    NES_ERROR("Cannot read BOOLEAN into Tensor");
                    break;
                }
                    std::vector<std::string> values = NES::Util::splitWithStringDelimiter<std::string>(line, delimiter);
            }
        }
        tupleCount++;
    }
    return true;
}

}// namespace NES