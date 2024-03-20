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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace NES {

std::string Util::printTupleBufferAsText(Runtime::TupleBuffer& buffer) {
    std::stringstream ss;
    for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++) {
        ss << buffer.getBuffer<char>()[i];
    }
    return ss.str();
}

std::string Util::printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema) {
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto* buffer = tbuffer.getBuffer<char>();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint64_t i = 0; i < numberOfTuples; i++) {
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            auto dataType = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(dataType);
            auto fieldSize = physicalType->size();
            std::string str;
            auto indexInBuffer = buffer + offset + i * schema->getSchemaSizeInBytes();

            // handle variable-length field
            if (dataType->isText()) {
                NES_DEBUG("Util::printTupleBufferAsCSV(): trying to read the variable length TEXT field: "
                          "from the tuple buffer");

                // read the child buffer index from the tuple buffer
                auto childIdx = *reinterpret_cast<uint32_t const*>(indexInBuffer);
                str = Runtime::MemoryLayouts::readVarSizedData(tbuffer, childIdx);
            } else {
                str = physicalType->convertRawToString(indexInBuffer);
            }

            ss << str;
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

std::string Util::toCSVString(const SchemaPtr& schema) {
    std::stringstream ss;
    for (auto& f : schema->fields) {
        ss << f->toString() << ",";
    }
    ss.seekp(-1, std::ios_base::end);
    ss << std::endl;
    return ss.str();
}

bool Util::assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan,
                                            std::vector<std::map<std::string, std::any>> properties) {
    // count the number of operators in the query
    auto queryPlanIterator = PlanIterator(queryPlan);
    size_t numOperators = queryPlanIterator.snapshot().size();
    ;

    // check if we supply operator properties for all operators
    if (numOperators != properties.size()) {
        NES_ERROR("UtilityFunctions::assignPropertiesToQueryOperators: the number of properties does not match the number of "
                  "operators. The query plan is: {}",
                  queryPlan->toString());
        return false;
    }

    // prepare the query plan iterator
    auto propertyIterator = properties.begin();

    // iterate over all operators in the query
    for (auto&& node : queryPlanIterator) {
        for (auto const& [key, val] : *propertyIterator) {
            // add the current property to the current operator
            node->as<LogicalOperator>()->addProperty(key, val);
        }
        ++propertyIterator;
    }

    return true;
}

std::vector<Runtime::TupleBuffer> Util::createBuffersFromCSVFile(const std::string& csvFile,
                                                                 const SchemaPtr& schema,
                                                                 Runtime::BufferManagerPtr bufferManager,
                                                                 const std::string& timeStampFieldName,
                                                                 uint64_t lastTimeStamp) {
    std::vector<Runtime::TupleBuffer> recordBuffers;
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(csvFile)), "CSVFile " << csvFile << " does not exist!!!");

    // Creating everything for the csv parser
    std::ifstream file(csvFile);
    std::istream_iterator<std::string> beginIt(file);
    std::istream_iterator<std::string> endIt;
    const std::string delimiter = ",";
    auto parser = std::make_shared<CSVParser>(schema->fields.size(), getPhysicalTypes(schema), delimiter);

    // Do-while loop for checking, if we have another line to parse from the inputFile
    const auto maxTuplesPerBuffer = bufferManager->getBufferSize() / schema->getSchemaSizeInBytes();
    auto it = beginIt;
    auto tupleCount = 0UL;
    auto buffer = bufferManager->getBufferBlocking();
    do {
        std::string line = *it;
        auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
        parser->writeInputTupleToTupleBuffer(line, tupleCount, testTupleBuffer, schema, bufferManager);
        ++tupleCount;

        // If we have read enough tuples from the csv file, then stop iterating over it
        auto curTimeStamp = testTupleBuffer[tupleCount - 1][timeStampFieldName].read<uint64_t>();
        if (curTimeStamp >= lastTimeStamp) {
            break;
        }

        if (tupleCount >= maxTuplesPerBuffer) {
            buffer.setNumberOfTuples(tupleCount);
            recordBuffers.emplace_back(buffer);
            buffer = bufferManager->getBufferBlocking();
            tupleCount = 0UL;
        }
        ++it;
    } while (it != endIt);

    if (tupleCount > 0) {
        buffer.setNumberOfTuples(tupleCount);
        recordBuffers.emplace_back(buffer);
    }

    return recordBuffers;
}

std::vector<PhysicalTypePtr> Util::getPhysicalTypes(SchemaPtr schema) {
    std::vector<PhysicalTypePtr> retVector;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }
    return retVector;
}

#ifdef WRAP_READ_CALL
// If NES is build with NES_ENABLES_TESTS the linker is instructed to wrap the read function
// to keep the usual functionality __wrap_read just calls __real_read which is the real read function.
// However this allows to mock calls to read (e.g. TCPSourceTest)
extern "C" ssize_t __real_read(int fd, void* data, size_t size);
__attribute__((weak)) extern "C" ssize_t __wrap_read(int fd, void* data, size_t size) { return __real_read(fd, data, size); }
#endif
}// namespace NES