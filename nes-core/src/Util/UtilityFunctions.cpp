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
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <unistd.h>

namespace NES {

uint64_t Util::numberOfUniqueValues(std::vector<uint64_t>& values) {
    std::sort(values.begin(), values.end());
    return std::unique(values.begin(), values.end()) - values.begin();
}

std::string Util::escapeJson(const std::string& str) {
    std::ostringstream o;
    for (char c : str) {
        if (c == '"' || c == '\\' || ('\x00' <= c && c <= '\x1f')) {
            o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int) c;
        } else {
            o << c;
        }
    }
    return o.str();
}

std::string Util::trim(std::string str) {
    auto not_space = [](char c) {
        return isspace(c) == 0;
    };
    // trim left
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), not_space));
    // trim right
    str.erase(find_if(str.rbegin(), str.rend(), not_space).base(), str.end());
    return str;
}

std::string Util::trim(std::string str, char trimFor) {
    // remove all trimFor characters from left and right
    str.erase(std::remove(str.begin(), str.end(), trimFor), str.end());
    return str;
}

std::string Util::printTupleBufferAsText(Runtime::TupleBuffer& buffer) {
    std::stringstream ss;
    for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++) {
        ss << buffer.getBuffer<char>()[i];
    }
    return ss.str();
}

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
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
                NES_DEBUG2("Util::printTupleBufferAsCSV(): trying to read the variable length TEXT field: "
                           "from the tuple buffer");

                // read the child buffer index from the tuple buffer
                Runtime::TupleBuffer::NestedTupleBufferKey childIdx = *reinterpret_cast<uint32_t const*>(indexInBuffer);

                // retrieve the child buffer from the tuple buffer
                auto childTupleBuffer = tbuffer.loadChildBuffer(childIdx);

                // retrieve the size of the variable-length field from the child buffer
                uint32_t sizeOfTextField = *(childTupleBuffer.getBuffer<uint32_t>());

                // build the string
                if (sizeOfTextField > 0) {
                    auto begin = childTupleBuffer.getBuffer() + sizeof(uint32_t);
                    std::string deserialized(begin, begin + sizeOfTextField);
                    str = std::move(deserialized);
                }

                else {
                    NES_WARNING2("Util::printTupleBufferAsCSV(): Variable-length field could not be read. Invalid size in the "
                                 "variable-length TEXT field. Returning an empty string.")
                }
            }

            else {
                str = physicalType->convertRawToString(indexInBuffer);
            }

            ss << str.c_str();
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

void Util::findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr) {
    // Get the first occurrence
    uint64_t pos = data.find(toSearch);
    // Repeat till end is reached
    while (pos != std::string::npos) {
        // Replace this occurrence of Sub String
        data.replace(pos, toSearch.size(), replaceStr);
        // Get the next occurrence from the current position
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

std::string Util::replaceFirst(std::string origin, const std::string& search, const std::string& replace) {
    if (origin.find(search) != std::string::npos) {
        return origin.replace(origin.find(search), search.size(), replace);
    }
    return origin;
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

bool Util::endsWith(const std::string& fullString, const std::string& ending) {
    if (fullString.length() >= ending.length()) {
        // get the start of the ending index of the full string and compare with the ending string
        return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
    }// if full string is smaller than the ending automatically return false
    return false;
}

bool Util::startsWith(const std::string& fullString, const std::string& ending) { return (fullString.rfind(ending, 0) == 0); }

std::string Util::toUpperCase(std::string string) {
    std::transform(string.begin(), string.end(), string.begin(), ::toupper);
    return string;
}

OperatorId Util::getNextOperatorId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t Util::getNextPipelineId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

bool Util::assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan,
                                            std::vector<std::map<std::string, std::any>> properties) {
    // count the number of operators in the query
    auto queryPlanIterator = QueryPlanIterator(queryPlan);
    size_t numOperators = queryPlanIterator.snapshot().size();
    ;

    // check if we supply operator properties for all operators
    if (numOperators != properties.size()) {
        NES_ERROR2("UtilityFunctions::assignPropertiesToQueryOperators: the number of properties does not match the number of "
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
            node->as<LogicalOperatorNode>()->addProperty(key, val);
        }
        ++propertyIterator;
    }

    return true;
}

std::string Util::updateSourceName(std::string queryPlanSourceConsumed, std::string subQueryPlanSourceConsumed) {
    //Update the Source names by sorting and then concatenating the source names from the sub query plan
    std::vector<std::string> sourceNames;
    sourceNames.emplace_back(subQueryPlanSourceConsumed);
    sourceNames.emplace_back(queryPlanSourceConsumed);
    std::sort(sourceNames.begin(), sourceNames.end());
    // accumulating sourceNames with delimiters between all sourceNames to enable backtracking of origin
    auto updatedSourceName =
        std::accumulate(sourceNames.begin(), sourceNames.end(), std::string("-"), [](std::string a, std::string b) {
            return a + "_" + b;
        });
    return updatedSourceName;
}

void Util::writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header) {
    std::ofstream ofstream(csvFileName, std::ios::trunc | std::ios::out);
    ofstream << header << std::endl;
    ofstream.close();
}

void Util::writeRowToCsvFile(const std::string& csvFileName, const std::string& row) {
    std::ofstream ofstream(csvFileName, std::ios::app | std::ios::out);
    ofstream << row << std::endl;
    ofstream.close();
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
        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);
        parser->writeInputTupleToTupleBuffer(line, tupleCount, dynamicTupleBuffer, schema, bufferManager);
        ++tupleCount;

        // If we have read enough tuples from the csv file, then stop iterating over it
        auto curTimeStamp = dynamicTupleBuffer[tupleCount - 1][timeStampFieldName].read<uint64_t>();
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
}// namespace NES