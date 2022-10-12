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

#include <Runtime/TupleBuffer.hpp>

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
            auto ptr = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(ptr);
            auto fieldSize = physicalType->size();
            auto str = physicalType->convertRawToString(buffer + offset + i * schema->getSchemaSizeInBytes());
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