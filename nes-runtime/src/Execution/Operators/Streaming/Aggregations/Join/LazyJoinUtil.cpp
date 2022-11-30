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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinUtil.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution::Util {
    uint64_t murmurHash(uint64_t key) {
        uint64_t hash = key;

        hash ^= hash >> 33;
        hash *= UINT64_C(0xff51afd7ed558ccd);
        hash ^= hash >> 33;
        hash *= UINT64_C(0xc4ceb9fe1a85ec53);
        hash ^= hash >> 33;

        return hash;
    }

    Runtime::TupleBuffer getBufferFromPointer(uint8_t* recordPtr, SchemaPtr schema, BufferManagerPtr bufferManager) {
        auto buffer = bufferManager->getBufferBlocking();
        uint8_t* bufferPtr = buffer.getBuffer();

        auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
        for (auto& field : schema->fields) {
            auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
            std::memcpy(bufferPtr, recordPtr, fieldType->size());
            bufferPtr += fieldType->size();
            recordPtr += fieldType->size();
        }
        buffer.setNumberOfTuples(1);
        return buffer;
    }

    Runtime::TupleBuffer getBufferFromNautilus(Nautilus::Record nautilusRecord, SchemaPtr schema, BufferManagerPtr bufferManager) {
        auto buffer = bufferManager->getBufferBlocking();
        uint8_t* bufferPtr = buffer.getBuffer();

        auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
        for (auto& field : schema->fields) {
            auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

            auto fieldVal = nautilusRecord.read(field->getName());
            i do not think that this is possible... ask philipp
            memcpy(bufferPtr, fieldVal, fieldType->size());
            bufferPtr += fieldType->size();
        }

        buffer.setNumberOfTuples(1);
        return buffer;
    }

    std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema) {
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
            //        #ifdef TFDEF
            //        auto ts = std::chrono::system_clock::now();
            //        auto expulsionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
            //                                 ts.time_since_epoch()).count();
            //        ss << "," << std::to_string(expulsionTime);
            //        #endif
            ss << std::endl;
        }
        return ss.str();
    }
}