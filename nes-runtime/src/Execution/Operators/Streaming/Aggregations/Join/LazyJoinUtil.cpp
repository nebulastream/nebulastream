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

    Runtime::TupleBuffer getRecordFromPointer(uint8_t* recordPtr, SchemaPtr schema) {
        Runtime::TupleBuffer buffer;
        uint8_t* bufferPtr = buffer.getBuffer();

        auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
        for (auto& field : schema->fields) {
            auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
            memcpy(recordPtr, bufferPtr, fieldType->size());
            bufferPtr += fieldType->size();
            recordPtr += fieldType->size();
        }
        return buffer;
    }

    Runtime::TupleBuffer getBufferFromNautilus(Nautilus::Record nautilusRecord, SchemaPtr schema) {
        Runtime::TupleBuffer buffer;
        uint8_t* bufferPtr = buffer.getBuffer();

        auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
        for (auto& field : schema->fields) {
            auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

            auto fieldVal = nautilusRecord.read(field->getName());

            memcpy(&fieldVal, bufferPtr, fieldType->size());
            bufferPtr += fieldType->size();
        }


        return buffer;
    }
}