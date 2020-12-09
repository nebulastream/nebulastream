/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <Util/Logger.hpp>
#include <assert.h>

namespace NES::NodeEngine {

PhysicalSchema::PhysicalSchema(SchemaPtr schemaPtr) : schema(schemaPtr){};

PhysicalSchemaPtr PhysicalSchema::createPhysicalSchema(SchemaPtr schema) { return std::make_shared<PhysicalSchema>(schema); }

PhysicalFieldPtr PhysicalSchema::createPhysicalField(uint64_t fieldIndex, uint64_t bufferOffset) {
    assert(validFieldIndex(fieldIndex));
    auto logicalFieldsInSchema = schema->fields;
    auto logicalField = logicalFieldsInSchema[fieldIndex];
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(logicalField->getDataType());
    return PhysicalFieldUtil::createPhysicalField(physicalType, bufferOffset);
}

uint64_t PhysicalSchema::getFieldOffset(uint64_t fieldIndex) {
    assert(validFieldIndex(fieldIndex));
    uint64_t offset = 0;
    auto physicalDataFactory = DefaultPhysicalTypeFactory();
    for (uint64_t index = 0; index < fieldIndex; index++) {
        offset += physicalDataFactory.getPhysicalType(schema->get(index)->getDataType())->size();
    }
    return offset;
}

bool PhysicalSchema::validFieldIndex(uint64_t fieldIndex) {
    auto fields = schema->fields;
    if (fieldIndex > fields.size()) {
        NES_FATAL_ERROR("PhysicalSchema: field index " << fieldIndex << " is out of bound. Schema only contains " << fields.size()
                                                       << " fields.");
        throw IllegalArgumentException("Field index out of bound");
    }
    return true;
}

uint64_t PhysicalSchema::getRecordSize() { return this->schema->getSchemaSizeInBytes(); }
}// namespace NES
