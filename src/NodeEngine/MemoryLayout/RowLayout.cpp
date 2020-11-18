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

#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <utility>

namespace NES {

RowLayout::RowLayout(PhysicalSchemaPtr physicalSchema) : MemoryLayout(std::move(physicalSchema)) {}

uint64_t RowLayout::getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) {
    return this->physicalSchema->getFieldOffset(fieldIndex) + (this->physicalSchema->getRecordSize() * recordIndex);
}

MemoryLayoutPtr RowLayout::copy() const { return std::make_shared<RowLayout>(*this); }

MemoryLayoutPtr createRowLayout(SchemaPtr schema) {
    auto physicalSchema = PhysicalSchema::createPhysicalSchema(schema);
    return std::make_shared<RowLayout>(physicalSchema);
};

}// namespace NES