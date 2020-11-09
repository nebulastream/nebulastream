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


#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/MemoryLayout/ArrayPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <memory>
#include <utility>
namespace NES {
ArrayPhysicalField::ArrayPhysicalField(PhysicalTypePtr componentField, uint64_t bufferOffset)
    : PhysicalField(bufferOffset), componentField(std::move(componentField)) {}

std::shared_ptr<ArrayPhysicalField> ArrayPhysicalField::asArrayField() {
    return std::static_pointer_cast<ArrayPhysicalField>(this->shared_from_this());
}

std::shared_ptr<PhysicalField> ArrayPhysicalField::operator[](uint64_t arrayIndex) {
    auto offsetInArray = componentField->size() * arrayIndex;
    return PhysicalFieldUtil::createPhysicalField(componentField, bufferOffset + offsetInArray);
}

PhysicalFieldPtr createArrayPhysicalField(PhysicalTypePtr componentField, uint64_t bufferOffset) {
    return std::make_shared<ArrayPhysicalField>(componentField, bufferOffset);
}
}// namespace NES