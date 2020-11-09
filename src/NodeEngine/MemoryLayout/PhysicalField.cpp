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

#include <NodeEngine/MemoryLayout/PhysicalField.hpp>

namespace NES {

PhysicalField::PhysicalField(uint64_t bufferOffset) : bufferOffset(bufferOffset){};

PhysicalField::~PhysicalField(){};

/**
 * @brief This function is only valid for ArrayPhysicalFields.
 * @return std::shared_ptr<ArrayPhysicalField>
 */
std::shared_ptr<ArrayPhysicalField> PhysicalField::asArrayField() {
    NES_FATAL_ERROR("This field is not an array field");
    throw IllegalArgumentException("This field is not an array field");
}
}// namespace NES