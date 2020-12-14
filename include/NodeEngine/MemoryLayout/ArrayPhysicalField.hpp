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

#ifndef NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
#define NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>

namespace NES::NodeEngine {

/**
 * @brief Represents an array field at a specific position in a memory buffer.
 */
class ArrayPhysicalField : public PhysicalField {
  public:
    ArrayPhysicalField(PhysicalTypePtr componentField, uint64_t bufferOffset);
    ~ArrayPhysicalField() = default;
    /**
     * @brief Cast the PhysicalField into an ArrayField
     * @return std::shared_ptr<ArrayPhysicalField>
     */
    std::shared_ptr<ArrayPhysicalField> asArrayField() override;

    /**
     * @brief Get the PhysicalField at a specific array position.
     * @return std::shared_ptr<PhysicalField>
     */
    std::shared_ptr<PhysicalField> operator[](uint64_t arrayIndex);

  private:
    PhysicalTypePtr componentField;
};
}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
