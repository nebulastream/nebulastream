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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_PHYSICAL_FIELD_UTIL_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_PHYSICAL_FIELD_UTIL_HPP_
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <memory>

namespace NES::Runtime {

/**
 * @brief Util class to create a PhysicalField for a specific data type and a offset in a buffer.
 */
class PhysicalFieldUtil {
  public:
    /**
     * @brief creates the corresponding PhysicalFieldPtr with respect to a particular data type.
     * @param dataType
     * @param bufferOffset offset in the underling buffer
     */
    [[nodiscard]] static PhysicalFieldPtr createPhysicalField(PhysicalTypePtr const& physicalType, uint64_t bufferOffset);
};
}// namespace NES::Runtime

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_PHYSICAL_FIELD_UTIL_HPP_
