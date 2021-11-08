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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_MEMORY_LAYOUT_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_MEMORY_LAYOUT_HPP_

#include <Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

namespace NES::Runtime::MemoryLayouts {

class MemoryLayoutTupleBuffer;

/**
 * @brief This abstract class is the base class for DynamicRowLayout and DynamicColumnLayout.
 * As the base class, it has multiple methods or members that are useful for both a row and column layout.
 */
class MemoryLayout {

  public:
    /**
     * @brief Constructor for abstract class DynamicMemoryLayout
     * @param checkBoundaryFieldChecks
     * @param recordSize
     * @param fieldSizes
     */
    MemoryLayout(uint64_t bufferSize, const SchemaPtr& schema);

    virtual ~MemoryLayout() = default;

    /**
     * @param fieldName
     * @return either field index for fieldName or empty optinal
     */
    [[nodiscard]] std::optional<uint64_t> getFieldIndexFromName(const std::string& fieldName) const;

    /**
     * @return number of current records
     */
    [[nodiscard]] uint64_t getRecordSize() const;

    /**
     * @return reference of field sizes vector
     */
    [[nodiscard]] const std::vector<FIELD_SIZE>& getFieldSizes() const;

    virtual uint64_t getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) = 0;
    uint64_t getCapacity() const;

  protected:
    const uint64_t bufferSize;
    const SchemaPtr schema;
    uint64_t recordSize;
    uint64_t capacity;
    std::vector<uint64_t> physicalFieldSizes;
    std::map<std::string, uint64_t> nameFieldIndexMap;
};

}// namespace NES::Runtime::MemoryLayouts
#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_MEMORY_LAYOUT_HPP_
