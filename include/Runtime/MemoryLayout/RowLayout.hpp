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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_ROW_LAYOUT_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_ROW_LAYOUT_HPP_

#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

namespace NES::Runtime::MemoryLayouts {

using FIELD_OFFSET = uint64_t;

/**
 * @brief This class derives from DynamicMemoryLayout. It implements abstract bind() function as well as adding fieldOffsets as a new member
 * @caution This class is non-thread safe
 */
class RowLayout : public MemoryLayout, public std::enable_shared_from_this<RowLayout> {

  public:
    RowLayout(SchemaPtr schema, uint64_t bufferSize);

    /**
     * @brief Creates a DynamicColumnLayout as a shared_ptr
     * @param schema
     * @param checkBoundaries
     * @return created DynamicRowLayout as a shared ptr
     */
    static std::shared_ptr<RowLayout> create(SchemaPtr schema, uint64_t bufferSize);

    /**
     * @return fieldOffSets vector
     */
    const std::vector<FIELD_SIZE>& getFieldOffSets() const;

    /**
     * Binds a memoryLayout to a tupleBuffer
     * @param tupleBuffer
     * @return shared_ptr to DynamicRowLayoutBuffer
     */
    std::shared_ptr<RowLayoutTupleBuffer> bind(const TupleBuffer& tupleBuffer);

    uint64_t getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) override;

  private:
    std::vector<FIELD_OFFSET> fieldOffSets;
};

}// namespace NES::Runtime::MemoryLayouts

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_ROW_LAYOUT_HPP_
