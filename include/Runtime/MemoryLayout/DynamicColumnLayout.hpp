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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_HPP_

#include <Runtime/MemoryLayout/DynamicMemoryLayout.hpp>
#include <Runtime/NodeEngine.hpp>

namespace NES::Runtime::DynamicMemoryLayout {

/**
 * @brief This class derives from DynamicMemoryLayout. It implements abstract bind() function as well as adding fieldOffsets as a new member
 * @caution This class is non-thread safe
 */
class DynamicColumnLayout;

class DynamicColumnLayout : public DynamicMemoryLayout, public std::enable_shared_from_this<DynamicColumnLayout> {

  public:
    /**
     * @brief Copies a DynamicMemoryLayoutPtr
     * @return copied version
     */
    DynamicMemoryLayoutPtr copy() const override;
    ~DynamicColumnLayout() override = default;

    /**
     * @brief Creates a DynamicColumnLayout as a shared_ptr
     * @param schema
     * @param checkBoundaries
     * @return created DynamicColumnLayout as a shared ptr
     */
    static DynamicColumnLayoutPtr create(const SchemaPtr& schema, bool checkBoundaries);

    /**
     * Binds a memoryLayout to a tupleBuffer
     * @param tupleBuffer
     * @return shared_ptr to DynamicRowLayoutBuffer
     */
    DynamicColumnLayoutBufferPtr bind(const TupleBuffer& tupleBuffer);

    /**
     * @brief Constructor for DynamicColumnLayout
     * @param checkBoundaries
     * @param schema
     */
    DynamicColumnLayout(bool checkBoundaries, const SchemaPtr& schema);
};

}// namespace NES::Runtime::DynamicMemoryLayout

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_HPP_
