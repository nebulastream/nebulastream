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

#ifndef NES_DYNAMICCOLUMNLAYOUT_HPP
#define NES_DYNAMICCOLUMNLAYOUT_HPP

#include <NodeEngine/MemoryLayout/DynamicMemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {

/**
 * @brief This class derives from DynamicMemoryLayout. It implements abstract bind() function as well as adding fieldOffsets as a new member
 * This class is non-thread safe
 */
class DynamicColumnLayout;

class DynamicColumnLayout : public DynamicMemoryLayout, public std::enable_shared_from_this<DynamicColumnLayout> {

  public:
    DynamicMemoryLayoutPtr copy() const override;

    /**
     * @brief Creates a DynamicColumnLayout as a shared_ptr
     * @param schema
     * @param checkBoundaries
     * @return
     */
    static DynamicColumnLayoutPtr create(SchemaPtr schema, bool checkBoundaries);

    /**
     * Binds a memoryLayout to a tupleBuffer
     * @param tupleBuffer
     * @return shared_ptr to DynamicRowLayoutBuffer
     */
    DynamicColumnLayoutBufferPtr bind(TupleBuffer tupleBuffer);

    /**
     * @brief Constructor for DynamicColumnLayout
     * @param checkBoundaries
     * @param schema
     */
    DynamicColumnLayout(bool checkBoundaries, SchemaPtr schema);
};

}// namespace NES::NodeEngine::DynamicMemoryLayout

#endif//NES_DYNAMICCOLUMNLAYOUT_HPP
