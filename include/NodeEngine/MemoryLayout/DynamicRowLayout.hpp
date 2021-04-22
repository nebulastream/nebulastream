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

#ifndef NES_DYNAMICROWLAYOUT_HPP
#define NES_DYNAMICROWLAYOUT_HPP

#include <NodeEngine/MemoryLayout/DynamicMemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {

typedef uint64_t FIELD_OFFSET;

/**
 * @brief This class derives from DynamicMemoryLayout. It implements abstract bind() function as well as adding fieldOffsets as a new member
 * This class is non-thread safe
 */
class DynamicRowLayout : public DynamicMemoryLayout, public std::enable_shared_from_this<DynamicRowLayout> {

  public:
    DynamicMemoryLayoutPtr copy() const override;
    DynamicRowLayout(bool checkBoundaries, SchemaPtr schema);
    static DynamicRowLayoutPtr create(SchemaPtr schema, bool checkBoundaries);
    const std::vector<FIELD_SIZE>& getFieldOffSets() const;

    /**
     * Maps a memoryLayout to a tupleBuffer
     * @param tupleBuffer
     * @return
     */
    DynamicRowLayoutBufferPtr bind(TupleBuffer tupleBuffer);

  private:
    std::vector<FIELD_OFFSET> fieldOffSets;
};

}// namespace NES::NodeEngine::DynamicMemoryLayout

#endif//NES_DYNAMICROWLAYOUT_HPP
