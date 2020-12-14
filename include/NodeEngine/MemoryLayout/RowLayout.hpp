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

#ifndef INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_
#define INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_

#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
namespace NES::NodeEngine {

/**
 * @brief Rowlayout every field of a tuple is stores sequentially.
 */
class RowLayout : public MemoryLayout {
  public:
    RowLayout(PhysicalSchemaPtr physicalSchema);

    uint64_t getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) override;

    MemoryLayoutPtr copy() const override;
};

}// namespace NES

#endif//INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_
