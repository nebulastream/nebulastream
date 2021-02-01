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

#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {

uint64_t DynamicRowLayoutBuffer::calcOffset(uint64_t ithRecord, uint64_t jthField, const bool boundaryChecks) {

    auto fieldOffSets = dynamicRowLayout->getFieldOffSets();
    auto recordSize = dynamicRowLayout->getRecordSize();
    if (boundaryChecks) NES_VERIFY(jthField < fieldOffSets.size(), "jthField" << jthField << " is larger than fieldOffsets.size() " << fieldOffSets.size());

    auto offSet = (ithRecord * recordSize) + fieldOffSets[jthField];
    NES_DEBUG("DynamicRowLayoutBuffer.calcOffset: offSet = " << offSet);
    return offSet;
}
DynamicRowLayoutBuffer::DynamicRowLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity, DynamicRowLayoutPtr dynamicRowLayout)
    : DynamicLayoutBuffer(tupleBuffer, capacity), dynamicRowLayout(dynamicRowLayout) {}


}