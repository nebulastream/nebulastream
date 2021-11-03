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

#include <Exceptions/BufferAccessException.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <utility>

namespace NES::Runtime::DynamicMemoryLayout {

uint64_t DynamicRowLayoutBuffer::calcOffset(uint64_t recordIndex, uint64_t fieldIndex, const bool boundaryChecks) {
    auto fieldOffSets = dynamicRowLayout->getFieldOffSets();
    auto recordSize = dynamicRowLayout->getRecordSize();
    if (boundaryChecks && fieldIndex >= fieldOffSets.size()) {
        throw BufferAccessException("jthField " + std::to_string(fieldIndex) + " is larger than fieldOffsets.size() "
                                    + std::to_string(fieldOffSets.size()));
    }
    auto offSet = (recordIndex * recordSize) + fieldOffSets[fieldIndex];
    NES_TRACE("DynamicRowLayoutBuffer.calcOffset: offSet = " << offSet);
    return offSet;
}
DynamicRowLayoutBuffer::DynamicRowLayoutBuffer(TupleBuffer tupleBuffer,
                                               uint64_t capacity,
                                               std::shared_ptr<DynamicRowLayout> dynamicRowLayout)
    : DynamicLayoutBuffer(tupleBuffer, capacity), dynamicRowLayout(std::move(dynamicRowLayout)),
      basePointer(tupleBuffer.getBuffer<uint8_t>()) {}

}// namespace NES::Runtime::DynamicMemoryLayout