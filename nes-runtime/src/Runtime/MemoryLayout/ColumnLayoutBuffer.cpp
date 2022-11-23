/*
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

#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>
#include <utility>

namespace NES::Runtime::MemoryLayouts {

ColumnLayoutTupleBuffer::ColumnLayoutTupleBuffer(TupleBuffer tupleBuffer,
                                                 uint64_t capacity,
                                                 std::shared_ptr<ColumnLayout> dynamicColLayout,
                                                 std::vector<uint64_t> columnOffsets)
    : MemoryLayoutTupleBuffer(tupleBuffer, capacity), columnOffsets(std::move(columnOffsets)),
      dynamicColLayout(std::move(dynamicColLayout)), basePointer(tupleBuffer.getBuffer<uint8_t>()) {}

}// namespace NES::Runtime::MemoryLayouts