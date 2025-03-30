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

#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Runtime {

TupleBuffer TupleBuffer::reinterpretAsTupleBuffer(void* bufferPointer) {
    auto controlBlockSize = alignBufferSize(sizeof(Runtime::detail::BufferControlBlock), 64);
    auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
    auto block = reinterpret_cast<Runtime::detail::BufferControlBlock*>(buffer - controlBlockSize);
    auto memorySegment = block->getOwner();
    auto tb = TupleBuffer(memorySegment->controlBlock.get(), memorySegment->ptr, memorySegment->size);
    tb.retain();
    return tb;
}

uint32_t TupleBuffer::storeChildBuffer(TupleBuffer& buffer) const noexcept {
    TupleBuffer empty;
    auto* control = buffer.controlBlock;
    NES_ASSERT2_FMT(controlBlock != control, "Cannot attach buffer to self");
    auto index = controlBlock->storeChildBuffer(control);
    std::swap(empty, buffer);
    return index;
}

TupleBuffer TupleBuffer::loadChildBuffer(NestedTupleBufferKey bufferIndex) const noexcept {
    TupleBuffer childBuffer;
    NES_ASSERT(controlBlock->loadChildBuffer(bufferIndex, childBuffer.controlBlock, childBuffer.ptr, childBuffer.size),
               "Cannot load tuple buffer");
    return childBuffer;
}

bool recycleTupleBuffer(void* bufferPointer) {
    NES_ASSERT2_FMT(bufferPointer, "invalid bufferPointer");
    auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
    auto block = reinterpret_cast<Runtime::detail::BufferControlBlock*>(buffer - sizeof(Runtime::detail::BufferControlBlock));
    return block->release();
}

bool TupleBuffer::hasSpaceLeft(uint64_t used, uint64_t needed) const {
    if (used + needed <= this->size) {
        return true;
    }
    return false;
}
}// namespace NES::Runtime
