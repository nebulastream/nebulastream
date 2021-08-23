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

#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>
namespace NES::Runtime {

TupleBuffer TupleBuffer::wrapMemory(uint8_t* ptr, size_t length, BufferRecycler* parent) {
    auto callback = [](detail::MemorySegment* segment, BufferRecycler* recycler) {
        recycler->recyclePooledBuffer(segment);
    };
    auto freeCallback = [](void*, size_t) {};
    auto* memSegment = new detail::MemorySegment(ptr, length, parent, callback, freeCallback, true);
    return TupleBuffer(memSegment->controlBlock, ptr, length);
}

TupleBuffer TupleBuffer::wrapMemory(uint8_t* ptr, size_t length, std::function<void(detail::MemorySegment*, BufferRecycler*)>&& callback) {
    auto* memSegment = new detail::MemorySegment(ptr, length, nullptr, std::move(callback), [](void*, size_t) {}, true);
    return TupleBuffer(memSegment->controlBlock, ptr, length);
}

}// namespace NES::Runtime