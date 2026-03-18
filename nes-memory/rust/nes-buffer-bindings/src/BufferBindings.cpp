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

#include <BufferBindings.hpp>

#include <cstdint>
#include <memory>
#include <stop_token>

#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>

#include <nes-buffer-bindings/lib.h>
#include <TupleBufferImpl.hpp>

namespace NES::detail
{
size_t buffer_size(MemorySegment* segment)
{
    return segment->size;
}

uint8_t* buffer_data(MemorySegment* segment)
{
    return segment->getPointer();
}

void buffer_retain(MemorySegment* segment)
{
    segment->controlBlock->retain();
}

void buffer_release(MemorySegment* segment)
{
    segment->controlBlock->release();
}

MemorySegment* buffer_load_child(MemorySegment* segment, size_t index)
{
    return segment->controlBlock->getChild(VariableSizedAccess::Index(index));
}

size_t buffer_store_child(MemorySegment* segment, MemorySegment* child)
{
    return segment->controlBlock->storeChild(child).getRawIndex();
}

size_t buffer_num_children(MemorySegment* segment)
{
    return segment->controlBlock->getNumberOfChildBuffers();
}

size_t buffer_number_of_tuples(MemorySegment* segment)
{
    return segment->controlBlock->getNumberOfTuples();
}
}

NES::detail::MemorySegment* try_allocate(const std::shared_ptr<BufferProviderHandle>& handle)
{
    if (auto buffer = handle->getBufferNoBlocking())
    {
        /// a little hack to keep the buffer alive: we increment the reference count once so if the ref count is reduced because the buffer
        /// is destroyed before rust can increase the ref count again, the buffer is not recycled: This requires that the rust side
        /// decrements the ref count
        buffer->retain();
        return NES::toRust(*buffer);
    }
    return nullptr;
}

void wake_on_buffer_available(
    const std::shared_ptr<BufferProviderHandle>& handle, rust::Fn<bool(rust::Box<OnBufferAvailable> ctx)> done, rust::Box<OnBufferAvailable> ctx)
{
    handle->notifyOnAvailableBuffer([done = std::move(done), ctx = std::move(ctx)]() mutable { return done(std::move(ctx)); });
}
