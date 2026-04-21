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

#pragma once

#include <memory>
#include <rust/cxx.h>
#include "Runtime/BufferManager.hpp"

namespace NES::detail
{
size_t buffer_size(MemorySegment*);
uint8_t* buffer_data(MemorySegment*);
void buffer_retain(MemorySegment* segment);
void buffer_release(MemorySegment* segment);
MemorySegment* buffer_load_child(MemorySegment* segment, size_t index);
size_t buffer_store_child(MemorySegment* segment, MemorySegment* child);
size_t buffer_num_children(MemorySegment* segment);
size_t buffer_number_of_tuples(MemorySegment* segment);
uint64_t buffer_sequence_number(MemorySegment* segment);
uint64_t buffer_origin_id(MemorySegment* segment);
uint64_t buffer_chunk_number(MemorySegment* segment);
uint64_t buffer_watermark(MemorySegment* segment);
bool buffer_last_chunk(MemorySegment* segment);

void buffer_set_number_of_tuples(MemorySegment* segment, size_t n);
void buffer_set_sequence_number(MemorySegment* segment, uint64_t seq);
void buffer_set_origin_id(MemorySegment* segment, uint64_t origin);
void buffer_set_chunk_number(MemorySegment* segment, uint64_t chunk);
void buffer_set_watermark(MemorySegment* segment, uint64_t watermark);
void buffer_set_last_chunk(MemorySegment* segment, bool last_chunk);
}

using BufferProviderHandle = NES::AbstractBufferProvider;
struct OnBufferAvailable;

NES::detail::MemorySegment* try_allocate(const std::shared_ptr<BufferProviderHandle>& handle);

NES::detail::MemorySegment* try_allocate_unpooled(const std::shared_ptr<BufferProviderHandle>& handle, std::size_t size);

void wake_on_buffer_available(
    const std::shared_ptr<BufferProviderHandle>& handle, rust::Fn<bool(rust::Box<OnBufferAvailable>)> done, rust::Box<OnBufferAvailable> ctx);
