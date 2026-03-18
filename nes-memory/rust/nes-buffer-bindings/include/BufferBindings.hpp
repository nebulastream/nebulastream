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
}

using BufferProviderHandle = NES::BufferManager;
struct OnBufferAvailable;

NES::detail::MemorySegment* try_allocate(const std::shared_ptr<BufferProviderHandle>& handle);

void wake_on_buffer_available(
    const std::shared_ptr<BufferProviderHandle>& handle, rust::Fn<bool(rust::Box<OnBufferAvailable>)> done, rust::Box<OnBufferAvailable> ctx);
