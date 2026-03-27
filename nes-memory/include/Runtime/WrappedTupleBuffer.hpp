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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
namespace detail
{
class MemorySegment;
}

/// Wraps externally-owned memory into a TupleBuffer with a custom recycle callback.
/// The caller must ensure that the backing memory outlives all TupleBuffer copies.
/// Returns both the TupleBuffer and a unique_ptr to the MemorySegment that must be kept alive.
struct WrappedBufferResult
{
    TupleBuffer buffer;
    std::shared_ptr<detail::MemorySegment> segment;
};

WrappedBufferResult wrapExternalMemory(
    uint8_t* ptr,
    uint32_t size,
    std::function<void(detail::MemorySegment*, BufferRecycler*)> recycleCallback);

}
