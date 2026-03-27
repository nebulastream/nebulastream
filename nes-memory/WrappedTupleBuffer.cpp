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

#include <Runtime/WrappedTupleBuffer.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

WrappedBufferResult wrapExternalMemory(
    uint8_t* ptr,
    uint32_t size,
    std::function<void(detail::MemorySegment*, BufferRecycler*)> recycleCallback)
{
    auto segment = std::make_shared<detail::MemorySegment>(ptr, size, std::move(recycleCallback));
    auto buffer = segment->toTupleBuffer();
    return {std::move(buffer), std::move(segment)};
}

}
