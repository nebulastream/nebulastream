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
#include <cstdint>
#include <thread>
#include <utility>

namespace NES
{
namespace detail
{
class MemorySegment;
}

/// Stores necessary information for the recycle unpooled buffer callback
struct AllocationThreadInfo
{
    AllocationThreadInfo(std::thread::id threadId, uint8_t* lastChunkPtr) : threadId(std::move(threadId)), lastChunkPtr(lastChunkPtr) { }

    bool operator==(const AllocationThreadInfo& other) const = default;

    std::thread::id threadId;
    uint8_t* lastChunkPtr;
};

///@brief Interface for buffer recycling mechanism
class BufferRecycler
{
public:
    /// @brief Interface method for pooled buffer recycling
    /// @param buffer the buffer to recycle
    virtual void recyclePooledBuffer(detail::MemorySegment* buffer) = 0;

    /// @brief Interface method for unpooled buffer recycling
    /// @param buffer the buffer to recycle
    /// @param threadCopyLastChunkPtr stores the thread id and last chunk ptr
    virtual void recycleUnpooledBuffer(detail::MemorySegment* buffer, const AllocationThreadInfo& threadCopyLastChunkPtr) = 0;
    virtual ~BufferRecycler() = default;
};

}
