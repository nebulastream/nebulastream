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

#include <chrono>
#include <cstddef>
#include <thread>
#include <type_traits>
#include <variant>

namespace NES
{

/// Common part of every buffer event.
///
/// Unlike 'EventBase' of the query engine's 'QueryEngineStatisticListener' this deliberately carries no
/// timestamp. Buffer events are emitted on the buffer handoff path, which runs orders of magnitude more
/// often than a task starts, and a 'clock_gettime' would be the most expensive part of emitting one.
/// A listener that needs a timestamp can take it inside 'onEvent': that runs on the emitting thread, at
/// the emitting instant.
struct BufferEventBase
{
    std::thread::id threadId = std::this_thread::get_id();
};

/// A pooled buffer left the pool. 'availableAfter' is the pool's fill level observed right afterwards,
/// so a consumer can plot occupancy without reconstructing it from the +1/-1 sequence.
struct PooledBufferAcquired : BufferEventBase
{
    explicit PooledBufferAcquired(size_t availableAfter) : availableAfter(availableAfter) { }

    PooledBufferAcquired() = default;

    size_t availableAfter = 0;
};

/// A pooled buffer was returned to the pool, i.e. the last 'TupleBuffer' referencing it went away.
struct PooledBufferRecycled : BufferEventBase
{
    explicit PooledBufferRecycled(size_t availableAfter) : availableAfter(availableAfter) { }

    PooledBufferRecycled() = default;

    size_t availableAfter = 0;
};

/// A request for a pooled buffer found the pool empty. 'waited' is the timeout the caller granted, so a
/// zero value distinguishes the non-blocking probe from a real starvation stall.
struct PooledBufferRequestFailed : BufferEventBase
{
    explicit PooledBufferRequestFailed(std::chrono::milliseconds waited) : waited(waited) { }

    PooledBufferRequestFailed() = default;

    std::chrono::milliseconds waited{0};
};

/// An unpooled (variable sized) buffer was handed out, served either from an existing chunk or from a
/// chunk that was just allocated (reported separately as 'UnpooledChunkAllocated').
struct UnpooledBufferAllocated : BufferEventBase
{
    UnpooledBufferAllocated(size_t requestedBytes, size_t unpooledBytesInUse)
        : requestedBytes(requestedBytes), unpooledBytesInUse(unpooledBytesInUse)
    {
    }

    UnpooledBufferAllocated() = default;

    size_t requestedBytes = 0;
    size_t unpooledBytesInUse = 0;
};

/// The unpooled memory budget or the underlying allocator refused a request.
struct UnpooledBufferRequestFailed : BufferEventBase
{
    UnpooledBufferRequestFailed(size_t requestedBytes, size_t unpooledBytesInUse)
        : requestedBytes(requestedBytes), unpooledBytesInUse(unpooledBytesInUse)
    {
    }

    UnpooledBufferRequestFailed() = default;

    size_t requestedBytes = 0;
    size_t unpooledBytesInUse = 0;
};

/// A new unpooled chunk was taken from the memory resource.
struct UnpooledChunkAllocated : BufferEventBase
{
    UnpooledChunkAllocated(size_t chunkBytes, size_t unpooledBytesInUse)
        : chunkBytes(chunkBytes), unpooledBytesInUse(unpooledBytesInUse) { }

    UnpooledChunkAllocated() = default;

    size_t chunkBytes = 0;
    size_t unpooledBytesInUse = 0;
};

/// All buffers of an unpooled chunk were released and the chunk went back to the memory resource.
struct UnpooledChunkReleased : BufferEventBase
{
    UnpooledChunkReleased(size_t chunkBytes, size_t unpooledBytesInUse) : chunkBytes(chunkBytes), unpooledBytesInUse(unpooledBytesInUse) { }

    UnpooledChunkReleased() = default;

    size_t chunkBytes = 0;
    size_t unpooledBytesInUse = 0;
};

/// The pool finished initializing. Makes the event stream self describing: a consumer learns the pool's
/// dimensions without having to be handed the configuration.
struct BufferPoolCreated : BufferEventBase
{
    BufferPoolCreated(size_t numberOfBuffers, size_t bufferSize, size_t unpooledBudgetBytes)
        : numberOfBuffers(numberOfBuffers), bufferSize(bufferSize), unpooledBudgetBytes(unpooledBudgetBytes)
    {
    }

    BufferPoolCreated() = default;

    size_t numberOfBuffers = 0;
    size_t bufferSize = 0;
    size_t unpooledBudgetBytes = 0;
};

/// The pool is being torn down. 'leakedBuffers' is how many pooled buffers were still checked out, which
/// is the condition 'BufferManager::destroy' turns into a fatal INVARIANT right afterwards.
struct BufferPoolDestroyed : BufferEventBase
{
    explicit BufferPoolDestroyed(size_t leakedBuffers) : leakedBuffers(leakedBuffers) { }

    BufferPoolDestroyed() = default;

    size_t leakedBuffers = 0;
};

using BufferEvent = std::variant<
    PooledBufferAcquired,
    PooledBufferRecycled,
    PooledBufferRequestFailed,
    UnpooledBufferAllocated,
    UnpooledBufferRequestFailed,
    UnpooledChunkAllocated,
    UnpooledChunkReleased,
    BufferPoolCreated,
    BufferPoolDestroyed>;
static_assert(std::is_default_constructible_v<BufferEvent>, "Events should be default constructible");

/// Observes what the buffer providers do with pooled and unpooled memory.
///
/// The buffer providers hold this as an optional 'shared_ptr': when nobody listens the cost of the
/// instrumentation is a single branch per buffer handoff.
struct BufferProviderStatisticListener
{
    virtual ~BufferProviderStatisticListener() = default;

    /// Called on whichever thread asks for or releases a buffer: query engine worker threads, source
    /// threads, and the thread that tears the pool down. It must not block, and it has to be thread safe.
    ///
    /// Note that this can be called while the buffer provider holds an internal lock ('UnpooledChunkAllocated'
    /// is emitted under the per-thread unpooled chunk lock) and after the provider itself has been destroyed
    /// ('UnpooledChunkReleased' outlives it by design), so an implementation must not call back into the
    /// buffer provider.
    virtual void onEvent(BufferEvent event) = 0;
};

}
