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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <functional>
#include <optional>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <folly/Synchronized.h>
#include <BackpressureChannel.hpp>

namespace NES
{

/// Buffers tuple buffers that downstream is not yet ready to accept and applies backpressure with hysteresis.
///
/// A sink describes its outbound channel with a `TrySend` function and forwards its `execute` to `send` and its `stop` to `stop`.
///
/// When the channel rejects a buffer (queue full / max in-flight reached), the handler stashes the buffer in an
/// internal deque and acquires backpressure from the associated `BackpressureController` once the deque hits `upperThreshold`.
///
/// While rejected buffers remain, exactly one is marked "pending": it is either being attempted by
/// the sink or held by a scheduled retry task, outside the deque. Other rejected buffers stay queued.
///
/// `onSuccess` is called with the buffer accepted by the channel. Only success of the pending buffer
/// clears its marker and transfers it to the next queued buffer (if any) for an immediate attempt.
/// Success of an unrelated buffer leaves the pending retry and deque unchanged.
///
/// When `TrySend` throws, the channel is considered unusable: the handler is closed and never calls into the sink again.
class BackpressureHandler
{
    struct BufferId
    {
        OriginId originId;
        SequenceNumber sequenceNumber;
        ChunkNumber chunkNumber;

        bool operator==(const BufferId&) const = default;
    };

    struct State
    {
        bool hasBackpressure = false;
        std::deque<TupleBuffer> buffered;
        std::optional<BufferId> pending;
    };

    folly::Synchronized<State> stateLock;
    size_t upperThreshold;
    size_t lowerThreshold;
    std::atomic_bool closed;

public:
    /// Attempts to hand a buffer to the outbound channel.
    /// Returns true if the channel accepted the buffer and false if it is at capacity. Throws if the channel is unusable.
    using TrySend = std::function<bool(const TupleBuffer&)>;

    static constexpr auto RETRY_INTERVAL = std::chrono::milliseconds(10);

    /// @param upperThreshold Number of buffered tuple buffers at which backpressure is acquired.
    /// @param lowerThreshold Number of buffered tuple buffers at which backpressure is released.
    explicit BackpressureHandler(size_t upperThreshold = 1, size_t lowerThreshold = 0); /// NOLINT(fuchsia-default-arguments-declarations)

    std::optional<TupleBuffer> onFull(TupleBuffer buffer, BackpressureController& backpressureController);
    std::optional<TupleBuffer> onSuccess(const TupleBuffer& buffer, BackpressureController& backpressureController);
    [[nodiscard]] bool empty() const;

    /// Sends `buffer` and, as long as the channel accepts, the buffers queued before it. If the channel is at capacity the
    /// buffer is queued. Exceptions of `trySend` close the handler and are rethrown; buffers arriving after that are dropped.
    /// Returns the buffer the sink has to retry after `RETRY_INTERVAL` via `PipelineExecutionContext::repeatTask`, if any.
    [[nodiscard]] std::optional<TupleBuffer>
    send(const TupleBuffer& buffer, BackpressureController& backpressureController, const TrySend& trySend);

    /// Sends the buffers that are still queued and then asks `flushed` whether the channel has acknowledged everything that was
    /// sent. `trySend` or `flushed` throwing closes the handler.
    /// Returns false as long as the channel is at capacity or not flushed: the sink has to stay open and either repeat its stop
    /// after `RETRY_INTERVAL` via `PipelineExecutionContext::repeatTask` or give up by calling `close`.
    /// Returns true once the sink can close; the handler is closed then.
    [[nodiscard]] bool stop(BackpressureController& backpressureController, const TrySend& trySend, const std::function<bool()>& flushed);

    /// Drops all queued buffers, releases backpressure and never calls into the sink again.
    void close(BackpressureController& backpressureController);
};

}
