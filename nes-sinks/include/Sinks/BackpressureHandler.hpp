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
#include <deque>
#include <optional>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <folly/Synchronized.h>
#include <BackpressureChannel.hpp>

namespace NES
{

/// Buffers tuple buffers that downstream is not yet ready to accept and applies backpressure with hysteresis.
///
/// A sink calls `onFull` when its outbound channel rejects a buffer (queue full / max in-flight reached).
/// The handler stashes the buffer in an internal deque and acquires backpressure from the
/// associated `BackpressureController` once the deque hits `upperThreshold`.
///
/// Exactly one buffer is kept "pending" at any time so the sink has something to retry on its next
/// call, breaking the deadlock that would arise if both deque and pending slot were empty while
/// upstream is blocked on backpressure.
///
/// `onSuccess` is called when a buffer was accepted by the channel. The handler clears the pending
/// slot, releases backpressure once the deque drops to `lowerThreshold`, and returns the next
/// queued buffer (if any) so the sink can immediately attempt the next send.
class BackpressureHandler
{
    struct State
    {
        bool hasBackpressure = false;
        std::deque<TupleBuffer> buffered;
        SequenceNumber pendingSequenceNumber = INVALID<SequenceNumber>;
        ChunkNumber pendingChunkNumber = INVALID<ChunkNumber>;
    };

    folly::Synchronized<State> stateLock;
    size_t upperThreshold;
    size_t lowerThreshold;

public:
    /// @param upperThreshold Number of buffered tuple buffers at which backpressure is acquired.
    /// @param lowerThreshold Number of buffered tuple buffers at which backpressure is released.
    explicit BackpressureHandler(size_t upperThreshold = 1, size_t lowerThreshold = 0); /// NOLINT(fuchsia-default-arguments-declarations)

    std::optional<TupleBuffer> onFull(TupleBuffer buffer, BackpressureController& backpressureController);
    std::optional<TupleBuffer> onSuccess(BackpressureController& backpressureController);
    [[nodiscard]] bool empty() const;
};

}
