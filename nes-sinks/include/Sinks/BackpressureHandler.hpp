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
#include <Runtime/TupleBuffer.hpp>
#include <folly/Synchronized.h>
#include <BackpressureManager.hpp>

namespace NES
{

/// Buffers tuple buffers that downstream is not yet ready to accept and applies backpressure with hysteresis.
///
/// A sink calls `onFull` when its outbound channel rejects a buffer (queue full / max in-flight reached).
/// The handler stashes the buffer in an internal deque and acquires backpressure from the
/// voter passed to `start` once the deque hits `upperThreshold`.
/// The voter casts a hard vote, which blocks all sources of the query.
///
/// While rejected buffers remain, exactly one is marked "pending": it is either being attempted by
/// the sink or held by a scheduled retry task, outside the deque. Other rejected buffers stay queued.
///
/// `onSuccess` is called with the buffer accepted by the channel. Only success of the pending buffer
/// clears its marker and transfers it to the next queued buffer (if any) for an immediate attempt.
/// Success of an unrelated buffer leaves the pending retry and deque unchanged.
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
        std::optional<BackpressureVoter> voter;
        std::deque<TupleBuffer> buffered;
        std::optional<BufferId> pending;
    };

    folly::Synchronized<State> stateLock;
    size_t upperThreshold;
    size_t lowerThreshold;

public:
    /// @param upperThreshold Number of buffered tuple buffers at which backpressure is acquired.
    /// @param lowerThreshold Number of buffered tuple buffers at which backpressure is released.
    explicit BackpressureHandler(size_t upperThreshold = 1, size_t lowerThreshold = 0); /// NOLINT(fuchsia-default-arguments-declarations)

    /// Must be called before `onFull` or `onSuccess`, usually in the `start` of the sink.
    void start(BackpressureVoter voter);

    std::optional<TupleBuffer> onFull(TupleBuffer buffer);
    std::optional<TupleBuffer> onSuccess(const TupleBuffer& buffer);
    [[nodiscard]] bool empty() const;
};

}
