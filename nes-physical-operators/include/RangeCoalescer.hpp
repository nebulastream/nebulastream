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
#include <mutex>
#include <optional>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/BufferMerge.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Merges the complete outputs of adjacent sequence ranges of an origin into fewer buffers covering their union.
/// Held outputs form runs, one per contiguous range, and no two runs are adjacent. A run leaves when it is full, `maxDelay` after its
/// oldest member arrived, or when its origin holds more than `maxHeldRuns` runs and it covers the lowest sequence numbers.
/// An output that cannot merge with an adjacent run leaves if it is fuller, otherwise the run leaves.
/// An output more than half full that merges with nothing leaves at once.
/// A merged buffer keeps each output's tuples together, but not their order across outputs.
/// While anything is held, a pipeline callback is armed to flush expired runs. The pipeline's stop must call `flushAll`.
class RangeCoalescer final
{
public:
    using Clock = std::chrono::steady_clock;

    RangeCoalescer(BufferLayout layout, std::chrono::microseconds maxDelay, size_t maxHeldRuns);

    /// Takes `output`, the complete output of its input range, with all metadata set except the chunk. Buffers that leave are emitted
    /// through `pec` after the lock is released.
    void offer(const TupleBuffer& output, PipelineExecutionContext& pec, Clock::time_point now);

    /// Emits everything held. It never schedules a callback, so the pipeline's stop can call it.
    void flushAll(PipelineExecutionContext& pec);

    /// Emits the runs that expired by `now` and arms the timer again while anything is held. The timer callback calls it.
    void onTimer(PipelineExecutionContext& pec, Clock::time_point now);

private:
    /// Held output of the input range [first, last]. Its buffer always carries its tuple count.
    struct Run
    {
        SequenceNumber::Underlying first;
        SequenceNumber::Underlying last;
        TupleBuffer buffer;
        Timestamp watermark;
        Timestamp creation;
        Clock::time_point deadline;
    };

    struct Origin
    {
        OriginId id;
        /// Sorted by `first`.
        std::vector<Run> runs;
        /// The earliest deadline of `runs`, or `Clock::time_point::max()` if it is empty.
        Clock::time_point earliestDeadline = Clock::time_point::max();
    };

    /// Caller holds `mutex`.
    Origin& originFor(OriginId id);
    /// Merges `current` into the runs of `origin` and moves what leaves to `outgoing`. Caller holds `mutex`.
    void place(Origin& origin, Run&& current, std::vector<TupleBuffer>& outgoing) const;

    [[nodiscard]] bool fits(const Run& lhs, const Run& rhs) const;
    /// Merges the adjacent `from` into `into` by appending the emptier buffer's tuples to the fuller one's.
    void absorb(Run& into, Run from) const;
    static void queueForEmit(Run run, std::vector<TupleBuffer>& outgoing);
    /// Moves the runs of `origin` that expired by `now` to `outgoing` and recomputes its earliest deadline. Caller holds `mutex`.
    static void takeExpired(Origin& origin, Clock::time_point now, std::vector<TupleBuffer>& outgoing);
    /// Caller holds `mutex`.
    void takeAllExpired(Clock::time_point now, std::vector<TupleBuffer>& outgoing);
    static void emit(const std::vector<TupleBuffer>& outgoing, PipelineExecutionContext& pec);
    /// Marks the timer armed and returns its delay if something is held and no callback is pending. Caller holds `mutex`.
    [[nodiscard]] std::optional<std::chrono::microseconds> armIfNeeded(Clock::time_point now);
    void schedule(PipelineExecutionContext& pec, std::chrono::microseconds delay);

    BufferLayout layout;
    std::chrono::microseconds maxDelay;
    size_t maxHeldRuns;
    std::mutex mutex;
    std::vector<Origin> origins;
    bool timerArmed = false;
};

}
