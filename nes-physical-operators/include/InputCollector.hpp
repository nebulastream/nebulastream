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
#include <cstdint>
#include <mutex>
#include <span>
#include <type_traits>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Holds small complete inputs as runs of adjacent sequence ranges, which a scan processes as one unit. `collect` returns the
/// unit in a thread-local slot that stays valid until the next `collect` or `releaseUnitProxy` on this thread.
class InputCollector final : public OperatorHandler
{
public:
    using Clock = std::chrono::steady_clock;

    /// Members hold raw values, since traced code reads them through `offsetof`.
    struct Unit
    {
        const TupleBuffer* buffers;
        uint64_t numberOfBuffers;
        OriginId::Underlying origin;
        SequenceNumber::Underlying sequenceNumber;
        ChunkNumber::Underlying chunkNumber;
        Timestamp::Underlying watermark;
        Timestamp::Underlying creation;
        uint32_t rangeOffset;
        bool lastChunk;
    };

    InputCollector(uint64_t capacity, std::chrono::microseconds maxDelay);

    void start(PipelineExecutionContext&) override { }

    void stop(QueryTerminationType terminationType, PipelineExecutionContext&) override;

    static constexpr size_t MAX_BUFFERS_PER_RUN = 64;
    static constexpr size_t MAX_HELD_BUFFERS = 512;

    /// Returns the unit to process, or nullptr if `input` is held.
    [[nodiscard]] const Unit* collect(const TupleBuffer& input);
    /// Collects the complete `input` as of `now`.
    [[nodiscard]] const Unit* collectAt(const TupleBuffer& input, Clock::time_point now);

    static const Unit* collectProxy(OperatorHandler* collector, const TupleBuffer* input);
    static void releaseUnitProxy() noexcept;
    /// A pipeline stage terminates its operators only in a graceful stop.
    static void stopProxy(OperatorHandler* collector, PipelineExecutionContext* pec);
    /// Repeats `input` if the last `collect` on this thread armed the timer. The task must end after this call.
    static void repeatIfArmedProxy(PipelineExecutionContext* pec, const TupleBuffer* input);

private:
    struct Run
    {
        SequenceNumber::Underlying first;
        SequenceNumber::Underlying last;
        std::vector<TupleBuffer> buffers;
        uint64_t tuples;
        Timestamp watermark;
        Timestamp creation;
        Clock::time_point deadline;
    };

    struct Origin
    {
        OriginId id;
        std::vector<Run> runs;
    };

    Origin& originFor(OriginId id);
    [[nodiscard]] bool moreThanHalfFull(uint64_t tuples) const;
    void absorb(Run& into, Origin& origin, size_t index);
    void takeRun(Origin& origin, size_t index);
    bool takeOldestExpired(std::span<Origin> candidates, Clock::time_point now);
    void takeLargestRun();
    [[nodiscard]] Clock::time_point earliestDeadline() const;
    /// Arms the timer while anything is held. The timer is a repeat of `carrier`, which `repeatIfArmedProxy` issues.
    void armIfNeeded(OriginId origin, SequenceNumber::Underlying carrier, Clock::time_point now);

    uint64_t capacity;
    std::chrono::microseconds maxDelay;
    /// Guards the members below, which the private functions access only with it held. `tickArmed` is also read without it.
    std::mutex mutex;
    std::vector<Origin> origins;
    size_t heldBuffers = 0;
    /// Written under `mutex`. False only while nothing is held, so `collect` may read it without the lock.
    std::atomic<bool> tickArmed = false;
    OriginId carrierOrigin = INVALID<OriginId>;
    SequenceNumber::Underlying carrierSequence = 0;
};

static_assert(std::is_standard_layout_v<InputCollector::Unit>, "Traced code reads `Unit` through `offsetof`");

}
