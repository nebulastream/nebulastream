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

#include <InputCollector.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
/// `unit.buffers` points into `unitBuffers`, which owns the handles.
thread_local InputCollector::Unit unit;
thread_local std::vector<TupleBuffer> unitBuffers;
thread_local std::optional<std::chrono::microseconds> repeatDelay;

const InputCollector::Unit* publishUnit()
{
    unit.buffers = unitBuffers.data();
    unit.numberOfBuffers = unitBuffers.size();
    return &unit;
}

/// An input without a watermark keeps none only if what it merges with has none either. `INVALID_VALUE` orders above every
/// watermark, the end-of-stream one included, so `std::max` would drop it.
Timestamp laterWatermark(const Timestamp lhs, const Timestamp rhs)
{
    constexpr Timestamp none{Timestamp::INVALID_VALUE};
    if (lhs == none)
    {
        return rhs;
    }
    if (rhs == none)
    {
        return lhs;
    }
    return std::max(lhs, rhs);
}

const InputCollector::Unit* unitOf(const TupleBuffer& input)
{
    repeatDelay.reset();
    unitBuffers.clear();
    unitBuffers.push_back(input);
    unit.origin = input.getOriginId().getRawValue();
    unit.sequenceNumber = input.getSequenceNumber().getRawValue();
    unit.rangeOffset = input.getSequenceRangeOffset();
    unit.chunkNumber = input.getChunkNumber().getRawValue();
    unit.lastChunk = input.isLastChunk();
    unit.watermark = input.getWatermark().getRawValue();
    unit.creation = input.getCreationTimestampInMS().getRawValue();
    return publishUnit();
}
}

InputCollector::InputCollector(const uint64_t capacity, const std::chrono::microseconds maxDelay) : capacity(capacity), maxDelay(maxDelay)
{
}

const InputCollector::Unit* InputCollector::collect(const TupleBuffer& input)
{
    /// A run tracks whole sequence ranges, so a chunked input passes through.
    const bool complete = input.getChunkNumber() == INITIAL_CHUNK_NUMBER && input.isLastChunk();
    if (!complete || (moreThanHalfFull(input.getNumberOfTuples()) && !tickArmed.load(std::memory_order::acquire)))
    {
        return unitOf(input);
    }
    return collectAt(input, Clock::now());
}

const InputCollector::Unit* InputCollector::collectAt(const TupleBuffer& input, const Clock::time_point now)
{
    PRECONDITION(input.getChunkNumber() == INITIAL_CHUNK_NUMBER && input.isLastChunk(), "Only a complete input can be held");
    /// A task that failed after its `collect` leaves a stale delay behind.
    repeatDelay.reset();
    const auto last = input.getSequenceNumber().getRawValue();
    const auto originId = input.getOriginId();
    const auto tuples = input.getNumberOfTuples();
    const std::scoped_lock lock(mutex);
    /// The repeated carrier is the timer tick. Its records were held or processed when it first arrived.
    if (tickArmed.load(std::memory_order::relaxed) && carrierOrigin == originId && carrierSequence == last)
    {
        tickArmed.store(false, std::memory_order::relaxed);
        const bool took = takeOldestExpired(origins, now);
        armIfNeeded(originId, last, now);
        return took ? publishUnit() : nullptr;
    }

    auto& origin = originFor(originId);
    auto& runs = origin.runs;
    Run current{
        .first = last - input.getSequenceRangeOffset(),
        .last = last,
        .buffers = {},
        .tuples = tuples,
        .watermark = input.getWatermark(),
        .creation = input.getCreationTimestampInMS(),
        .deadline = now + maxDelay};
    if (tuples > 0)
    {
        current.buffers.push_back(input);
    }

    auto position = static_cast<size_t>(std::ranges::upper_bound(runs, current.last, {}, &Run::first) - runs.begin());
    INVARIANT(position == 0 || runs[position - 1].last < current.first, "Range of origin {} overlaps a held range", originId);
    /// A buffer stores its range as an offset of 32 bits.
    const auto withinOffset = [](const Run& lhs, const Run& rhs)
    { return std::max(lhs.last, rhs.last) - std::min(lhs.first, rhs.first) <= std::numeric_limits<uint32_t>::max(); };
    bool merged = false;
    if (position > 0 && runs[position - 1].last + 1 == current.first && withinOffset(runs[position - 1], current))
    {
        --position;
        absorb(current, origin, position);
        merged = true;
    }
    if (position < runs.size() && runs[position].first == current.last + 1 && withinOffset(runs[position], current))
    {
        absorb(current, origin, position);
        merged = true;
    }

    bool took = false;
    if (current.tuples >= capacity || current.buffers.size() >= MAX_BUFFERS_PER_RUN || (!merged && moreThanHalfFull(current.tuples)))
    {
        runs.insert(runs.begin() + static_cast<std::ptrdiff_t>(position), std::move(current));
        heldBuffers += runs[position].buffers.size();
        takeRun(origin, position);
        took = true;
    }
    else
    {
        heldBuffers += current.buffers.size();
        runs.insert(runs.begin() + static_cast<std::ptrdiff_t>(position), std::move(current));
        if (heldBuffers > MAX_HELD_BUFFERS)
        {
            takeLargestRun();
            took = true;
        }
        else
        {
            took = takeOldestExpired(std::span(&origin, 1), now);
        }
    }
    armIfNeeded(originId, last, now);
    return took ? publishUnit() : nullptr;
}

void InputCollector::stop([[maybe_unused]] const QueryTerminationType terminationType, PipelineExecutionContext&)
{
    const std::scoped_lock lock(mutex);
    /// A pipeline stops gracefully only after its last timer tick, which re-arms while anything is held.
    INVARIANT(
        terminationType != QueryTerminationType::Graceful || earliestDeadline() == Clock::time_point::max(),
        "A graceful stop found held runs");
}

InputCollector::Origin& InputCollector::originFor(const OriginId id)
{
    const auto found = std::ranges::find(origins, id, &Origin::id);
    return found != origins.end() ? *found : origins.emplace_back(Origin{.id = id, .runs = {}});
}

void InputCollector::takeRun(Origin& origin, const size_t index)
{
    auto& run = origin.runs[index];
    heldBuffers -= run.buffers.size();
    unitBuffers = std::move(run.buffers);
    unit.origin = origin.id.getRawValue();
    unit.sequenceNumber = run.last;
    unit.rangeOffset = static_cast<uint32_t>(run.last - run.first);
    unit.chunkNumber = INITIAL_CHUNK_NUMBER.getRawValue();
    unit.lastChunk = true;
    unit.watermark = run.watermark.getRawValue();
    unit.creation = run.creation.getRawValue();
    origin.runs.erase(origin.runs.begin() + static_cast<std::ptrdiff_t>(index));
}

void InputCollector::absorb(Run& into, Origin& origin, const size_t index)
{
    auto& from = origin.runs[index];
    heldBuffers -= from.buffers.size();
    if (from.buffers.size() > into.buffers.size())
    {
        std::swap(into.buffers, from.buffers);
    }
    into.buffers.insert(into.buffers.end(), std::make_move_iterator(from.buffers.begin()), std::make_move_iterator(from.buffers.end()));
    into.first = std::min(into.first, from.first);
    into.last = std::max(into.last, from.last);
    into.tuples += from.tuples;
    into.watermark = laterWatermark(into.watermark, from.watermark);
    into.creation = std::min(into.creation, from.creation);
    into.deadline = std::min(into.deadline, from.deadline);
    origin.runs.erase(origin.runs.begin() + static_cast<std::ptrdiff_t>(index));
}

void InputCollector::takeLargestRun()
{
    Origin* largestOrigin = nullptr;
    size_t largestIndex = 0;
    size_t largestSize = 0;
    for (auto& origin : origins)
    {
        for (size_t index = 0; index < origin.runs.size(); ++index)
        {
            if (origin.runs[index].buffers.size() > largestSize)
            {
                largestOrigin = &origin;
                largestIndex = index;
                largestSize = origin.runs[index].buffers.size();
            }
        }
    }
    INVARIANT(largestOrigin != nullptr, "Held buffers exceed the cap, but no run holds a buffer");
    takeRun(*largestOrigin, largestIndex);
}

bool InputCollector::moreThanHalfFull(const uint64_t tuples) const
{
    return 2 * tuples > capacity;
}

bool InputCollector::takeOldestExpired(const std::span<Origin> candidates, const Clock::time_point now)
{
    Origin* oldestOrigin = nullptr;
    size_t oldestIndex = 0;
    auto oldestDeadline = now;
    for (auto& origin : candidates)
    {
        for (size_t index = 0; index < origin.runs.size(); ++index)
        {
            if (origin.runs[index].deadline <= oldestDeadline)
            {
                oldestOrigin = &origin;
                oldestIndex = index;
                oldestDeadline = origin.runs[index].deadline;
            }
        }
    }
    if (oldestOrigin == nullptr)
    {
        return false;
    }
    takeRun(*oldestOrigin, oldestIndex);
    return true;
}

InputCollector::Clock::time_point InputCollector::earliestDeadline() const
{
    auto earliest = Clock::time_point::max();
    for (const auto& origin : origins)
    {
        for (const auto& run : origin.runs)
        {
            earliest = std::min(earliest, run.deadline);
        }
    }
    return earliest;
}

void InputCollector::armIfNeeded(const OriginId origin, const SequenceNumber::Underlying carrier, const Clock::time_point now)
{
    if (tickArmed.load(std::memory_order::relaxed))
    {
        return;
    }
    const auto earliest = earliestDeadline();
    if (earliest == Clock::time_point::max())
    {
        return;
    }
    tickArmed.store(true, std::memory_order::release);
    carrierOrigin = origin;
    carrierSequence = carrier;
    repeatDelay = std::clamp(std::chrono::ceil<std::chrono::microseconds>(earliest - now), std::chrono::microseconds::zero(), maxDelay);
}

const InputCollector::Unit* InputCollector::collectProxy(OperatorHandler* collector, const TupleBuffer* input)
{
    return static_cast<InputCollector*>(collector)->collect(*input);
}

void InputCollector::releaseUnitProxy() noexcept
{
    unitBuffers.clear();
}

void InputCollector::stopProxy(OperatorHandler* collector, PipelineExecutionContext* pec)
{
    static_cast<InputCollector*>(collector)->stop(QueryTerminationType::Graceful, *pec);
}

void InputCollector::repeatIfArmedProxy(PipelineExecutionContext* pec, const TupleBuffer* input)
{
    if (repeatDelay)
    {
        const auto delay = *repeatDelay;
        repeatDelay.reset();
        pec->repeatTask(*input, delay);
    }
}

}
