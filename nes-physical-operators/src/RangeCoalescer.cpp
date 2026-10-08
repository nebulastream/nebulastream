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

#include <RangeCoalescer.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

RangeCoalescer::RangeCoalescer(const uint64_t capacity, const std::chrono::microseconds maxDelay, const size_t maxHeldRuns, Merge merge)
    : capacity(capacity), maxDelay(maxDelay), maxHeldRuns(maxHeldRuns), merge(std::move(merge))
{
    PRECONDITION(maxDelay > std::chrono::microseconds::zero(), "A coalescer needs a positive delay");
    PRECONDITION(maxHeldRuns > 0, "A coalescer needs room for at least one run per origin");
    PRECONDITION(this->merge != nullptr, "A coalescer needs a merge");
}

void RangeCoalescer::offer(const TupleBuffer& output, PipelineExecutionContext& pec, const Clock::time_point now)
{
    const auto last = output.getSequenceNumber().getRawValue();
    Run current{
        .first = last - output.getSequenceRangeOffset(),
        .last = last,
        .buffer = output,
        .watermark = output.getWatermark(),
        .creation = output.getCreationTimestampInMS(),
        .deadline = now + maxDelay};

    std::vector<TupleBuffer> outgoing;
    std::optional<std::chrono::microseconds> timerDelay;
    {
        const std::scoped_lock lock(mutex);
        auto& origin = originFor(output.getOriginId());
        place(origin, std::move(current), outgoing);
        if (origin.runs.size() > maxHeldRuns)
        {
            queueForEmit(origin.runs.front(), outgoing);
            origin.runs.erase(origin.runs.begin());
        }
        takeExpired(origin, now, outgoing);
        timerDelay = armIfNeeded(now);
    }
    emit(outgoing, pec);
    if (timerDelay)
    {
        schedule(pec, *timerDelay);
    }
}

void RangeCoalescer::place(Origin& origin, Run&& current, std::vector<TupleBuffer>& outgoing) const
{
    auto& runs = origin.runs;
    /// Runs are disjoint and sorted, so only `runs[position - 1]` can overlap or adjoin `current` from the left.
    auto position = static_cast<size_t>(std::ranges::upper_bound(runs, current.last, {}, &Run::first) - runs.begin());
    INVARIANT(
        position == 0 || runs[position - 1].last < current.first,
        "Range [{}, {}] of origin {} overlaps a held range",
        current.first,
        current.last,
        origin.id);
    const auto adjoinsLeft = [&] { return position > 0 && runs[position - 1].last + 1 == current.first; };
    const auto adjoinsRight = [&] { return position < runs.size() && runs[position].first == current.last + 1; };
    const auto erase = [&](const size_t index) { runs.erase(runs.begin() + static_cast<std::ptrdiff_t>(index)); };

    bool merged = false;
    if (adjoinsLeft() && fits(runs[position - 1], current))
    {
        --position;
        absorb(current, runs[position]);
        erase(position);
        merged = true;
    }
    if (adjoinsRight() && fits(runs[position], current))
    {
        absorb(current, runs[position]);
        erase(position);
        merged = true;
    }
    if (current.buffer.getNumberOfTuples() >= capacity)
    {
        queueForEmit(current, outgoing);
        return;
    }

    /// A run that still adjoins `current` could not merge with it. The fuller side leaves, the run on a tie.
    if (adjoinsLeft())
    {
        if (runs[position - 1].buffer.getNumberOfTuples() < current.buffer.getNumberOfTuples())
        {
            queueForEmit(current, outgoing);
            return;
        }
        --position;
        queueForEmit(runs[position], outgoing);
        erase(position);
    }
    if (adjoinsRight())
    {
        if (runs[position].buffer.getNumberOfTuples() < current.buffer.getNumberOfTuples())
        {
            queueForEmit(current, outgoing);
            return;
        }
        queueForEmit(runs[position], outgoing);
        erase(position);
    }

    if (!merged && 2 * current.buffer.getNumberOfTuples() > capacity)
    {
        queueForEmit(current, outgoing);
        return;
    }
    runs.insert(runs.begin() + static_cast<std::ptrdiff_t>(position), std::move(current));
}

void RangeCoalescer::flushAll(PipelineExecutionContext& pec)
{
    std::vector<TupleBuffer> outgoing;
    {
        const std::scoped_lock lock(mutex);
        takeAllExpired(Clock::time_point::max(), outgoing);
    }
    emit(outgoing, pec);
}

void RangeCoalescer::onTimer(PipelineExecutionContext& pec, const Clock::time_point now)
{
    std::vector<TupleBuffer> outgoing;
    std::optional<std::chrono::microseconds> timerDelay;
    {
        const std::scoped_lock lock(mutex);
        timerArmed = false;
        takeAllExpired(now, outgoing);
        timerDelay = armIfNeeded(now);
    }
    emit(outgoing, pec);
    if (timerDelay)
    {
        schedule(pec, *timerDelay);
    }
}

RangeCoalescer::Origin& RangeCoalescer::originFor(const OriginId id)
{
    const auto found = std::ranges::find(origins, id, &Origin::id);
    return found != origins.end() ? *found : origins.emplace_back(Origin{.id = id, .runs = {}});
}

bool RangeCoalescer::fits(const Run& lhs, const Run& rhs) const
{
    /// A buffer stores its range as an offset of 32 bits.
    return lhs.buffer.getNumberOfTuples() + rhs.buffer.getNumberOfTuples() <= capacity
        && std::max(lhs.last, rhs.last) - std::min(lhs.first, rhs.first) <= std::numeric_limits<uint32_t>::max();
}

void RangeCoalescer::absorb(Run& into, Run& from) const
{
    const bool keepFrom = from.buffer.getNumberOfTuples() > into.buffer.getNumberOfTuples();
    auto& target = keepFrom ? from.buffer : into.buffer;
    const auto& source = keepFrom ? into.buffer : from.buffer;
    /// Merging variable sized data may allocate child buffers while `mutex` is held.
    if (source.getNumberOfTuples() > 0)
    {
        merge(target, source);
    }
    if (keepFrom)
    {
        into.buffer = std::move(from.buffer);
    }
    into.first = std::min(into.first, from.first);
    into.last = std::max(into.last, from.last);
    into.watermark = std::max(into.watermark, from.watermark);
    into.creation = std::min(into.creation, from.creation);
    into.deadline = std::min(into.deadline, from.deadline);
}

void RangeCoalescer::queueForEmit(Run& run, std::vector<TupleBuffer>& outgoing)
{
    auto& buffer = run.buffer;
    buffer.setSequenceRange(SequenceNumber(run.last), static_cast<uint32_t>(run.last - run.first));
    buffer.setChunkNumber(INITIAL_CHUNK_NUMBER);
    buffer.setLastChunk(true);
    buffer.setWatermark(run.watermark);
    buffer.setCreationTimestampInMS(run.creation);
    outgoing.push_back(std::move(buffer));
}

void RangeCoalescer::takeExpired(Origin& origin, const Clock::time_point now, std::vector<TupleBuffer>& outgoing)
{
    const auto expired = [now](const Run& run) { return run.deadline <= now; };
    for (auto& run : origin.runs | std::views::filter(expired))
    {
        queueForEmit(run, outgoing);
    }
    std::erase_if(origin.runs, expired);
}

void RangeCoalescer::takeAllExpired(const Clock::time_point now, std::vector<TupleBuffer>& outgoing)
{
    for (auto& origin : origins)
    {
        takeExpired(origin, now, outgoing);
    }
}

std::optional<std::chrono::microseconds> RangeCoalescer::armIfNeeded(const Clock::time_point now)
{
    if (timerArmed)
    {
        return std::nullopt;
    }
    auto earliest = Clock::time_point::max();
    for (const auto& origin : origins)
    {
        for (const auto& run : origin.runs)
        {
            earliest = std::min(earliest, run.deadline);
        }
    }
    if (earliest == Clock::time_point::max())
    {
        return std::nullopt;
    }
    timerArmed = true;
    return std::clamp(std::chrono::ceil<std::chrono::microseconds>(earliest - now), std::chrono::microseconds::zero(), maxDelay);
}

void RangeCoalescer::schedule(PipelineExecutionContext& pec, const std::chrono::microseconds delay)
{
    pec.scheduleCallback(delay, [this](PipelineExecutionContext& callbackContext) { onTimer(callbackContext, Clock::now()); });
}

void RangeCoalescer::emit(const std::vector<TupleBuffer>& outgoing, PipelineExecutionContext& pec)
{
    for (const auto& buffer : outgoing)
    {
        pec.emitBuffer(buffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}

}
