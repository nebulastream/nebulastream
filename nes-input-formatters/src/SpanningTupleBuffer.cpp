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

#include <SpanningTupleBuffer.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

SpanningTupleBuffer::SpanningTupleBuffer()
{
    /// Sequence zero is the virtual delimiter before the first raw byte.
    entries.emplace(0, Entry{StagedBuffer{}, StagedBuffer{}, true, true, false});
}

bool SpanningTupleBuffer::isEmptySequence(const uint64_t sequenceNumber) const
{
    const auto next = emptyRanges.upper_bound(sequenceNumber);
    return next != emptyRanges.begin() and std::prev(next)->second >= sequenceNumber;
}

bool SpanningTupleBuffer::insertEmptySequence(const uint64_t sequenceNumber)
{
    if (entries.contains(sequenceNumber) or isEmptySequence(sequenceNumber))
    {
        return false;
    }

    auto first = sequenceNumber;
    auto last = sequenceNumber;
    auto next = emptyRanges.lower_bound(sequenceNumber);
    if (next != emptyRanges.begin())
    {
        const auto previous = std::prev(next);
        if (previous->second + 1 == sequenceNumber)
        {
            first = previous->first;
            emptyRanges.erase(previous);
        }
    }
    if (next != emptyRanges.end() and sequenceNumber != std::numeric_limits<uint64_t>::max() and next->first == sequenceNumber + 1)
    {
        last = next->second;
        emptyRanges.erase(next);
    }
    emptyRanges.emplace(first, last);
    return true;
}

bool SpanningTupleBuffer::insertBuffer(const uint64_t sequenceNumber, const StagedBuffer& indexedRawBuffer, const bool hasDelimiter)
{
    if (entries.contains(sequenceNumber) or isEmptySequence(sequenceNumber))
    {
        return false;
    }
    entries.emplace(sequenceNumber, Entry{indexedRawBuffer, indexedRawBuffer, hasDelimiter, false, false});
    return true;
}

std::optional<uint64_t> SpanningTupleBuffer::findPreviousDelimiter(const uint64_t sequenceNumber) const
{
    if (sequenceNumber == 0)
    {
        return std::nullopt;
    }
    auto cursor = sequenceNumber - 1;
    while (true)
    {
        auto nextEmpty = emptyRanges.upper_bound(cursor);
        if (nextEmpty != emptyRanges.begin())
        {
            const auto empty = std::prev(nextEmpty);
            if (empty->second >= cursor)
            {
                if (empty->first == 0)
                {
                    return std::nullopt;
                }
                cursor = empty->first - 1;
                continue;
            }
        }
        const auto entry = entries.find(cursor);
        if (entry == entries.end())
        {
            return std::nullopt;
        }
        if (entry->second.hasDelimiter)
        {
            return cursor;
        }
        if (cursor == 0)
        {
            return std::nullopt;
        }
        --cursor;
    }
}

std::optional<uint64_t> SpanningTupleBuffer::findNextDelimiter(const uint64_t sequenceNumber) const
{
    if (sequenceNumber == std::numeric_limits<uint64_t>::max())
    {
        return std::nullopt;
    }
    auto cursor = sequenceNumber + 1;
    while (true)
    {
        auto nextEmpty = emptyRanges.upper_bound(cursor);
        if (nextEmpty != emptyRanges.begin())
        {
            const auto empty = std::prev(nextEmpty);
            if (empty->second >= cursor)
            {
                if (empty->second == std::numeric_limits<uint64_t>::max())
                {
                    return std::nullopt;
                }
                cursor = empty->second + 1;
                continue;
            }
        }
        const auto entry = entries.find(cursor);
        if (entry == entries.end())
        {
            return std::nullopt;
        }
        if (entry->second.hasDelimiter)
        {
            return cursor;
        }
        if (cursor == std::numeric_limits<uint64_t>::max())
        {
            return std::nullopt;
        }
        ++cursor;
    }
}

void SpanningTupleBuffer::eraseEmptySequences(const uint64_t firstSequenceNumber, const uint64_t lastSequenceNumber)
{
    if (firstSequenceNumber > lastSequenceNumber)
    {
        return;
    }
    auto interval = emptyRanges.upper_bound(firstSequenceNumber);
    if (interval != emptyRanges.begin())
    {
        --interval;
    }
    std::vector<std::pair<uint64_t, uint64_t>> remaining;
    while (interval != emptyRanges.end() and interval->first <= lastSequenceNumber)
    {
        if (interval->second < firstSequenceNumber)
        {
            ++interval;
            continue;
        }
        if (interval->first < firstSequenceNumber)
        {
            remaining.emplace_back(interval->first, firstSequenceNumber - 1);
        }
        if (interval->second > lastSequenceNumber)
        {
            remaining.emplace_back(lastSequenceNumber + 1, interval->second);
        }
        interval = emptyRanges.erase(interval);
    }
    for (const auto& [first, last] : remaining)
    {
        emptyRanges.emplace(first, last);
    }
}

SpanningBuffers SpanningTupleBuffer::claimSpan(const uint64_t firstSequenceNumber, const uint64_t lastSequenceNumber)
{
    const auto first = entries.find(firstSequenceNumber);
    const auto last = entries.find(lastSequenceNumber);
    if (first == entries.end() or last == entries.end() or first->second.trailingUsed or last->second.leadingUsed)
    {
        return {};
    }
    INVARIANT(first->second.hasDelimiter and last->second.hasDelimiter, "A spanning tuple must have delimiters at both ends");

    std::vector<StagedBuffer> spanningBuffers;
    spanningBuffers.emplace_back(std::move(first->second.trailing));
    first->second.trailingUsed = true;

    auto cursor = firstSequenceNumber + 1;
    while (cursor < lastSequenceNumber)
    {
        const auto nextEmpty = emptyRanges.upper_bound(cursor);
        if (nextEmpty != emptyRanges.begin() and std::prev(nextEmpty)->second >= cursor)
        {
            const auto empty = std::prev(nextEmpty);
            spanningBuffers.emplace_back();
            cursor = empty->second + 1;
            continue;
        }
        const auto entry = entries.find(cursor);
        INVARIANT(entry != entries.end() and not entry->second.hasDelimiter, "A spanning tuple has a missing intermediate buffer");
        spanningBuffers.emplace_back(std::move(entry->second.leading));
        entries.erase(entry);
        ++cursor;
    }

    spanningBuffers.emplace_back(std::move(last->second.leading));
    last->second.leadingUsed = true;
    eraseEmptySequences(firstSequenceNumber + 1, lastSequenceNumber - 1);
    if (first->second.leadingUsed)
    {
        entries.erase(first);
    }
    if (last->second.trailingUsed)
    {
        entries.erase(last);
    }
    return SpanningBuffers(std::move(spanningBuffers));
}

SequenceShredderResult SpanningTupleBuffer::tryFindLeadingSpanningTupleForBufferWithDelimiter(
    const SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    const std::scoped_lock lock(mutex);
    const auto rawSequenceNumber = sequenceNumber.getRawValue();
    if (not insertBuffer(rawSequenceNumber, indexedRawBuffer, true))
    {
        return SequenceShredderResult{.isInRange = false, .spanningBuffers = {}};
    }
    if (const auto previousDelimiter = findPreviousDelimiter(rawSequenceNumber))
    {
        auto spanningBuffers = claimSpan(*previousDelimiter, rawSequenceNumber);
        if (spanningBuffers.hasSpanningTuple())
        {
            return SequenceShredderResult{.isInRange = true, .spanningBuffers = std::move(spanningBuffers)};
        }
    }
    return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers({indexedRawBuffer})};
}

SpanningBuffers SpanningTupleBuffer::tryFindTrailingSpanningTupleForBufferWithDelimiter(const SequenceNumber sequenceNumber)
{
    const std::scoped_lock lock(mutex);
    const auto rawSequenceNumber = sequenceNumber.getRawValue();
    if (const auto nextDelimiter = findNextDelimiter(rawSequenceNumber))
    {
        return claimSpan(rawSequenceNumber, *nextDelimiter);
    }
    return {};
}

SequenceShredderResult SpanningTupleBuffer::tryFindSpanningTupleForBufferWithoutDelimiter(
    const SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    const std::scoped_lock lock(mutex);
    const auto rawSequenceNumber = sequenceNumber.getRawValue();
    const auto inserted = indexedRawBuffer.getSizeOfBufferInBytes() == 0 ? insertEmptySequence(rawSequenceNumber)
                                                                         : insertBuffer(rawSequenceNumber, indexedRawBuffer, false);
    if (not inserted)
    {
        return SequenceShredderResult{.isInRange = false, .spanningBuffers = {}};
    }
    const auto previousDelimiter = findPreviousDelimiter(rawSequenceNumber);
    const auto nextDelimiter = findNextDelimiter(rawSequenceNumber);
    if (previousDelimiter and nextDelimiter)
    {
        auto spanningBuffers = claimSpan(*previousDelimiter, *nextDelimiter);
        if (spanningBuffers.hasSpanningTuple())
        {
            return SequenceShredderResult{.isInRange = true, .spanningBuffers = std::move(spanningBuffers)};
        }
    }
    return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers({indexedRawBuffer})};
}

bool SpanningTupleBuffer::validate() const
{
    const std::scoped_lock lock(mutex);
    for (const auto& [sequenceNumber, entry] : entries)
    {
        if (entry.hasDelimiter)
        {
            if ((not entry.leadingUsed and findPreviousDelimiter(sequenceNumber))
                or (not entry.trailingUsed and findNextDelimiter(sequenceNumber)))
            {
                return false;
            }
        }
        else if (findPreviousDelimiter(sequenceNumber) and findNextDelimiter(sequenceNumber))
        {
            return false;
        }
    }
    return true;
}

std::ostream& operator<<(std::ostream& os, const SpanningTupleBuffer& sequenceRingBuffer)
{
    const std::scoped_lock lock(sequenceRingBuffer.mutex);
    return os << "SpanningTupleBuffer(pending=" << sequenceRingBuffer.entries.size()
              << ", emptyRanges=" << sequenceRingBuffer.emptyRanges.size() << ')';
}

}
