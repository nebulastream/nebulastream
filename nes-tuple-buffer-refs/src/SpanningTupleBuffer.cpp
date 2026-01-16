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

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <span>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>
#include <SpanningTupleBufferState.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

SpanningTupleBuffer::SpanningTupleBuffer(const size_t initialSize, TupleBuffer dummyBuffer)
    : buffer(std::vector<SpanningTupleBufferEntry>(initialSize))
{
    PRECONDITION(initialSize > 0, "Constructing an SpanningTupleBuffer with an initial size of 0 is not allowed");
    buffer.at(0).setStateOfFirstIndex(std::move(dummyBuffer));
}

SpanningTupleBuffer::WithoutDelimiterSearchResult
SpanningTupleBuffer::searchAndTryClaimWithoutDelimiter(const SequenceNumber sequenceNumber)
{
    const auto [sequenceNumberBufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
    if (const auto leadingDistance = searchLeading(sequenceNumberBufferIdx, abaItNumber))
    {
        const auto stStartSN = SequenceNumber{sequenceNumber.getRawValue() - leadingDistance.value()};
        if (auto [startBuffer, stEndSN] = claimingTrailingDelimiterSearch(stStartSN, sequenceNumber); startBuffer.has_value())
        {
            return WithoutDelimiterSearchResult{
                .leadingSpanningTupleStartBuffer = std::move(startBuffer), .firstSequenceNumber = stStartSN, .lastSequenceNumber = stEndSN};
        }
    }
    return WithoutDelimiterSearchResult{};
}

void SpanningTupleBuffer::claimSpanningTupleBuffers(const SequenceNumber stStartSn, const std::span<StagedBuffer> spanningTupleBuffers)
{
    const auto lastOffset = spanningTupleBuffers.size() - 1;
    for (size_t offset = 1; offset < lastOffset; ++offset)
    {
        const auto nextBufferIdx = (stStartSn.getRawValue() + offset) % buffer.size();
        buffer[nextBufferIdx].claimNoDelimiterBuffer(spanningTupleBuffers, offset);
    }
    const auto lastBufferIdx = (stStartSn.getRawValue() + lastOffset) % buffer.size();
    buffer[lastBufferIdx].claimLeadingBuffer(spanningTupleBuffers, lastOffset);
}

SequenceShredderResult SpanningTupleBuffer::tryFindLeadingSpanningTupleForBufferWithDelimiter(
    const SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    /// Try to set 'indexedRawBuffer' at corresponding index in buffer. Return as 'out-of-range' buffer, if setting the buffer fails
    if (const auto [bufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
        not buffer[bufferIdx.getRawValue()].trySetWithDelimiter(abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .spanningBuffers = {}};
    }

    /// claimingLeadingDelimiterSearch checks if valid trailing delimiter offset
    auto [firstSpanningTupleStartBuffer, firstSpanningTupleStartSN] = claimingLeadingDelimiterSearch(sequenceNumber);
    if (firstSpanningTupleStartBuffer.has_value())
    {
        const auto sizeOfSpanningTuple = sequenceNumber.getRawValue() - firstSpanningTupleStartSN.getRawValue() + 1;
        std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
        spanningTupleBuffers[0] = std::move(firstSpanningTupleStartBuffer.value());
        claimSpanningTupleBuffers(firstSpanningTupleStartSN, spanningTupleBuffers);
        return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers(std::move(spanningTupleBuffers))};
    }
    return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers({indexedRawBuffer})};
}

SpanningBuffers SpanningTupleBuffer::tryFindTrailingSpanningTupleForBufferWithDelimiter(const SequenceNumber sequenceNumber)
{
    auto [firstSpanningTupleStartBuffer, lastSN] = claimingTrailingDelimiterSearch(sequenceNumber);
    if (firstSpanningTupleStartBuffer.has_value())
    {
        const auto sizeOfSpanningTuple = lastSN.getRawValue() - sequenceNumber.getRawValue() + 1;
        std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
        spanningTupleBuffers[0] = std::move(firstSpanningTupleStartBuffer.value());
        claimSpanningTupleBuffers(sequenceNumber, spanningTupleBuffers);
        return SpanningBuffers(std::move(spanningTupleBuffers));
    }
    return SpanningBuffers{};
}

SpanningBuffers SpanningTupleBuffer::tryFindTrailingSpanningTupleForBufferWithDelimiter(
    const SequenceNumber sequenceNumber, const FieldIndex offsetOfLastTuple)
{
    const auto [sequenceNumberBufferIdx, _] = getBufferIdxAndABAItNo(sequenceNumber);
    this->buffer.at(sequenceNumberBufferIdx.getRawValue()).setOffsetOfTrailingSpanningTuple(offsetOfLastTuple);

    auto [firstSpanningTupleStartBuffer, lastSN] = claimingTrailingDelimiterSearch(sequenceNumber);
    if (firstSpanningTupleStartBuffer.has_value())
    {
        const auto sizeOfSpanningTuple = lastSN.getRawValue() - sequenceNumber.getRawValue() + 1;
        std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
        spanningTupleBuffers[0] = std::move(firstSpanningTupleStartBuffer.value());
        claimSpanningTupleBuffers(sequenceNumber, spanningTupleBuffers);
        return SpanningBuffers(std::move(spanningTupleBuffers));
    }
    return SpanningBuffers{};
}

SequenceShredderResult SpanningTupleBuffer::tryFindSpanningTupleForBufferWithoutDelimiter(
    const SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    /// Try to set 'indexedRawBuffer' at corresponding index in buffer. Return as 'out-of-range' buffer, if setting the buffer fails
    if (const auto [bufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
        not buffer[bufferIdx.getRawValue()].trySetWithoutDelimiter(abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .spanningBuffers = {}};
    }
    if (auto searchAndClaimResult = searchAndTryClaimWithoutDelimiter(sequenceNumber);
        searchAndClaimResult.leadingSpanningTupleStartBuffer.has_value())
    {
        const auto sizeOfSpanningTuple
            = searchAndClaimResult.lastSequenceNumber.getRawValue() - searchAndClaimResult.firstSequenceNumber.getRawValue() + 1;
        std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
        INVARIANT(
            searchAndClaimResult.leadingSpanningTupleStartBuffer.has_value(), "A leading spanning tuple must have start buffer value");
        spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSpanningTupleStartBuffer.value());
        claimSpanningTupleBuffers(searchAndClaimResult.firstSequenceNumber, spanningTupleBuffers);
        return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers(std::move(spanningTupleBuffers))};
    }
    return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers({indexedRawBuffer})};
}

bool SpanningTupleBuffer::validate() const
{
    bool isValid = true;
    const auto lastIdxOfRB = buffer.size() - 1;
    for (const auto& [idx, metaData] : buffer | NES::views::enumerate)
    {
        isValid &= metaData.validateFinalState(
            static_cast<SpanningTupleBufferIdx>(idx),
            buffer.at((idx + 1) % buffer.size()),
            static_cast<SpanningTupleBufferIdx>(lastIdxOfRB));
    }
    return isValid;
}

std::ostream& operator<<(std::ostream& os, const SpanningTupleBuffer& sequenceRingBuffer)
{
    return os << fmt::format("RingBuffer({})", sequenceRingBuffer.buffer.size());
}

std::pair<SpanningTupleBufferIdx, ABAItNo> SpanningTupleBuffer::getBufferIdxAndABAItNo(const SequenceNumber sequenceNumber) const
{
    const auto sequenceNumberBufferIdx = SpanningTupleBufferIdx{static_cast<uint32_t>(sequenceNumber.getRawValue() % buffer.size())};
    const auto abaItNumber = static_cast<ABAItNo>((sequenceNumber.getRawValue() / buffer.size()) + 1);
    return std::make_pair(sequenceNumberBufferIdx, abaItNumber);
}

std::optional<size_t> SpanningTupleBuffer::searchLeading(const SpanningTupleBufferIdx searchStartBufferIdx, const ABAItNo abaItNumber) const
{
    const auto searchStartBufferIdxV = searchStartBufferIdx.getRawValue();
    size_t leadingDistance = 1;
    auto isPriorIteration = searchStartBufferIdxV < leadingDistance;
    auto entryState = this->buffer[(searchStartBufferIdxV - leadingDistance) % buffer.size()].getEntryState(
        static_cast<ABAItNo>(abaItNumber.getRawValue() - static_cast<size_t>(isPriorIteration)));
    while (entryState.hasCorrectABA and not entryState.hasValidTrailingDelimiterOffset and not(entryState.hasDelimiter))
    {
        ++leadingDistance;
        isPriorIteration = searchStartBufferIdxV < leadingDistance;
        entryState = this->buffer[(searchStartBufferIdxV - leadingDistance) % buffer.size()].getEntryState(
            static_cast<ABAItNo>(abaItNumber.getRawValue() - static_cast<size_t>(isPriorIteration)));
    }
    return (entryState.hasCorrectABA) ? std::optional{leadingDistance} : std::nullopt;
}

std::optional<size_t>
SpanningTupleBuffer::searchTrailing(const SpanningTupleBufferIdx searchStartBufferIdx, const ABAItNo abaItNumber) const
{
    const auto searchStartBufferIdxV = searchStartBufferIdx.getRawValue();
    size_t trailingDistance = 1;
    auto isNextIteration = (searchStartBufferIdxV + trailingDistance) >= buffer.size();
    auto entryState = this->buffer[(searchStartBufferIdxV + trailingDistance) % buffer.size()].getEntryState(
        static_cast<ABAItNo>(abaItNumber.getRawValue() + static_cast<size_t>(isNextIteration)));
    while (entryState.hasCorrectABA and not(entryState.hasDelimiter))
    {
        ++trailingDistance;
        isNextIteration = (searchStartBufferIdxV + trailingDistance) >= buffer.size();
        entryState = this->buffer[(searchStartBufferIdxV + trailingDistance) % buffer.size()].getEntryState(
            static_cast<ABAItNo>(abaItNumber.getRawValue() + static_cast<size_t>(isNextIteration)));
    }
    return (entryState.hasCorrectABA) ? std::optional{trailingDistance} : std::nullopt;
}

SpanningTupleBuffer::ClaimedSpanningTuple SpanningTupleBuffer::claimingLeadingDelimiterSearch(const SequenceNumber stEndSN)
{
    const auto [stEndBufferIdx, stEndAbaItNo] = getBufferIdxAndABAItNo(stEndSN);
    if (const auto leadingDistance = searchLeading(stEndBufferIdx, stEndAbaItNo))
    {
        const auto stStartSN = stEndSN.getRawValue() - leadingDistance.value();
        const auto stStartBufferIdx = stStartSN % buffer.size();
        const auto isPriorIteration = stEndBufferIdx.getRawValue() < stStartBufferIdx;
        return ClaimedSpanningTuple{
            .firstBuffer = buffer[stStartBufferIdx].tryClaimSpanningTuple(
                static_cast<ABAItNo>(stEndAbaItNo.getRawValue() - static_cast<size_t>(isPriorIteration))),
            .snOfLastBuffer = SequenceNumber{stStartSN},
        };
    }
    return ClaimedSpanningTuple{.firstBuffer = std::nullopt, .snOfLastBuffer = INVALID<SequenceNumber>};
}

SpanningTupleBuffer::ClaimedSpanningTuple SpanningTupleBuffer::claimingTrailingDelimiterSearch(const SequenceNumber stStartSN)
{
    return claimingTrailingDelimiterSearch(stStartSN, stStartSN);
}

SpanningTupleBuffer::ClaimedSpanningTuple
SpanningTupleBuffer::claimingTrailingDelimiterSearch(const SequenceNumber stStartSN, const SequenceNumber searchStartSN)
{
    const auto [stStartBufferIdx, stStartABAItNo] = getBufferIdxAndABAItNo(stStartSN);
    const auto [searchStartBufferIdx, searchStartABAItNo] = getBufferIdxAndABAItNo(searchStartSN);

    if (const auto trailingDistance = searchTrailing(searchStartBufferIdx, searchStartABAItNo))
    {
        const auto lastSN = SequenceNumber{searchStartSN.getRawValue() + trailingDistance.value()};
        return ClaimedSpanningTuple{
            .firstBuffer = buffer[stStartBufferIdx.getRawValue()].tryClaimSpanningTuple(stStartABAItNo),
            .snOfLastBuffer = lastSN,
        };
    }
    return ClaimedSpanningTuple{.firstBuffer = std::nullopt, .snOfLastBuffer = INVALID<SequenceNumber>};
}
}
