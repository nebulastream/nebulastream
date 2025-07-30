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

#include <STBuffer.hpp>

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
#include <STBufferState.hpp>
#include <SequenceShredder.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

STBuffer::STBuffer(const size_t initialSize) : buffer(std::vector<STBufferEntry>(initialSize))
{
    buffer[0].setStateOfFirstIndex();
}

STBuffer::ClaimingSearchResult STBuffer::searchAndTryClaimLeadingST(const SequenceNumber sequenceNumber)
{
    const auto [sequenceNumberBufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
    if (const auto leadingDistance = searchLeading(sequenceNumberBufferIdx, abaItNumber))
    {
        const auto stStartSN = SequenceNumber{sequenceNumber.getRawValue() - leadingDistance.value()};
        if (auto [startBuffer, stEndSN] = claimingTrailingDelimiterSearch(stStartSN, sequenceNumber); startBuffer.has_value())
        {
            return ClaimingSearchResult{
                .type = ClaimingSearchResult::Type::LEADING_ST_ONLY,
                .leadingSTStartBuffer = std::move(startBuffer),
                .trailingSTStartBuffer = std::nullopt,
                .firstSequenceNumber = stStartSN,
                .lastSequenceNumber = stEndSN};
        }
    }
    return ClaimingSearchResult{.type = ClaimingSearchResult::Type::NONE};
}

STBuffer::ClaimingSearchResult STBuffer::searchAndTryClaimLeadingAndTrailingST(const SequenceNumber sequenceNumber)
{
    auto [firstSTStartBuffer, firstSTStartSN] = claimingLeadingDelimiterSearch(sequenceNumber);
    auto [secondSTStartBuffer, secondSTFinalSN] = claimingTrailingDelimiterSearch(sequenceNumber);
    const auto rangeState = static_cast<ClaimingSearchResult::Type>(
        (static_cast<uint8_t>(secondSTStartBuffer.has_value()) << 1UL) + static_cast<uint8_t>(firstSTStartBuffer.has_value()));
    return ClaimingSearchResult{
        .type = rangeState,
        .leadingSTStartBuffer = std::move(firstSTStartBuffer),
        .trailingSTStartBuffer = std::move(secondSTStartBuffer),
        .firstSequenceNumber = firstSTStartSN,
        .lastSequenceNumber = secondSTFinalSN};
}

void STBuffer::claimSTBuffers(const SequenceNumber stStartSn, const std::span<StagedBuffer> spanningTupleBuffers)
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

SequenceShredderResult STBuffer::tryFindSTsForBufferWithDelimiter(const SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    /// Try to set 'indexedRawBuffer' at corresponding index in buffer. Return as 'out-of-range' buffer, if setting the buffer fails
    if (const auto [bufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
        not buffer[bufferIdx.getRawValue()].trySetWithDelimiter(abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .spanningBuffers = {}};
    }
    switch (auto searchAndClaimResult = searchAndTryClaimLeadingAndTrailingST(sequenceNumber); searchAndClaimResult.type)
    {
        case ClaimingSearchResult::Type::NONE: {
            return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers(0, {indexedRawBuffer})};
        }
        case ClaimingSearchResult::Type::LEADING_ST_ONLY: {
            const auto sizeOfSpanningTuple = sequenceNumber.getRawValue() - searchAndClaimResult.firstSequenceNumber.getRawValue() + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            INVARIANT(searchAndClaimResult.leadingSTStartBuffer.has_value(), "A leading spanning tuple must have start buffer value");
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTStartBuffer.value());
            claimSTBuffers(searchAndClaimResult.firstSequenceNumber, spanningTupleBuffers);
            return SequenceShredderResult{
                .isInRange = true, .spanningBuffers = SpanningBuffers(sizeOfSpanningTuple - 1, std::move(spanningTupleBuffers))};
        }
        case ClaimingSearchResult::Type::TRAILING_ST_ONLY: {
            const auto sizeOfSpanningTuple = searchAndClaimResult.lastSequenceNumber.getRawValue() - sequenceNumber.getRawValue() + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            INVARIANT(searchAndClaimResult.trailingSTStartBuffer.has_value(), "A trailing spanning tuple must have start buffer value");
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.trailingSTStartBuffer.value());
            claimSTBuffers(sequenceNumber, spanningTupleBuffers);
            return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers(0, std::move(spanningTupleBuffers))};
        }
        case ClaimingSearchResult::Type::LEADING_AND_TRAILING_ST: {
            const auto sizeOfFirstST = sequenceNumber.getRawValue() - searchAndClaimResult.firstSequenceNumber.getRawValue() + 1;
            const auto sizeOfBothSTs
                = searchAndClaimResult.lastSequenceNumber.getRawValue() - searchAndClaimResult.firstSequenceNumber.getRawValue() + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfBothSTs);
            INVARIANT(searchAndClaimResult.leadingSTStartBuffer.has_value(), "A leading spanning tuple must have start buffer value");
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTStartBuffer.value());
            /// Claim the remaining buffers of both STs (separately, to not double claim the trailing buffer of the entry at 'rbIdxOfSN')
            claimSTBuffers(searchAndClaimResult.firstSequenceNumber, std::span{spanningTupleBuffers}.subspan(0, sizeOfFirstST));
            claimSTBuffers(sequenceNumber, std::span{spanningTupleBuffers}.subspan(sizeOfFirstST - 1));
            return SequenceShredderResult{
                .isInRange = true, .spanningBuffers = SpanningBuffers(sizeOfFirstST - 1, std::move(spanningTupleBuffers))};
        }
    }
    std::unreachable();
}

SequenceShredderResult
STBuffer::tryFindSTsForBufferWithoutDelimiter(const SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    /// Try to set 'indexedRawBuffer' at corresponding index in buffer. Return as 'out-of-range' buffer, if setting the buffer fails
    if (const auto [bufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
        not buffer[bufferIdx.getRawValue()].trySetWithoutDelimiter(abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .spanningBuffers = {}};
    }
    switch (auto searchAndClaimResult = searchAndTryClaimLeadingST(sequenceNumber); searchAndClaimResult.type)
    {
        case ClaimingSearchResult::Type::NONE: {
            return SequenceShredderResult{.isInRange = true, .spanningBuffers = SpanningBuffers(0, {indexedRawBuffer})};
        }
        case ClaimingSearchResult::Type::LEADING_ST_ONLY: {
            const auto sizeOfSpanningTuple
                = searchAndClaimResult.lastSequenceNumber.getRawValue() - searchAndClaimResult.firstSequenceNumber.getRawValue() + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            INVARIANT(searchAndClaimResult.leadingSTStartBuffer.has_value(), "A leading spanning tuple must have start buffer value");
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTStartBuffer.value());
            claimSTBuffers(searchAndClaimResult.firstSequenceNumber, spanningTupleBuffers);
            const auto currentBufferIdx = sequenceNumber.getRawValue() - searchAndClaimResult.lastSequenceNumber.getRawValue();
            return SequenceShredderResult{
                .isInRange = true, .spanningBuffers = SpanningBuffers(currentBufferIdx, std::move(spanningTupleBuffers))};
        }
        case ClaimingSearchResult::Type::TRAILING_ST_ONLY:
        case ClaimingSearchResult::Type::LEADING_AND_TRAILING_ST:
            INVARIANT(false, "A buffer without a delimiter can only find one spanning tuple in leading direction.");
    }
    std::unreachable();
}

bool STBuffer::validate() const
{
    bool isValid = true;
    const auto lastIdxOfRB = buffer.size() - 1;
    for (const auto& [idx, metaData] : buffer | NES::views::enumerate)
    {
        isValid &= metaData.validateFinalState(
            static_cast<STBufferIdx>(idx), buffer.at((idx + 1) % buffer.size()), static_cast<STBufferIdx>(lastIdxOfRB));
    }
    return isValid;
}

std::ostream& operator<<(std::ostream& os, const STBuffer& sequenceRingBuffer)
{
    return os << fmt::format("RingBuffer({})", sequenceRingBuffer.buffer.size());
}

std::pair<STBufferIdx, ABAItNo> STBuffer::getBufferIdxAndABAItNo(const SequenceNumber sequenceNumber) const
{
    const auto sequenceNumberBufferIdx = STBufferIdx{static_cast<uint32_t>(sequenceNumber.getRawValue() % buffer.size())};
    const auto abaItNumber = static_cast<ABAItNo>((sequenceNumber.getRawValue() / buffer.size()) + 1);
    return std::make_pair(sequenceNumberBufferIdx, abaItNumber);
}

std::optional<size_t> STBuffer::searchLeading(const STBufferIdx searchStartBufferIdx, const ABAItNo abaItNumber) const
{
    const auto searchStartBufferIdxV = searchStartBufferIdx.getRawValue();
    size_t leadingDistance = 1;
    auto isPriorIteration = searchStartBufferIdxV < leadingDistance;
    auto entryState = this->buffer[(searchStartBufferIdxV - leadingDistance) % buffer.size()].getEntryState(
        static_cast<ABAItNo>(abaItNumber.getRawValue() - static_cast<size_t>(isPriorIteration)));
    while (entryState.hasCorrectABA and not(entryState.hasDelimiter))
    {
        ++leadingDistance;
        isPriorIteration = searchStartBufferIdxV < leadingDistance;
        entryState = this->buffer[(searchStartBufferIdxV - leadingDistance) % buffer.size()].getEntryState(
            static_cast<ABAItNo>(abaItNumber.getRawValue() - static_cast<size_t>(isPriorIteration)));
    }
    return (entryState.hasCorrectABA) ? std::optional{leadingDistance} : std::nullopt;
}

std::optional<size_t> STBuffer::searchTrailing(const STBufferIdx searchStartBufferIdx, const ABAItNo abaItNumber) const
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

STBuffer::ClaimedSpanningTuple STBuffer::claimingLeadingDelimiterSearch(const SequenceNumber stEndSN)
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

STBuffer::ClaimedSpanningTuple STBuffer::claimingTrailingDelimiterSearch(const SequenceNumber stStartSN)
{
    return claimingTrailingDelimiterSearch(stStartSN, stStartSN);
}

STBuffer::ClaimedSpanningTuple STBuffer::claimingTrailingDelimiterSearch(const SequenceNumber stStartSN, const SequenceNumber searchStartSN)
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
};
}
