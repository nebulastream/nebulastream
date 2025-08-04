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

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string_view>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Ranges.hpp>
#include <RawTupleBuffer.hpp>

#include <ErrorHandling.hpp>

#include "SequenceShredder.hpp"

namespace NES::InputFormatters
{

STBuffer::STBuffer(const size_t initialSize) : buffer(std::vector<STBufferEntry>(initialSize))
{
    buffer[0].setStateOfFirstIndex();
}

// Todo: rename to something more appropriate <-- single buffer can be found
STBuffer::ClaimingSearchResult STBuffer::searchAndTryClaimLeadingSTuple(const SequenceNumberType sequenceNumber)
{
    const auto [sequenceNumberBufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
    if (const auto leadingDistance = searchLeading(sequenceNumberBufferIdx, abaItNumber))
    {
        const auto sTupleStartSN = sequenceNumber - leadingDistance.value();
        if (auto [startBuffer, sTupleEndSN] = claimingTrailingDelimiterSearch(sTupleStartSN, sequenceNumber); startBuffer.has_value())
        {
            return ClaimingSearchResult{
                .type = ClaimingSearchResult::Type::LEADING_ST_ONLY,
                .leadingSTupleStartBuffer = std::move(startBuffer),
                .trailingSTupleStartBuffer = std::nullopt,
                .sTupleStartSN = sTupleStartSN,
                .sTupleEndSN = sTupleEndSN};
        }
    }
    return ClaimingSearchResult{.type = ClaimingSearchResult::Type::NONE};
}

STBuffer::ClaimingSearchResult STBuffer::searchAndTryClaimLeadingAndTrailingSTuple(const SequenceNumberType sequenceNumber)
{
    auto [firstSTStartBuffer, firstSTFinalSN] = claimingLeadingDelimiterSearch(sequenceNumber);
    auto [secondSTStartBuffer, secondSTFinalSN] = claimingTrailingDelimiterSearch(sequenceNumber);
    const auto rangeState = static_cast<ClaimingSearchResult::Type>(
        (static_cast<uint8_t>(secondSTStartBuffer.has_value()) << 1UL) + static_cast<uint8_t>(firstSTStartBuffer.has_value()));
    return ClaimingSearchResult{
        .type = rangeState,
        .leadingSTupleStartBuffer = std::move(firstSTStartBuffer),
        .trailingSTupleStartBuffer = std::move(secondSTStartBuffer),
        .sTupleStartSN = firstSTFinalSN,
        .sTupleEndSN = secondSTFinalSN};
}

void STBuffer::claimSTupleBuffers(const size_t sTupleStartSN, const std::span<StagedBuffer> spanningTupleBuffers)
{
    const auto lastOffset = spanningTupleBuffers.size() - 1;
    for (size_t offset = 1; offset < lastOffset; ++offset)
    {
        const auto nextBufferIdx = (sTupleStartSN + offset) % buffer.size();
        buffer[nextBufferIdx].claimNoDelimiterBuffer(spanningTupleBuffers, offset);
    }
    const auto lastBufferIdx = (sTupleStartSN + lastOffset) % buffer.size();
    buffer[lastBufferIdx].claimLeadingBuffer(spanningTupleBuffers, lastOffset);
}

SequenceShredderResult STBuffer::tryFindSTsForBufferWithDelimiter(const size_t sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    const auto [bufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
    if (buffer[bufferIdx].trySetWithDelimiter(abaItNumber, indexedRawBuffer))
    {
        switch (auto searchAndClaimResult = searchAndTryClaimLeadingAndTrailingSTuple(sequenceNumber); searchAndClaimResult.type)
        {
            case ClaimingSearchResult::Type::NONE: {
                return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
            }
            case ClaimingSearchResult::Type::LEADING_ST_ONLY: {
                const auto sizeOfSpanningTuple = sequenceNumber - searchAndClaimResult.sTupleStartSN + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
                spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTupleStartBuffer.value());
                claimSTupleBuffers(searchAndClaimResult.sTupleStartSN, spanningTupleBuffers);
                return SequenceShredderResult{
                    .isInRange = true, .indexOfInputBuffer = sizeOfSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
            }
            case ClaimingSearchResult::Type::TRAILING_ST_ONLY: {
                const auto sizeOfSpanningTuple = searchAndClaimResult.sTupleEndSN - sequenceNumber + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
                spanningTupleBuffers[0] = std::move(searchAndClaimResult.trailingSTupleStartBuffer.value());
                claimSTupleBuffers(sequenceNumber, spanningTupleBuffers);
                return SequenceShredderResult{
                    .isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = std::move(spanningTupleBuffers)};
            }
            case ClaimingSearchResult::Type::LEADING_AND_TRAILING_ST: {
                const auto sizeOfFirstST = sequenceNumber - searchAndClaimResult.sTupleStartSN + 1;
                const auto sizeOfBothSTs = searchAndClaimResult.sTupleEndSN - searchAndClaimResult.sTupleStartSN + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfBothSTs);
                spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTupleStartBuffer.value());
                /// Claim the remaining buffers of both STs (separately, to not double claim the trailing buffer of the entry at 'rbIdxOfSN')
                claimSTupleBuffers(searchAndClaimResult.sTupleStartSN, std::span{spanningTupleBuffers}.subspan(0, sizeOfFirstST));
                claimSTupleBuffers(sequenceNumber, std::span{spanningTupleBuffers}.subspan(sizeOfFirstST - 1));
                return SequenceShredderResult{
                    .isInRange = true, .indexOfInputBuffer = sizeOfFirstST - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
            }
        }
        std::unreachable();
    }
    return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
}
SequenceShredderResult STBuffer::tryFindSTsForBufferWithoutDelimiter(const size_t sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    const auto [bufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
    if (buffer[bufferIdx].trySetWithoutDelimiter(abaItNumber, indexedRawBuffer))
    {
        switch (auto searchAndClaimResult = searchAndTryClaimLeadingSTuple(sequenceNumber); searchAndClaimResult.type)
        {
            case ClaimingSearchResult::Type::NONE: {
                return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
            }
            case ClaimingSearchResult::Type::LEADING_ST_ONLY: {
                const auto sizeOfSpanningTuple = searchAndClaimResult.sTupleEndSN - searchAndClaimResult.sTupleStartSN + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
                spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTupleStartBuffer.value());
                claimSTupleBuffers(searchAndClaimResult.sTupleStartSN, spanningTupleBuffers);
                const auto currentBufferIdx = sequenceNumber - searchAndClaimResult.sTupleEndSN;
                return SequenceShredderResult{
                    .isInRange = true, .indexOfInputBuffer = currentBufferIdx, .spanningBuffers = std::move(spanningTupleBuffers)};
            }
            case ClaimingSearchResult::Type::TRAILING_ST_ONLY:
            case ClaimingSearchResult::Type::LEADING_AND_TRAILING_ST:
                INVARIANT(false, "A buffer without a delimiter can only find one spanning tuple in leading direction.");
        }
        std::unreachable();
    }
    return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
}

bool STBuffer::validate() const
{
    bool isValid = true;
    const auto lastIdxOfRB = buffer.size() - 1;
    for (const auto& [idx, metaData] : buffer | NES::views::enumerate)
    {
        isValid &= metaData.validateFinalState(idx, buffer.at((idx + 1) % buffer.size()), lastIdxOfRB);
    }
    return isValid;
}

std::ostream& operator<<(std::ostream& os, const STBuffer& sequenceRingBuffer)
{
    return os << fmt::format("RingBuffer({})", sequenceRingBuffer.buffer.size());
}


std::pair<SequenceNumberType, uint32_t> STBuffer::getBufferIdxAndABAItNo(const SequenceNumberType sequenceNumber) const
{
    const auto sequenceNumberBufferIdx = sequenceNumber % buffer.size();
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / buffer.size()) + 1;
    return std::make_pair(sequenceNumberBufferIdx, abaItNumber);
}

std::optional<size_t> STBuffer::searchLeading(const size_t searchStartBufferIdx, const size_t abaItNumber) const
{
    size_t leadingDistance = 1;
    auto isPriorIteration = static_cast<size_t>(searchStartBufferIdx < leadingDistance);
    auto cellState = this->buffer[(searchStartBufferIdx - leadingDistance) % buffer.size()].getEntryState(abaItNumber - isPriorIteration);
    while (cellState.hasCorrectABA and not(cellState.hasDelimiter))
    {
        ++leadingDistance;
        isPriorIteration = static_cast<size_t>(searchStartBufferIdx < leadingDistance);
        cellState = this->buffer[(searchStartBufferIdx - leadingDistance) % buffer.size()].getEntryState(abaItNumber - isPriorIteration);
    }
    return (cellState.hasCorrectABA) ? std::optional{leadingDistance} : std::nullopt;
}

std::optional<size_t> STBuffer::searchTrailing(const size_t searchStartBufferIdx, const size_t abaItNumber) const
{
    size_t trailingDistance = 1;
    auto isNextIteration = static_cast<size_t>((searchStartBufferIdx + trailingDistance) >= buffer.size());
    auto cellState = this->buffer[(searchStartBufferIdx + trailingDistance) % buffer.size()].getEntryState(abaItNumber + isNextIteration);
    while (cellState.hasCorrectABA and not(cellState.hasDelimiter))
    {
        ++trailingDistance;
        isNextIteration = static_cast<size_t>((searchStartBufferIdx + trailingDistance) >= buffer.size());
        cellState = this->buffer[(searchStartBufferIdx + trailingDistance) % buffer.size()].getEntryState(abaItNumber + isNextIteration);
    }
    return (cellState.hasCorrectABA) ? std::optional{trailingDistance} : std::nullopt;
}

STBuffer::ClaimedSpanningTuple STBuffer::claimingLeadingDelimiterSearch(const SequenceNumberType spanningTupleEndSN)
{
    const auto [sTupleEndBufferIdx, sTupleEndAbaItNo] = getBufferIdxAndABAItNo(spanningTupleEndSN);
    if (const auto leadingDistance = searchLeading(sTupleEndBufferIdx, sTupleEndAbaItNo))
    {
        const auto sTupleStartSN = spanningTupleEndSN - leadingDistance.value();
        const auto sTupleStartBufferIdx = sTupleStartSN % buffer.size();
        const auto isPriorIteration = static_cast<size_t>(sTupleEndBufferIdx < sTupleStartBufferIdx);
        return ClaimedSpanningTuple{
            .firstBuffer = buffer[sTupleStartBufferIdx].tryClaimSpanningTuple(sTupleEndAbaItNo - isPriorIteration),
            .snOfLastBuffer = sTupleStartSN,
        };
    }
    return ClaimedSpanningTuple{std::nullopt, 0};
}

STBuffer::ClaimedSpanningTuple STBuffer::claimingTrailingDelimiterSearch(const SequenceNumberType sTupleStartSN)
{
    return claimingTrailingDelimiterSearch(sTupleStartSN, sTupleStartSN);
}

STBuffer::ClaimedSpanningTuple
STBuffer::claimingTrailingDelimiterSearch(const SequenceNumberType sTupleStartSN, const SequenceNumberType searchStartSN)
{
    const auto [sTupleStartBufferIdx, sTupleStartABAItNo] = getBufferIdxAndABAItNo(sTupleStartSN);
    const auto [searchStartBufferIdx, searchStartABAItNo] = getBufferIdxAndABAItNo(searchStartSN);

    if (const auto trailingDistance = searchTrailing(searchStartBufferIdx, searchStartABAItNo))
    {
        const auto leadingSequenceNumber = searchStartSN + trailingDistance.value();
        return ClaimedSpanningTuple{
            .firstBuffer = buffer[sTupleStartBufferIdx].tryClaimSpanningTuple(sTupleStartABAItNo),
            .snOfLastBuffer = leadingSequenceNumber,
        };
    }
    return ClaimedSpanningTuple{std::nullopt, 0};
};
}
