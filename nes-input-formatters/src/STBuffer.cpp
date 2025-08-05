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
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Ranges.hpp>
#include <RawTupleBuffer.hpp>
#include <STBufferState.hpp>
#include <SequenceShredder.hpp>

#include <ErrorHandling.hpp>


namespace NES::InputFormatters
{

STBuffer::STBuffer(const size_t initialSize, Memory::TupleBuffer dummyBuffer) : buffer(std::vector<STBufferEntry>(initialSize))
{
    buffer[0].setStateOfFirstIndex(std::move(dummyBuffer));
}

STBuffer::ClaimingSearchResult STBuffer::searchAndTryClaimLeadingSTuple(const SequenceNumber sequenceNumber)
{
    const auto [sequenceNumberBufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
    if (const auto leadingDistance = searchLeading(sequenceNumberBufferIdx, abaItNumber))
    {
        const auto sTupleStartSN = SequenceNumber{sequenceNumber.getRawValue() - leadingDistance.value()};
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

STBuffer::ClaimingSearchResult STBuffer::searchAndTryClaimLeadingAndTrailingSTuple(const SequenceNumber sequenceNumber)
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

void STBuffer::claimSTupleBuffers(const SequenceNumber sTupleStartSN, const std::span<StagedBuffer> spanningTupleBuffers)
{
    const auto lastOffset = spanningTupleBuffers.size() - 1;
    for (size_t offset = 1; offset < lastOffset; ++offset)
    {
        const auto nextBufferIdx = (sTupleStartSN.getRawValue() + offset) % buffer.size();
        buffer[nextBufferIdx].claimNoDelimiterBuffer(spanningTupleBuffers, offset);
    }
    const auto lastBufferIdx = (sTupleStartSN.getRawValue() + lastOffset) % buffer.size();
    buffer[lastBufferIdx].claimLeadingBuffer(spanningTupleBuffers, lastOffset);
}

SequenceShredderResult STBuffer::tryFindSTsForBufferWithDelimiter(const SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    /// Try to set 'indexedRawBuffer' at corresponding index in buffer. Return as 'out-of-range' buffer, if setting the buffer fails
    const auto [bufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
    if (buffer[bufferIdx.getRawValue()].trySetWithDelimiter(abaItNumber, indexedRawBuffer))
    {
        switch (auto searchAndClaimResult = searchAndTryClaimLeadingAndTrailingSTuple(sequenceNumber); searchAndClaimResult.type)
        {
            case ClaimingSearchResult::Type::NONE: {
                return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
            }
            case ClaimingSearchResult::Type::LEADING_ST_ONLY: {
                const auto sizeOfSpanningTuple = sequenceNumber.getRawValue() - searchAndClaimResult.sTupleStartSN.getRawValue() + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
                spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTupleStartBuffer.value());
                claimSTupleBuffers(searchAndClaimResult.sTupleStartSN, spanningTupleBuffers);
                return SequenceShredderResult{
                    .isInRange = true, .indexOfInputBuffer = sizeOfSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
            }
            case ClaimingSearchResult::Type::TRAILING_ST_ONLY: {
                const auto sizeOfSpanningTuple = searchAndClaimResult.sTupleEndSN.getRawValue() - sequenceNumber.getRawValue() + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
                spanningTupleBuffers[0] = std::move(searchAndClaimResult.trailingSTupleStartBuffer.value());
                claimSTupleBuffers(sequenceNumber, spanningTupleBuffers);
                return SequenceShredderResult{
                    .isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = std::move(spanningTupleBuffers)};
            }
            case ClaimingSearchResult::Type::LEADING_AND_TRAILING_ST: {
                const auto sizeOfFirstST = sequenceNumber.getRawValue() - searchAndClaimResult.sTupleStartSN.getRawValue() + 1;
                const auto sizeOfBothSTs
                    = searchAndClaimResult.sTupleEndSN.getRawValue() - searchAndClaimResult.sTupleStartSN.getRawValue() + 1;
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

SequenceShredderResult
STBuffer::tryFindSTsForBufferWithoutDelimiter(const SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    /// Try to set 'indexedRawBuffer' at corresponding index in buffer. Return as 'out-of-range' buffer, if setting the buffer fails
    const auto [bufferIdx, abaItNumber] = getBufferIdxAndABAItNo(sequenceNumber);
    if (buffer[bufferIdx.getRawValue()].trySetWithoutDelimiter(abaItNumber, indexedRawBuffer))
    {
        switch (auto searchAndClaimResult = searchAndTryClaimLeadingSTuple(sequenceNumber); searchAndClaimResult.type)
        {
            case ClaimingSearchResult::Type::NONE: {
                return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
            }
            case ClaimingSearchResult::Type::LEADING_ST_ONLY: {
                const auto sizeOfSpanningTuple
                    = searchAndClaimResult.sTupleEndSN.getRawValue() - searchAndClaimResult.sTupleStartSN.getRawValue() + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
                spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTupleStartBuffer.value());
                claimSTupleBuffers(searchAndClaimResult.sTupleStartSN, spanningTupleBuffers);
                const auto currentBufferIdx = sequenceNumber.getRawValue() - searchAndClaimResult.sTupleEndSN.getRawValue();
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
    const auto abaItNumber = static_cast<ABAItNo>(sequenceNumber.getRawValue() / buffer.size() + 1);
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

STBuffer::ClaimedSpanningTuple STBuffer::claimingLeadingDelimiterSearch(const SequenceNumber sTupleEndSN)
{
    const auto [sTupleEndBufferIdx, sTupleEndAbaItNo] = getBufferIdxAndABAItNo(sTupleEndSN);
    if (const auto leadingDistance = searchLeading(sTupleEndBufferIdx, sTupleEndAbaItNo))
    {
        const auto sTupleStartSN = sTupleEndSN.getRawValue() - leadingDistance.value();
        const auto sTupleStartBufferIdx = sTupleStartSN % buffer.size();
        const auto isPriorIteration = sTupleEndBufferIdx.getRawValue() < sTupleStartBufferIdx;
        return ClaimedSpanningTuple{
            .firstBuffer = buffer[sTupleStartBufferIdx].tryClaimSpanningTuple(
                static_cast<ABAItNo>(sTupleEndAbaItNo.getRawValue() - static_cast<size_t>(isPriorIteration))),
            .snOfLastBuffer = SequenceNumber{sTupleStartSN},
        };
    }
    return ClaimedSpanningTuple{.firstBuffer = std::nullopt, .snOfLastBuffer = INVALID<SequenceNumber>};
}

STBuffer::ClaimedSpanningTuple STBuffer::claimingTrailingDelimiterSearch(const SequenceNumber sTupleStartSN)
{
    return claimingTrailingDelimiterSearch(sTupleStartSN, sTupleStartSN);
}

STBuffer::ClaimedSpanningTuple
STBuffer::claimingTrailingDelimiterSearch(const SequenceNumber sTupleStartSN, const SequenceNumber searchStartSN)
{
    const auto [sTupleStartBufferIdx, sTupleStartABAItNo] = getBufferIdxAndABAItNo(sTupleStartSN);
    const auto [searchStartBufferIdx, searchStartABAItNo] = getBufferIdxAndABAItNo(searchStartSN);

    if (const auto trailingDistance = searchTrailing(searchStartBufferIdx, searchStartABAItNo))
    {
        const auto leadingSequenceNumber = SequenceNumber{searchStartSN.getRawValue() + trailingDistance.value()};
        return ClaimedSpanningTuple{
            .firstBuffer = buffer[sTupleStartBufferIdx.getRawValue()].tryClaimSpanningTuple(sTupleStartABAItNo),
            .snOfLastBuffer = leadingSequenceNumber,
        };
    }
    return ClaimedSpanningTuple{.firstBuffer = std::nullopt, .snOfLastBuffer = INVALID<SequenceNumber>};
};
}
