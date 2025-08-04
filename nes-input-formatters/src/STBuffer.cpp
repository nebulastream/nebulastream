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

namespace NES::InputFormatters
{

STBuffer::STBuffer(const size_t initialSize) : ringBuffer(std::vector<STBufferEntry>(initialSize))
{
    ringBuffer[0].setStateOfFirstIndex();
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
                .state = ClaimingSearchResult::State::LEADING_ST_ONLY,
                .leadingSTupleStartBuffer = std::move(startBuffer),
                .trailingSTupleStartBuffer = std::nullopt,
                .sTupleStartSN = sTupleStartSN,
                .sTupleEndSN = sTupleEndSN};
        }
    }
    return ClaimingSearchResult{.state = ClaimingSearchResult::State::NONE};
}

STBuffer::ClaimingSearchResult
STBuffer::searchAndTryClaimLeadingAndTrailingSTuple(const SequenceNumberType sequenceNumber)
{
    auto [firstSTStartBuffer, firstSTFinalSN] = claimingLeadingDelimiterSearch(sequenceNumber);
    auto [secondSTStartBuffer, secondSTFinalSN] = claimingTrailingDelimiterSearch(sequenceNumber);
    const auto rangeState = static_cast<ClaimingSearchResult::State>(
        (static_cast<uint8_t>(secondSTStartBuffer.has_value()) << 1UL) + static_cast<uint8_t>(firstSTStartBuffer.has_value()));
    return ClaimingSearchResult{
        .state = rangeState,
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
        const auto nextBufferIdx = (sTupleStartSN + offset) % ringBuffer.size();
        ringBuffer[nextBufferIdx].claimNoDelimiterBuffer(spanningTupleBuffers, offset);
    }
    const auto lastBufferIdx = (sTupleStartSN + lastOffset) % ringBuffer.size();
    ringBuffer[lastBufferIdx].claimLeadingBuffer(spanningTupleBuffers, lastOffset);
}

bool STBuffer::trySetNewBufferWithDelimiter(const size_t sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / ringBuffer.size()) + 1;
    const auto rbIdxOfSN = sequenceNumber % ringBuffer.size();
    return ringBuffer[rbIdxOfSN].trySetWithDelimiter(abaItNumber, indexedRawBuffer);
}
bool STBuffer::trySetNewBufferWithOutDelimiter(const size_t sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / ringBuffer.size()) + 1;
    const auto rbIdxOfSN = sequenceNumber % ringBuffer.size();
    return ringBuffer[rbIdxOfSN].trySetWithoutDelimiter(abaItNumber, indexedRawBuffer);
}

bool STBuffer::validate() const
{
    bool isValid = true;
    const auto lastIdxOfRB = ringBuffer.size() - 1;
    for (const auto& [idx, metaData] : ringBuffer | NES::views::enumerate)
    {
        isValid &= metaData.validateFinalState(idx, ringBuffer.at((idx + 1) % ringBuffer.size()), lastIdxOfRB);
    }
    return isValid;
}

std::ostream& operator<<(std::ostream& os, const STBuffer& sequenceRingBuffer)
{
    return os << fmt::format("RingBuffer({})", sequenceRingBuffer.ringBuffer.size());
}


std::pair<SequenceNumberType, uint32_t> STBuffer::getBufferIdxAndABAItNo(const SequenceNumberType sequenceNumber) const
{
    const auto sequenceNumberBufferIdx = sequenceNumber % ringBuffer.size();
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / ringBuffer.size()) + 1;
    return std::make_pair(sequenceNumberBufferIdx, abaItNumber);
}

std::optional<size_t> STBuffer::searchLeading(const size_t searchStartBufferIdx, const size_t abaItNumber) const
{
    size_t leadingDistance = 1;
    auto isPriorIteration = static_cast<size_t>(searchStartBufferIdx < leadingDistance);
    auto cellState
        = this->ringBuffer[(searchStartBufferIdx - leadingDistance) % ringBuffer.size()].getEntryState(abaItNumber - isPriorIteration);
    while (cellState.hasCorrectABA and not(cellState.hasDelimiter))
    {
        ++leadingDistance;
        isPriorIteration = static_cast<size_t>(searchStartBufferIdx < leadingDistance);
        cellState
            = this->ringBuffer[(searchStartBufferIdx - leadingDistance) % ringBuffer.size()].getEntryState(abaItNumber - isPriorIteration);
    }
    return (cellState.hasCorrectABA) ? std::optional{leadingDistance} : std::nullopt;
}

std::optional<size_t> STBuffer::searchTrailing(const size_t searchStartBufferIdx, const size_t abaItNumber) const
{
    size_t trailingDistance = 1;
    auto isNextIteration = static_cast<size_t>((searchStartBufferIdx + trailingDistance) >= ringBuffer.size());
    auto cellState
        = this->ringBuffer[(searchStartBufferIdx + trailingDistance) % ringBuffer.size()].getEntryState(abaItNumber + isNextIteration);
    while (cellState.hasCorrectABA and not(cellState.hasDelimiter))
    {
        ++trailingDistance;
        isNextIteration = static_cast<size_t>((searchStartBufferIdx + trailingDistance) >= ringBuffer.size());
        cellState
            = this->ringBuffer[(searchStartBufferIdx + trailingDistance) % ringBuffer.size()].getEntryState(abaItNumber + isNextIteration);
    }
    return (cellState.hasCorrectABA) ? std::optional{trailingDistance} : std::nullopt;
}

STBuffer::ClaimedSpanningTuple STBuffer::claimingLeadingDelimiterSearch(const SequenceNumberType spanningTupleEndSN)
{
    const auto [sTupleEndBufferIdx, sTupleEndAbaItNo] = getBufferIdxAndABAItNo(spanningTupleEndSN);
    if (const auto leadingDistance = searchLeading(sTupleEndBufferIdx, sTupleEndAbaItNo))
    {
        const auto sTupleStartSN = spanningTupleEndSN - leadingDistance.value();
        const auto sTupleStartBufferIdx = sTupleStartSN % ringBuffer.size();
        const auto isPriorIteration = static_cast<size_t>(sTupleEndBufferIdx < sTupleStartBufferIdx);
        return ClaimedSpanningTuple{
            .firstBuffer = ringBuffer[sTupleStartBufferIdx].tryClaimSpanningTuple(sTupleEndAbaItNo - isPriorIteration),
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
            .firstBuffer = ringBuffer[sTupleStartBufferIdx].tryClaimSpanningTuple(sTupleStartABAItNo),
            .snOfLastBuffer = leadingSequenceNumber,
        };
    }
    return ClaimedSpanningTuple{std::nullopt, 0};
};
}
