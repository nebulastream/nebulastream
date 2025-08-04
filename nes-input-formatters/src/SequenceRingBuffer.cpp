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

#include <SequenceRingBuffer.hpp>

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

//==--------------------------------------------------------------------------------------------------------==//
//==---------------------------------------- Atomic Bitmap State -----------------------------------------------==//
//==--------------------------------------------------------------------------------------------------------==//
bool AtomicBitmapState::tryClaimSpanningTuple(const uint32_t abaItNumber)
{
    auto atomicFirstDelimiter = getState();
    auto desiredFirstDelimiter = atomicFirstDelimiter;
    desiredFirstDelimiter.claimSpanningTuple();

    bool claimedSpanningTuple = false;
    while (atomicFirstDelimiter.getABAItNo() == abaItNumber and not atomicFirstDelimiter.hasClaimedSpanningTuple()
           and not claimedSpanningTuple)
    {
        desiredFirstDelimiter.updateLeading(atomicFirstDelimiter);
        claimedSpanningTuple = this->state.compare_exchange_weak(atomicFirstDelimiter.state, desiredFirstDelimiter.state);
    }
    return claimedSpanningTuple;
}

std::ostream& operator<<(std::ostream& os, const AtomicBitmapState& atomicBitmapState)

{
    const auto loadedBitmap = atomicBitmapState.getState();
    return os << fmt::format(
               "[{}-{}-{}-{}]",
               loadedBitmap.hasTupleDelimiter(),
               loadedBitmap.getABAItNo(),
               loadedBitmap.hasUsedLeadingBuffer(),
               loadedBitmap.hasUsedTrailingBuffer(),
               loadedBitmap.hasClaimedSpanningTuple());
}

//==--------------------------------------------------------------------------------------------------------==//
//==---------------------------------------- BufferEntry -----------------------------------------------==//
//==--------------------------------------------------------------------------------------------------------==//
bool BufferEntry::isCurrentEntryUsedUp(const size_t abaItNumber) const
{
    const auto currentEntry = this->atomicBitmapState.getState();
    const auto currentABAItNo = currentEntry.getABAItNo();
    const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
    const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
    const auto priorEntryIsUsed = currentABAItNo == (abaItNumber - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
    return priorEntryIsUsed;
}

void BufferEntry::setBuffersAndOffsets(const StagedBuffer& indexedBuffer)
{
    this->leadingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->trailingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->firstDelimiterOffset = indexedBuffer.getOffsetOfFirstTupleDelimiter();
    this->lastDelimiterOffset = indexedBuffer.getOffsetOfLastTupleDelimiter();
}

bool BufferEntry::trySetWithDelimiter(const size_t abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicBitmapState.setHasTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

bool BufferEntry::trySetWithoutDelimiter(const size_t abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicBitmapState.setNoTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

std::optional<StagedBuffer> BufferEntry::tryClaimSpanningTuple(const uint32_t abaItNumber)
{
    if (this->atomicBitmapState.tryClaimSpanningTuple(abaItNumber))
    {
        // Todo: improve handling of first buffer (which is dummy)
        // -> create BufferPool with a single (valid) buffer?
        // -> mock buffer by mocking control block to avoid 'getNumberOfTuples' crash? <-- reinterpret_cast
        if (firstDelimiterOffset == std::numeric_limits<uint32_t>::max() and lastDelimiterOffset == 0)
        {
            const auto dummyBuffer = StagedBuffer(RawTupleBuffer{}, 1, firstDelimiterOffset, lastDelimiterOffset);
            this->atomicBitmapState.setUsedTrailingBuffer();
            return {dummyBuffer};
        }
        INVARIANT(this->trailingBufferRef.getBuffer() != nullptr, "Tried to claim a trailing buffer with a nullptr");
        const auto stagedBuffer
            = StagedBuffer(RawTupleBuffer{std::move(this->trailingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
        this->atomicBitmapState.setUsedTrailingBuffer();
        return {stagedBuffer};
    }
    return std::nullopt;
}

void BufferEntry::setStateOfFirstIndex()
{
    /// The first entry is a dummy that makes sure that we can resolve the first tuple in the first buffer
    this->atomicBitmapState.setStateOfFirstEntry();
    this->firstDelimiterOffset = std::numeric_limits<uint32_t>::max();
    this->lastDelimiterOffset = 0;
}

void BufferEntry::claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getBuffer() != nullptr, "Tried to claim a leading buffer with a nullptr");
    INVARIANT(this->trailingBufferRef.getBuffer() != nullptr, "Tried to claim a trailing buffer with a nullptr");

    /// First claim buffer uses
    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->trailingBufferRef = NES::Memory::TupleBuffer{};

    /// Then atomically mark buffer as used
    this->atomicBitmapState.setUsedLeadingAndTrailingBuffer();
}

void BufferEntry::claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getBuffer() != nullptr, "Tried to claim a leading buffer with a nullptr");
    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->atomicBitmapState.setUsedLeadingBuffer();
}

BufferEntry::EntryState BufferEntry::getEntryState(const size_t expectedABAItNo) const
{
    const auto currentState = this->atomicBitmapState.getState();
    const bool isCorrectABA = expectedABAItNo == currentState.getABAItNo();
    const bool hasDelimiter = currentState.hasTupleDelimiter();
    return EntryState{.hasCorrectABA = isCorrectABA, .hasDelimiter = hasDelimiter};
}
bool BufferEntry::validateFinalState(const size_t idx, const BufferEntry& nextEntry, const size_t lastIdxOfRB) const
{
    bool isValidFinalState = true;
    const auto state = this->atomicBitmapState.getState();
    if (not state.hasUsedLeadingBuffer())
    {
        isValidFinalState = false;
        NES_ERROR("Buffer at index {} does still claim to own leading buffer", idx);
    }
    if (this->leadingBufferRef.getBuffer() != nullptr)
    {
        isValidFinalState = false;
        NES_ERROR("Buffer at index {} still owns a leading buffer reference", idx);
    }

    /// Add '1' to the ABA iteration number, if the current entry is the last index of the ring buffer and the next entry wraps around
    if (state.getABAItNo() + static_cast<size_t>(idx == lastIdxOfRB) == nextEntry.atomicBitmapState.getABAItNo())
    {
        if (not state.hasUsedTrailingBuffer())
        {
            isValidFinalState = false;
            NES_ERROR("Buffer at index {} does still claim to own leading buffer", idx);
        }
        if (this->trailingBufferRef.getBuffer() != nullptr)
        {
            isValidFinalState = false;
            NES_ERROR("Buffer at index {} still owns a trailing buffer reference", idx);
        }
    }
    return isValidFinalState;
}


//==--------------------------------------------------------------------------------------------------------==//
//==---------------------------------------- SEQUENCE BUFFER -----------------------------------------------==//
//==--------------------------------------------------------------------------------------------------------==//
SequenceRingBuffer::SequenceRingBuffer(const size_t initialSize) : ringBuffer(std::vector<BufferEntry>(initialSize))
{
    ringBuffer[0].setStateOfFirstIndex();
}

// Todo: rename to something more appropriate <-- single buffer can be found
SequenceRingBuffer::ClaimingSearchResult SequenceRingBuffer::searchAndTryClaimLeadingSTuple(const SequenceNumberType sequenceNumber)
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

SequenceRingBuffer::ClaimingSearchResult
SequenceRingBuffer::searchAndTryClaimLeadingAndTrailingSTuple(const SequenceNumberType sequenceNumber)
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

void SequenceRingBuffer::claimSTupleBuffers(const size_t sTupleStartSN, const std::span<StagedBuffer> spanningTupleBuffers)
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

bool SequenceRingBuffer::trySetNewBufferWithDelimiter(const size_t sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / ringBuffer.size()) + 1;
    const auto rbIdxOfSN = sequenceNumber % ringBuffer.size();
    return ringBuffer[rbIdxOfSN].trySetWithDelimiter(abaItNumber, indexedRawBuffer);
}
bool SequenceRingBuffer::trySetNewBufferWithOutDelimiter(const size_t sequenceNumber, const StagedBuffer& indexedRawBuffer)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / ringBuffer.size()) + 1;
    const auto rbIdxOfSN = sequenceNumber % ringBuffer.size();
    return ringBuffer[rbIdxOfSN].trySetWithoutDelimiter(abaItNumber, indexedRawBuffer);
}

bool SequenceRingBuffer::validate() const
{
    bool isValid = true;
    const auto lastIdxOfRB = ringBuffer.size() - 1;
    for (const auto& [idx, metaData] : ringBuffer | NES::views::enumerate)
    {
        isValid &= metaData.validateFinalState(idx, ringBuffer.at((idx + 1) % ringBuffer.size()), lastIdxOfRB);
    }
    return isValid;
}

std::ostream& operator<<(std::ostream& os, const SequenceRingBuffer& sequenceRingBuffer)
{
    return os << fmt::format("RingBuffer({})", sequenceRingBuffer.ringBuffer.size());
}


std::pair<SequenceNumberType, uint32_t> SequenceRingBuffer::getBufferIdxAndABAItNo(const SequenceNumberType sequenceNumber) const
{
    const auto sequenceNumberBufferIdx = sequenceNumber % ringBuffer.size();
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / ringBuffer.size()) + 1;
    return std::make_pair(sequenceNumberBufferIdx, abaItNumber);
}

std::optional<size_t> SequenceRingBuffer::searchLeading(const size_t searchStartBufferIdx, const size_t abaItNumber) const
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

std::optional<size_t> SequenceRingBuffer::searchTrailing(const size_t searchStartBufferIdx, const size_t abaItNumber) const
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

SequenceRingBuffer::ClaimedSpanningTuple SequenceRingBuffer::claimingLeadingDelimiterSearch(const SequenceNumberType spanningTupleEndSN)
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

SequenceRingBuffer::ClaimedSpanningTuple SequenceRingBuffer::claimingTrailingDelimiterSearch(const SequenceNumberType sTupleStartSN)
{
    return claimingTrailingDelimiterSearch(sTupleStartSN, sTupleStartSN);
}

SequenceRingBuffer::ClaimedSpanningTuple
SequenceRingBuffer::claimingTrailingDelimiterSearch(const SequenceNumberType sTupleStartSN, const SequenceNumberType searchStartSN)
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
