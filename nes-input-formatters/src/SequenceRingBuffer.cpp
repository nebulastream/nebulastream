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
//==---------------------------------------- SSMetaData -----------------------------------------------==//
//==--------------------------------------------------------------------------------------------------------==//
bool SSMetaData::isCurrentEntryUsedUp(const size_t abaItNumber) const
{
    const auto currentEntry = this->atomicBitmapState.getState();
    const auto currentABAItNo = currentEntry.getABAItNo();
    const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
    const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
    const auto priorEntryIsUsed = currentABAItNo == (abaItNumber - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
    return priorEntryIsUsed;
}

void SSMetaData::setBuffersAndOffsets(const StagedBuffer& indexedBuffer)
{
    this->leadingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->trailingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->firstDelimiterOffset = indexedBuffer.getOffsetOfFirstTupleDelimiter();
    this->lastDelimiterOffset = indexedBuffer.getOffsetOfLastTupleDelimiter();
}

bool SSMetaData::trySetWithDelimiter(const size_t abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicBitmapState.setHasTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

bool SSMetaData::trySetWithoutDelimiter(const size_t abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicBitmapState.setNoTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

std::optional<StagedBuffer> SSMetaData::tryClaimSpanningTuple(const uint32_t abaItNumber)
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

void SSMetaData::setStateOfFirstIndex()
{
    /// The first entry is a dummy that makes sure that we can resolve the first tuple in the first buffer
    this->atomicBitmapState.setStateOfFirstEntry();
    this->firstDelimiterOffset = std::numeric_limits<uint32_t>::max();
    this->lastDelimiterOffset = 0;
}

void SSMetaData::claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
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

void SSMetaData::claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getBuffer() != nullptr, "Tried to claim a leading buffer with a nullptr");
    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->atomicBitmapState.setUsedLeadingBuffer();
}

SSMetaData::EntryState SSMetaData::getEntryState(const size_t expectedABAItNo) const
{
    const auto currentState = this->atomicBitmapState.getState();
    const bool isCorrectABA = expectedABAItNo == currentState.getABAItNo();
    const bool hasDelimiter = currentState.hasTupleDelimiter();
    return EntryState{.isCorrectABA = isCorrectABA, .hasDelimiter = hasDelimiter};
}


//==--------------------------------------------------------------------------------------------------------==//
//==---------------------------------------- SEQUENCE BUFFER -----------------------------------------------==//
//==--------------------------------------------------------------------------------------------------------==//
SequenceRingBuffer::SequenceRingBuffer(const size_t initialSize) : ringBuffer(std::vector<SSMetaData>(initialSize)), bufferSize(initialSize)
{
    ringBuffer[0].setStateOfFirstIndex();
}

SequenceRingBuffer::NonClaimingSearchResult
SequenceRingBuffer::searchWithoutClaimingBuffers(const size_t pivotSN, const size_t abaItNumber, const SequenceNumberType sequenceNumber)
{
    if (const auto firstBufferSN = nonClaimingLeadingDelimiterSearch(pivotSN, abaItNumber, sequenceNumber))
    {
        /// Can't use 'claimingTrailingDelimiterSearch', since the start of the ST and start of the search (sequenceNumber+1) are different
        // Todo: could create 'claimingTrailingDelimiterSearch' that takes different vals for start of search and first SN
        // -> then overload and call more complex function from simpler version
        if (const auto secondBufferSN = nonClaimingTrailingDelimiterSearch(pivotSN, abaItNumber, sequenceNumber))
        {
            const auto sTupleStartBufferIdx = firstBufferSN.value() % bufferSize;
            const auto abaItNumberOfFirstDelimiter = static_cast<uint32_t>(firstBufferSN.value() / bufferSize) + 1;
            return NonClaimingSearchResult{
                .state = NonClaimingSearchResult::State::LEADING_AND_TRAILING_ST,
                .startBuffer = ringBuffer[sTupleStartBufferIdx].tryClaimSpanningTuple(abaItNumberOfFirstDelimiter),
                .startBufferSN = firstBufferSN.value(),
                .endBufferSN = secondBufferSN.value()};
        }
    }
    return NonClaimingSearchResult{.state = NonClaimingSearchResult::State::NONE};
}

SequenceRingBuffer::ClaimingSearchResult
SequenceRingBuffer::searchAndClaimBuffers(const size_t pivotSN, const size_t abaItNumber, const SequenceNumberType sequenceNumber)
{
    const auto [firstSTStartBuffer, firstSTFinalSN] = claimingLeadingDelimiterSearch(pivotSN, abaItNumber, sequenceNumber);
    const auto [secondSTStartBuffer, secondSTFinalSN] = claimingTrailingDelimiterSearch(pivotSN, abaItNumber, sequenceNumber);
    const auto rangeState = static_cast<ClaimingSearchResult::State>(
        (static_cast<uint8_t>(secondSTStartBuffer.has_value()) << 1UL) + static_cast<uint8_t>(firstSTStartBuffer.has_value()));
    return ClaimingSearchResult{
        .state = rangeState,
        .leadingStartBuffer = std::move(firstSTStartBuffer),
        .trailingStartBuffer = std::move(secondSTStartBuffer),
        .leadingStartSN = firstSTFinalSN,
        .trailingStartSN = secondSTFinalSN};
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

bool SequenceRingBuffer::trySetNewBufferWithDelimiter(
    const size_t rbIdxOfSN, const uint32_t abaItNumber, const StagedBuffer& indexedRawBuffer)
{
    return ringBuffer[rbIdxOfSN].trySetWithDelimiter(abaItNumber, indexedRawBuffer);
}
bool SequenceRingBuffer::trySetNewBufferWithOutDelimiter(
    const size_t rbIdxOfSN, const uint32_t abaItNumber, const StagedBuffer& indexedRawBuffer)
{
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


std::pair<SSMetaData::EntryState, size_t> SequenceRingBuffer::searchLeading(const size_t rbIdxOfSN, const size_t abaItNumber) const
{
    size_t leadingDistance = 1;
    auto isPriorIteration = static_cast<size_t>(rbIdxOfSN < leadingDistance);
    auto cellState = this->ringBuffer[(rbIdxOfSN - leadingDistance) % bufferSize].getEntryState(abaItNumber - isPriorIteration);
    while (cellState.isCorrectABA and not(cellState.hasDelimiter))
    {
        ++leadingDistance;
        isPriorIteration = static_cast<size_t>(rbIdxOfSN < leadingDistance);
        cellState = this->ringBuffer[(rbIdxOfSN - leadingDistance) % bufferSize].getEntryState(abaItNumber - isPriorIteration);
    }
    return std::make_pair(cellState, leadingDistance);
}

std::optional<uint32_t> SequenceRingBuffer::nonClaimingLeadingDelimiterSearch(
    const size_t rbIdxOfSN, const size_t abaItNumber, const SequenceNumberType sequenceNumber)
{
    /// Assumes spanningTupleStartSN as the start of a potential spanning tuple.
    const auto [cellState, leadingDistance] = searchLeading(rbIdxOfSN, abaItNumber);
    return (cellState.isCorrectABA) ? std::optional{sequenceNumber - leadingDistance} : std::nullopt;
}

SequenceRingBuffer::ClaimedSpanningTuple SequenceRingBuffer::claimingLeadingDelimiterSearch(
    const size_t rbIdxOfSN, const size_t abaItNumber, const SequenceNumberType spanningTupleEndSN)
{
    if (const auto [cellState, leadingDistance] = searchLeading(rbIdxOfSN, abaItNumber); cellState.isCorrectABA)
    {
        const auto sTupleSN = spanningTupleEndSN - leadingDistance;
        const auto sTupleIdx = sTupleSN % bufferSize;
        const auto isPriorIteration = static_cast<size_t>(rbIdxOfSN < leadingDistance);
        return ClaimedSpanningTuple{
            .firstBuffer = ringBuffer[sTupleIdx].tryClaimSpanningTuple(abaItNumber - isPriorIteration),
            .snOfLastBuffer = sTupleSN,
        };
    }
    return ClaimedSpanningTuple{std::nullopt, 0};
}

std::pair<SSMetaData::EntryState, size_t> SequenceRingBuffer::searchTrailing(const size_t rbIdxOfSN, const size_t abaItNumber)
{
    size_t trailingDistance = 1;
    auto isNextIteration = static_cast<size_t>((rbIdxOfSN + trailingDistance) >= bufferSize);
    auto cellState = this->ringBuffer[(rbIdxOfSN + trailingDistance) % bufferSize].getEntryState(abaItNumber + isNextIteration);
    while (cellState.isCorrectABA and not(cellState.hasDelimiter))
    {
        ++trailingDistance;
        isNextIteration = static_cast<size_t>((rbIdxOfSN + trailingDistance) >= bufferSize);
        cellState = this->ringBuffer[(rbIdxOfSN + trailingDistance) % bufferSize].getEntryState(abaItNumber + isNextIteration);
    }
    return std::make_pair(cellState, trailingDistance);
}

std::optional<uint32_t> SequenceRingBuffer::nonClaimingTrailingDelimiterSearch(
    const size_t rbIdxOfSN, const size_t abaItNumber, const SequenceNumberType currentSequenceNumber)
{
    const auto [cellState, trailingDistance] = searchTrailing(rbIdxOfSN, abaItNumber);
    return (cellState.isCorrectABA) ? std::optional{currentSequenceNumber + trailingDistance} : std::nullopt;
}

SequenceRingBuffer::ClaimedSpanningTuple SequenceRingBuffer::claimingTrailingDelimiterSearch(
    const size_t spanningTupleStartIdx, const size_t abaItNumberSpanningTupleStart, const SequenceNumberType spanningTupleStartSN)
{
    if (const auto [cellState, trailingDistance] = searchTrailing(spanningTupleStartIdx, abaItNumberSpanningTupleStart);
        cellState.isCorrectABA)
    {
        const auto leadingSequenceNumber = spanningTupleStartSN + trailingDistance;
        return ClaimedSpanningTuple{
            .firstBuffer = ringBuffer[spanningTupleStartIdx].tryClaimSpanningTuple(abaItNumberSpanningTupleStart),
            .snOfLastBuffer = leadingSequenceNumber,
        };
    }
    return ClaimedSpanningTuple{std::nullopt, 0};
};
}
