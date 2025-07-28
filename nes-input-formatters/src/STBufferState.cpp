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

#include <STBufferState.hpp>

#include <cstddef>
#include <optional>
#include <ostream>
#include <span>
#include <utility>
#include <fmt/format.h>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{
///==--------------------------------------------------------------------------------------------------------==//
///==------------------------------------------- Atomic State -----------------------------------------------==//
///==--------------------------------------------------------------------------------------------------------==//
bool AtomicState::tryClaimSpanningTuple(const ABAItNo abaItNumber)
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

std::ostream& operator<<(std::ostream& os, const AtomicState& atomicBitmapState)
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

///==--------------------------------------------------------------------------------------------------------==//
///==------------------------------------------- Buffer Entry -----------------------------------------------==//
///==--------------------------------------------------------------------------------------------------------==//
bool STBufferEntry::isCurrentEntryUsedUp(const ABAItNo abaItNumber) const
{
    const auto currentEntry = this->atomicState.getState();
    const auto currentABAItNo = currentEntry.getABAItNo();
    const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
    const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
    const auto priorEntryIsUsed
        = currentABAItNo.getRawValue() == (abaItNumber.getRawValue() - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
    return priorEntryIsUsed;
}

void STBufferEntry::setBuffersAndOffsets(const StagedBuffer& indexedBuffer)
{
    this->leadingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->trailingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->firstDelimiterOffset = indexedBuffer.getOffsetOfFirstTupleDelimiter();
    this->lastDelimiterOffset = indexedBuffer.getOffsetOfLastTupleDelimiter();
}

bool STBufferEntry::trySetWithDelimiter(const ABAItNo abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicState.setHasTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

bool STBufferEntry::trySetWithoutDelimiter(const ABAItNo abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicState.setNoTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

std::optional<StagedBuffer> STBufferEntry::tryClaimSpanningTuple(const ABAItNo abaItNumber)
{
    if (this->atomicState.tryClaimSpanningTuple(abaItNumber))
    {
        INVARIANT(this->trailingBufferRef.getReferenceCounter() != 0, "Tried to claim a trailing buffer with a nullptr");
        const auto stagedBuffer
            = StagedBuffer(RawTupleBuffer{std::move(this->trailingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
        this->atomicState.setUsedTrailingBuffer();
        return {stagedBuffer};
    }
    return std::nullopt;
}

void STBufferEntry::setStateOfFirstIndex(TupleBuffer dummyBuffer)
{
    /// The first entry is a dummy that makes sure that we can resolve the first tuple in the first buffer
    this->trailingBufferRef = std::move(dummyBuffer);
    this->atomicState.setStateOfFirstEntry();
    this->firstDelimiterOffset = 0;
    this->lastDelimiterOffset = 0;
}

void STBufferEntry::claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getReferenceCounter() != 0, "Tried to claim a leading buffer with a nullptr");
    INVARIANT(this->trailingBufferRef.getReferenceCounter() != 0, "Tried to claim a trailing buffer with a nullptr");

    /// First claim buffer uses
    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->trailingBufferRef = NES::TupleBuffer{};

    /// Then atomically mark buffer as used
    this->atomicState.setUsedLeadingAndTrailingBuffer();
}

void STBufferEntry::claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getReferenceCounter() != 0, "Tried to claim a leading buffer with a nullptr");
    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->atomicState.setUsedLeadingBuffer();
}

STBufferEntry::EntryState STBufferEntry::getEntryState(const ABAItNo expectedABAItNo) const
{
    const auto currentState = this->atomicState.getState();
    const bool isCorrectABA = expectedABAItNo == currentState.getABAItNo();
    const bool hasDelimiter = currentState.hasTupleDelimiter();
    return EntryState{.hasCorrectABA = isCorrectABA, .hasDelimiter = hasDelimiter};
}

bool STBufferEntry::validateFinalState(const STBufferIdx bufferIdx, const STBufferEntry& nextEntry, const STBufferIdx lastIdxOfBuffer) const
{
    bool isValidFinalState = true;
    const auto state = this->atomicState.getState();
    if (not state.hasUsedLeadingBuffer())
    {
        isValidFinalState = false;
        NES_ERROR("Buffer at index {} does still claim to own leading buffer", bufferIdx);
    }
    if (this->leadingBufferRef.getReferenceCounter() != 0)
    {
        isValidFinalState = false;
        NES_ERROR("Buffer at index {} still owns a leading buffer reference", bufferIdx);
    }

    /// Add '1' to the ABA iteration number, if the current entry is the last index of the ring buffer and the next entry wraps around
    if (state.getABAItNo().getRawValue() + static_cast<size_t>(bufferIdx == lastIdxOfBuffer)
        == nextEntry.atomicState.getABAItNo().getRawValue())
    {
        if (not state.hasUsedTrailingBuffer())
        {
            isValidFinalState = false;
            NES_ERROR("Buffer at index {} does still claim to own leading buffer", bufferIdx);
        }
        if (this->trailingBufferRef.getReferenceCounter() != 0)
        {
            isValidFinalState = false;
            NES_ERROR("Buffer at index {} still owns a trailing buffer reference", bufferIdx);
        }
    }
    return isValidFinalState;
}
}
