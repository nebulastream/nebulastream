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

#include <UncompiledSTBufferState.hpp>

#include <cstddef>
#include <optional>
#include <ostream>
#include <span>
#include <utility>
#include <fmt/format.h>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <UncompiledRawTupleBuffer.hpp>

namespace NES
{
///==--------------------------------------------------------------------------------------------------------==//
///==------------------------------------------- Atomic State -----------------------------------------------==//
///==--------------------------------------------------------------------------------------------------------==//
bool UncompiledAtomicState::tryClaimSpanningTuple(const UncompiledABAItNo abaItNumber)
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

std::ostream& operator<<(std::ostream& os, const UncompiledAtomicState& atomicBitmapState)
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
bool UncompiledSTBufferEntry::isCurrentEntryUsedUp(const UncompiledABAItNo abaItNumber) const
{
    const auto currentEntry = this->atomicState.getState();
    const auto currentABAItNo = currentEntry.getABAItNo();
    const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
    const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
    const auto priorEntryIsUsed
        = currentABAItNo.getRawValue() == (abaItNumber.getRawValue() - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
    return priorEntryIsUsed;
}

void UncompiledSTBufferEntry::setBuffersAndOffsets(const UncompiledStagedBuffer& indexedBuffer)
{
    this->leadingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->trailingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->firstDelimiterOffset = indexedBuffer.getOffsetOfFirstTupleDelimiter();
    this->lastDelimiterOffset = indexedBuffer.getOffsetOfLastTupleDelimiter();
}

bool UncompiledSTBufferEntry::trySetWithDelimiter(const UncompiledABAItNo abaItNumber, const UncompiledStagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicState.setHasTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

bool UncompiledSTBufferEntry::trySetWithoutDelimiter(const UncompiledABAItNo abaItNumber, const UncompiledStagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicState.setNoTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

std::optional<UncompiledStagedBuffer> UncompiledSTBufferEntry::tryClaimSpanningTuple(const UncompiledABAItNo abaItNumber)
{
    if (this->atomicState.tryClaimSpanningTuple(abaItNumber))
    {
        INVARIANT(this->trailingBufferRef.getReferenceCounter() != 0, "Tried to claim a trailing buffer with a nullptr");
        const auto stagedBuffer = UncompiledStagedBuffer(
            UncompiledRawTupleBuffer{std::move(this->trailingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
        this->atomicState.setUsedTrailingBuffer();
        return {stagedBuffer};
    }
    return std::nullopt;
}

void UncompiledSTBufferEntry::setStateOfFirstIndex(TupleBuffer dummyBuffer)
{
    /// The first entry is a dummy that makes sure that we can resolve the first tuple in the first buffer
    this->trailingBufferRef = std::move(dummyBuffer);
    this->atomicState.setStateOfFirstEntry();
    this->firstDelimiterOffset = 0;
    this->lastDelimiterOffset = 0;
}

void UncompiledSTBufferEntry::claimNoDelimiterBuffer(std::span<UncompiledStagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getReferenceCounter() != 0, "Tried to claim a leading buffer with a nullptr");
    INVARIANT(this->trailingBufferRef.getReferenceCounter() != 0, "Tried to claim a trailing buffer with a nullptr");

    /// First claim buffer uses
    spanningTupleVector[spanningTupleIdx]
        = UncompiledStagedBuffer(UncompiledRawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->trailingBufferRef = NES::TupleBuffer{};

    /// Then atomically mark buffer as used
    this->atomicState.setUsedLeadingAndTrailingBuffer();
}

void UncompiledSTBufferEntry::claimLeadingBuffer(std::span<UncompiledStagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getReferenceCounter() != 0, "Tried to claim a leading buffer with a nullptr");
    spanningTupleVector[spanningTupleIdx]
        = UncompiledStagedBuffer(UncompiledRawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->atomicState.setUsedLeadingBuffer();
}

UncompiledSTBufferEntry::EntryState UncompiledSTBufferEntry::getEntryState(const UncompiledABAItNo expectedABAItNo) const
{
    const auto currentState = this->atomicState.getState();
    const bool isCorrectABA = expectedABAItNo == currentState.getABAItNo();
    const bool hasDelimiter = currentState.hasTupleDelimiter();
    return EntryState{.hasCorrectABA = isCorrectABA, .hasDelimiter = hasDelimiter};
}

bool UncompiledSTBufferEntry::validateFinalState(
    const UncompiledSTBufferIdx bufferIdx, const UncompiledSTBufferEntry& nextEntry, const UncompiledSTBufferIdx lastIdxOfBuffer) const
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
