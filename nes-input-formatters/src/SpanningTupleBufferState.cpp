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

#include <SpanningTupleBufferState.hpp>

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
        /// while one thread tries to claim an SpanningTuple, by claiming the first buffer/entry,
        /// another thread may use the same buffer as the last buffer of an SpanningTuple, claiming its 'leading' use
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
bool SpanningTupleBufferEntry::isCurrentEntryUsedUp(const ABAItNo abaItNumber) const
{
    const auto currentEntry = this->atomicState.getState();
    const auto currentABAItNo = currentEntry.getABAItNo();
    const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
    const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
    const auto priorEntryIsUsed
        = currentABAItNo.getRawValue() == (abaItNumber.getRawValue() - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
    return priorEntryIsUsed;
}

void SpanningTupleBufferEntry::setBuffersAndOffsets(const StagedBuffer& indexedBuffer)
{
    this->leadingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->trailingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->firstDelimiterOffset = indexedBuffer.getOffsetOfLastTuple();
    this->lastDelimiterOffset = indexedBuffer.getByteOffsetOfLastTuple();
}

bool SpanningTupleBufferEntry::trySetWithDelimiter(const ABAItNo abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicState.setHasTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

bool SpanningTupleBufferEntry::trySetWithoutDelimiter(const ABAItNo abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->atomicState.setNoTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

std::optional<StagedBuffer> SpanningTupleBufferEntry::tryClaimSpanningTuple(const ABAItNo abaItNumber)
{
    if (this->atomicState.tryClaimSpanningTuple(abaItNumber))
    {
        /// The start-of-stream sentinel (very first entry, first iteration) owns no backing buffer (null data pointer). It
        /// anchors the first leading spanning tuple but contributes no bytes, so we hand back an empty StagedBuffer for it.
        if (!this->trailingBufferRef)
        {
            this->atomicState.setUsedTrailingBuffer();
            return {StagedBuffer{}};
        }
        const auto stagedBuffer
            = StagedBuffer(RawTupleBuffer{std::move(this->trailingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
        this->atomicState.setUsedTrailingBuffer();
        return {stagedBuffer};
    }
    return std::nullopt;
}

void SpanningTupleBufferEntry::setStateOfFirstIndex()
{
    /// The first entry is a start-of-stream sentinel. It owns no buffer; it only carries the atomic state that lets the
    /// SpanningTupleBuffer resolve the first tuple in the first buffer (acting as a virtual leading delimiter).
    this->atomicState.setStateOfFirstEntry();
    this->firstDelimiterOffset = 0;
    this->lastDelimiterOffset = 0;
}

void SpanningTupleBufferEntry::claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
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

void SpanningTupleBufferEntry::claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getReferenceCounter() != 0, "Tried to claim a leading buffer with a nullptr");
    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->atomicState.setUsedLeadingBuffer();
}

SpanningTupleBufferEntry::EntryState SpanningTupleBufferEntry::getEntryState(const ABAItNo expectedABAItNo) const
{
    const auto currentState = this->atomicState.getState();
    return EntryState{.hasCorrectABA = expectedABAItNo == currentState.getABAItNo(), .hasDelimiter = currentState.hasTupleDelimiter()};
}

bool SpanningTupleBufferEntry::validateFinalState(
    const SpanningTupleBufferIdx bufferIdx, const SpanningTupleBufferEntry& nextEntry, const SpanningTupleBufferIdx lastIdxOfBuffer) const
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
