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

#include <FragmentSpanningTupleBufferState.hpp>

#include <cstddef>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <utility>

#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <RawTupleBuffer.hpp>
#include <SpanningTupleBufferState.hpp>

namespace NES
{

bool FragmentSpanningTupleBufferEntry::isCurrentEntryUsedUp(const ABAItNo abaItNumber) const
{
    const auto currentEntry = this->atomicState.getState();
    const auto currentABAItNo = currentEntry.getABAItNo();
    const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
    const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
    const auto priorEntryIsUsed
        = currentABAItNo.getRawValue() == (abaItNumber.getRawValue() - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
    return priorEntryIsUsed;
}

bool FragmentSpanningTupleBufferEntry::trySetWithDelimiter(
    const ABAItNo abaItNumber, const StagedBuffer& indexedBuffer, const size_t delimiterSize, const size_t maxFragmentBytes)
{
    if (not isCurrentEntryUsedUp(abaItNumber))
    {
        return false;
    }

    const auto rawView = indexedBuffer.getBufferView();
    const auto offsetOfFirstDelimiter = indexedBuffer.getOffsetOfLastTuple();
    const auto offsetOfLastDelimiter = indexedBuffer.getByteOffsetOfLastTuple();
    this->firstDelimiterOffset = offsetOfFirstDelimiter;
    this->lastDelimiterOffset = offsetOfLastDelimiter;

    /// HEAD side (BACK of a leading spanning tuple): copy [0, firstDelimiterOffset) if it is a valid, small range.
    /// Tests may smuggle metadata through the offsets of mocked buffers; out-of-range offsets fall back to pinning,
    /// which reproduces the classic entry's behavior exactly (the shredder itself never dereferences the bytes).
    if (offsetOfFirstDelimiter <= rawView.size() and offsetOfFirstDelimiter <= maxFragmentBytes)
    {
        this->leadingFragment.assign(rawView.data(), offsetOfFirstDelimiter);
        this->leadingFallbackRef = TupleBuffer{};
    }
    else
    {
        this->leadingFragment.clear();
        this->leadingFallbackRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    }

    /// TAIL side (FRONT of a trailing spanning tuple): copy (lastDelimiterOffset + delimiterSize, end) if the
    /// offset is already known (eager indexers). The lazy-offset path (offset == max, set later via
    /// setOffsetOfTrailingSpanningTuple) must pin: a concurrent claimer may consume the entry before this
    /// thread learns the offset, so the bytes have to stay reachable through the buffer itself.
    const auto hasValidLastDelimiterOffset = offsetOfLastDelimiter != std::numeric_limits<FieldIndex>::max();
    const auto tailStart = static_cast<size_t>(offsetOfLastDelimiter) + delimiterSize;
    if (hasValidLastDelimiterOffset and tailStart <= rawView.size() and (rawView.size() - tailStart) <= maxFragmentBytes)
    {
        this->trailingFragment.assign(rawView.substr(tailStart));
        this->trailingFallbackRef = TupleBuffer{};
    }
    else
    {
        this->trailingFragment.clear();
        this->trailingFallbackRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    }

    /// The atomic store below publishes the fragments/refs written above (same happens-before as the classic entry)
    if (hasValidLastDelimiterOffset)
    {
        this->atomicState.setHasTupleDelimiterAndValidTrailingSpanningTupleState(abaItNumber);
    }
    else
    {
        this->atomicState.setHasTupleDelimiterState(abaItNumber);
    }
    return true;
}

bool FragmentSpanningTupleBufferEntry::trySetWithoutDelimiter(const ABAItNo abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (not isCurrentEntryUsedUp(abaItNumber))
    {
        return false;
    }
    /// Interior buffers contribute their WHOLE content to the spanning tuple; copying would defeat the purpose,
    /// so they stay pinned exactly like in the classic entry (both roles reference the same buffer)
    this->leadingFragment.clear();
    this->trailingFragment.clear();
    this->leadingFallbackRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->trailingFallbackRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->firstDelimiterOffset = indexedBuffer.getOffsetOfLastTuple();
    this->lastDelimiterOffset = indexedBuffer.getByteOffsetOfLastTuple();
    this->atomicState.setNoTupleDelimiterState(abaItNumber);
    return true;
}

std::optional<StagedBuffer> FragmentSpanningTupleBufferEntry::tryClaimSpanningTuple(const ABAItNo abaItNumber)
{
    if (this->atomicState.tryClaimSpanningTuple(abaItNumber))
    {
        std::optional<StagedBuffer> stagedBuffer;
        if (this->trailingFallbackRef.getReferenceCounter() != 0)
        {
            stagedBuffer = StagedBuffer(RawTupleBuffer{std::move(this->trailingFallbackRef)}, firstDelimiterOffset, lastDelimiterOffset);
        }
        else
        {
            stagedBuffer = StagedBuffer::fromOwnedFragment(std::move(this->trailingFragment), firstDelimiterOffset, lastDelimiterOffset);
            this->trailingFragment.clear();
        }
        this->atomicState.setUsedTrailingBuffer();
        return stagedBuffer;
    }
    return std::nullopt;
}

void FragmentSpanningTupleBufferEntry::setStateOfFirstIndex()
{
    /// The first entry is a dummy that makes sure that we can resolve the first tuple in the first buffer.
    /// An empty owned trailing fragment plays the role of the classic dummy TupleBuffer (its TAIL bytes are empty).
    this->leadingFragment.clear();
    this->trailingFragment.clear();
    this->leadingFallbackRef = TupleBuffer{};
    this->trailingFallbackRef = TupleBuffer{};
    this->atomicState.setStateOfFirstEntry();
    this->firstDelimiterOffset = 0;
    this->lastDelimiterOffset = 0;
}

void FragmentSpanningTupleBufferEntry::setOffsetOfTrailingSpanningTuple(const FieldIndex offsetOfLastTuple)
{
    PRECONDITION(offsetOfLastTuple != std::numeric_limits<FieldIndex>::max(), "offsetOfLastTuple is not valid");
    PRECONDITION(this->lastDelimiterOffset == std::numeric_limits<FieldIndex>::max(), "Must not overwrite an already valid offset");
    /// @NOTE: the order is important! The 'lastDelimiterOffset' must be valid before calling 'setHasValidLastDelimiterOffset'
    /// The trailing role keeps the buffer pinned (see trySetWithDelimiter); only the offset becomes visible here
    this->lastDelimiterOffset = offsetOfLastTuple;
    this->atomicState.setHasValidLastDelimiterOffset();
}

void FragmentSpanningTupleBufferEntry::claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingFallbackRef.getReferenceCounter() != 0, "Tried to claim a leading buffer with a nullptr");
    INVARIANT(this->trailingFallbackRef.getReferenceCounter() != 0, "Tried to claim a trailing buffer with a nullptr");

    /// First claim buffer uses
    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingFallbackRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->trailingFallbackRef = NES::TupleBuffer{};

    /// Then atomically mark buffer as used
    this->atomicState.setUsedLeadingAndTrailingBuffer();
}

void FragmentSpanningTupleBufferEntry::claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    if (this->leadingFallbackRef.getReferenceCounter() != 0)
    {
        spanningTupleVector[spanningTupleIdx]
            = StagedBuffer(RawTupleBuffer{std::move(this->leadingFallbackRef)}, firstDelimiterOffset, lastDelimiterOffset);
    }
    else
    {
        spanningTupleVector[spanningTupleIdx]
            = StagedBuffer::fromOwnedFragment(std::move(this->leadingFragment), firstDelimiterOffset, lastDelimiterOffset);
        this->leadingFragment.clear();
    }
    this->atomicState.setUsedLeadingBuffer();
}

FragmentSpanningTupleBufferEntry::EntryState FragmentSpanningTupleBufferEntry::getEntryState(const ABAItNo expectedABAItNo) const
{
    const auto currentState = this->atomicState.getState();
    const bool isCorrectABA = expectedABAItNo == currentState.getABAItNo();
    const bool hasDelimiter = currentState.hasTupleDelimiter();
    const bool validLastDelimiterOffset = currentState.hasValidLastDelimiterOffset();
    return EntryState{
        .hasCorrectABA = isCorrectABA, .hasDelimiter = hasDelimiter, .hasValidTrailingDelimiterOffset = validLastDelimiterOffset};
}

bool FragmentSpanningTupleBufferEntry::validateFinalState(
    const SpanningTupleBufferIdx bufferIdx,
    const FragmentSpanningTupleBufferEntry& nextEntry,
    const SpanningTupleBufferIdx lastIdxOfBuffer) const
{
    bool isValidFinalState = true;
    const auto state = this->atomicState.getState();
    if (not state.hasUsedLeadingBuffer())
    {
        isValidFinalState = false;
        NES_ERROR("Buffer at index {} does still claim to own leading buffer", bufferIdx);
    }
    if (this->leadingFallbackRef.getReferenceCounter() != 0)
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
            NES_ERROR("Buffer at index {} does still claim to own trailing buffer", bufferIdx);
        }
        if (this->trailingFallbackRef.getReferenceCounter() != 0)
        {
            isValidFinalState = false;
            NES_ERROR("Buffer at index {} still owns a trailing buffer reference", bufferIdx);
        }
        if (not this->trailingFragment.empty())
        {
            isValidFinalState = false;
            NES_ERROR("Buffer at index {} still owns a trailing fragment", bufferIdx);
        }
    }
    return isValidFinalState;
}
}
