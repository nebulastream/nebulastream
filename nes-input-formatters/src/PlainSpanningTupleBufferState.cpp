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

#include <PlainSpanningTupleBufferState.hpp>

#include <cstddef>
#include <limits>
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
///==------------------------------------------- Plain State ------------------------------------------------==//
///==--------------------------------------------------------------------------------------------------------==//
bool PlainState::tryClaimSpanningTuple(const ABAItNo abaItNumber)
{
    const auto current = getState();
    if (current.getABAItNo() == abaItNumber and not current.hasClaimedSpanningTuple())
    {
        state |= claimedSpanningTupleBit;
        return true;
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const PlainState& plainState)
{
    const auto loadedBitmap = plainState.getState();
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
bool PlainSpanningTupleBufferEntry::isCurrentEntryUsedUp(const ABAItNo abaItNumber) const
{
    const auto currentEntry = this->plainState.getState();
    const auto currentABAItNo = currentEntry.getABAItNo();
    const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
    const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
    const auto priorEntryIsUsed
        = currentABAItNo.getRawValue() == (abaItNumber.getRawValue() - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
    return priorEntryIsUsed;
}

void PlainSpanningTupleBufferEntry::setBuffersAndOffsets(const StagedBuffer& indexedBuffer)
{
    this->leadingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->trailingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
    this->firstDelimiterOffset = indexedBuffer.getOffsetOfLastTuple();
    this->lastDelimiterOffset = indexedBuffer.getByteOffsetOfLastTuple();
}

bool PlainSpanningTupleBufferEntry::trySetWithDelimiter(const ABAItNo abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        if (indexedBuffer.getByteOffsetOfLastTuple() != std::numeric_limits<FieldIndex>::max())
        {
            this->plainState.setHasTupleDelimiterAndValidTrailingSpanningTupleState(abaItNumber);
        }
        else
        {
            this->plainState.setHasTupleDelimiterState(abaItNumber);
        }
        return true;
    }
    return false;
}

bool PlainSpanningTupleBufferEntry::trySetWithoutDelimiter(const ABAItNo abaItNumber, const StagedBuffer& indexedBuffer)
{
    if (isCurrentEntryUsedUp(abaItNumber))
    {
        setBuffersAndOffsets(indexedBuffer);
        this->plainState.setNoTupleDelimiterState(abaItNumber);
        return true;
    }
    return false;
}

std::optional<StagedBuffer> PlainSpanningTupleBufferEntry::tryClaimSpanningTuple(const ABAItNo abaItNumber)
{
    if (this->plainState.tryClaimSpanningTuple(abaItNumber))
    {
        INVARIANT(this->trailingBufferRef.getReferenceCounter() != 0, "Tried to claim a trailing buffer with a nullptr");
        const auto stagedBuffer
            = StagedBuffer(RawTupleBuffer{std::move(this->trailingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
        this->plainState.setUsedTrailingBuffer();
        return {stagedBuffer};
    }
    return std::nullopt;
}

void PlainSpanningTupleBufferEntry::setStateOfFirstIndex(TupleBuffer dummyBuffer)
{
    this->trailingBufferRef = std::move(dummyBuffer);
    this->plainState.setStateOfFirstEntry();
    this->firstDelimiterOffset = 0;
    this->lastDelimiterOffset = 0;
}

void PlainSpanningTupleBufferEntry::setOffsetOfTrailingSpanningTuple(const FieldIndex offsetOfLastTuple)
{
    PRECONDITION(offsetOfLastTuple != std::numeric_limits<FieldIndex>::max(), "offsetOfLastTuple is not valid");
    PRECONDITION(this->lastDelimiterOffset == std::numeric_limits<FieldIndex>::max(), "Must not overwrite an already valid offset");
    this->lastDelimiterOffset = offsetOfLastTuple;
    this->plainState.setHasValidLastDelimiterOffset();
}

void PlainSpanningTupleBufferEntry::claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getReferenceCounter() != 0, "Tried to claim a leading buffer with a nullptr");
    INVARIANT(this->trailingBufferRef.getReferenceCounter() != 0, "Tried to claim a trailing buffer with a nullptr");

    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->trailingBufferRef = NES::TupleBuffer{};

    this->plainState.setUsedLeadingAndTrailingBuffer();
}

void PlainSpanningTupleBufferEntry::claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, const size_t spanningTupleIdx)
{
    INVARIANT(this->leadingBufferRef.getReferenceCounter() != 0, "Tried to claim a leading buffer with a nullptr");
    spanningTupleVector[spanningTupleIdx]
        = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
    this->plainState.setUsedLeadingBuffer();
}

PlainSpanningTupleBufferEntry::EntryState PlainSpanningTupleBufferEntry::getEntryState(const ABAItNo expectedABAItNo) const
{
    const auto currentState = this->plainState.getState();
    const bool isCorrectABA = expectedABAItNo == currentState.getABAItNo();
    const bool hasDelimiter = currentState.hasTupleDelimiter();
    const bool validLastDelimiterOffset = currentState.hasValidLastDelimiterOffset();
    return EntryState{
        .hasCorrectABA = isCorrectABA, .hasDelimiter = hasDelimiter, .hasValidTrailingDelimiterOffset = validLastDelimiterOffset};
}

bool PlainSpanningTupleBufferEntry::validateFinalState(
    const SpanningTupleBufferIdx bufferIdx,
    const PlainSpanningTupleBufferEntry& nextEntry,
    const SpanningTupleBufferIdx lastIdxOfBuffer) const
{
    bool isValidFinalState = true;
    const auto state = this->plainState.getState();
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

    if (state.getABAItNo().getRawValue() + static_cast<size_t>(bufferIdx == lastIdxOfBuffer)
        == nextEntry.plainState.getABAItNo().getRawValue())
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
