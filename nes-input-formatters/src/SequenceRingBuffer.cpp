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
}
