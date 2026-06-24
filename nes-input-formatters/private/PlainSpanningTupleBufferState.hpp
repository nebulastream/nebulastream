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

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <span>

#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <RawTupleBuffer.hpp>
#include <SpanningTupleBufferState.hpp>

namespace NES
{

/// Non-atomic state for use behind a mutex. Same bitmap layout as AtomicState but uses a plain uint64_t.
class PlainState
{
    friend class PlainSpanningTupleBufferEntry;

    static constexpr uint64_t hasTupleDelimiterBit = (1ULL << 32ULL); /// NOLINT(readability-magic-numbers)
    static constexpr uint64_t claimedSpanningTupleBit = (1ULL << 33ULL); /// NOLINT(readability-magic-numbers)
    static constexpr uint64_t usedLeadingBufferBit = (1ULL << 34ULL); /// NOLINT(readability-magic-numbers)
    static constexpr uint64_t usedTrailingBufferBit = (1ULL << 35ULL); /// NOLINT(readability-magic-numbers)
    static constexpr uint64_t hasValidLastDelimiterOffsetBit = (1ULL << 36ULL); /// NOLINT(readability-magic-numbers)
    static constexpr uint64_t usedLeadingAndTrailingBufferBits = (usedLeadingBufferBit | usedTrailingBufferBit);

    static constexpr uint64_t defaultState = (0ULL | claimedSpanningTupleBit | usedLeadingBufferBit | usedTrailingBufferBit);
    static constexpr uint64_t firstEntryDummy = (1ULL | hasTupleDelimiterBit | usedLeadingBufferBit | hasValidLastDelimiterOffsetBit);

    class BitmapState
    {
    public:
        explicit BitmapState(const uint64_t state) : state(state) { };

        [[nodiscard]] ABAItNo getABAItNo() const { return ABAItNo{static_cast<uint32_t>(state)}; }

        [[nodiscard]] bool hasTupleDelimiter() const { return (state & hasTupleDelimiterBit) == hasTupleDelimiterBit; }

        [[nodiscard]] bool hasUsedLeadingBuffer() const { return (state & usedLeadingBufferBit) == usedLeadingBufferBit; }

        [[nodiscard]] bool hasUsedTrailingBuffer() const { return (state & usedTrailingBufferBit) == usedTrailingBufferBit; }

        [[nodiscard]] bool hasClaimedSpanningTuple() const { return (state & claimedSpanningTupleBit) == claimedSpanningTupleBit; }

        [[nodiscard]] bool hasValidLastDelimiterOffset() const
        {
            return (state & hasValidLastDelimiterOffsetBit) == hasValidLastDelimiterOffsetBit;
        }

    private:
        friend class PlainState;

        [[nodiscard]] uint64_t getBitmapState() const { return state; }

        uint64_t state;
    };

    PlainState() : state(defaultState) { };
    ~PlainState() = default;

    /// Under a mutex, claiming is a simple check-and-set (no CAS loop needed)
    bool tryClaimSpanningTuple(ABAItNo abaItNumber);

    [[nodiscard]] ABAItNo getABAItNo() const { return ABAItNo{static_cast<uint32_t>(state)}; }

    [[nodiscard]] BitmapState getState() const { return BitmapState{this->state}; }

    void setStateOfFirstEntry() { this->state = firstEntryDummy; }

    void setHasTupleDelimiterState(const ABAItNo abaItNumber) { this->state = (hasTupleDelimiterBit | abaItNumber.getRawValue()); }

    void setHasTupleDelimiterAndValidTrailingSpanningTupleState(const ABAItNo abaItNumber)
    {
        this->state = (hasTupleDelimiterBit | hasValidLastDelimiterOffsetBit | abaItNumber.getRawValue());
    }

    void setNoTupleDelimiterState(const ABAItNo abaItNumber) { this->state = abaItNumber.getRawValue(); }

    void setUsedLeadingBuffer() { this->state |= usedLeadingBufferBit; }

    void setUsedTrailingBuffer() { this->state |= usedTrailingBufferBit; }

    void setUsedLeadingAndTrailingBuffer() { this->state |= usedLeadingAndTrailingBufferBits; }

    void setHasValidLastDelimiterOffset() { this->state |= hasValidLastDelimiterOffsetBit; }

    friend std::ostream& operator<<(std::ostream& os, const PlainState& plainState);

    uint64_t state;
};

/// Entry for the locking spanning tuple buffer. Same structure as SpanningTupleBufferEntry but uses PlainState instead of AtomicState.
class PlainSpanningTupleBufferEntry
{
public:
    struct EntryState
    {
        bool hasCorrectABA = false;
        bool hasDelimiter = false;
        bool hasValidTrailingDelimiterOffset = false;
    };

    PlainSpanningTupleBufferEntry() = default;

    void setStateOfFirstIndex(TupleBuffer dummyBuffer);

    void setOffsetOfTrailingSpanningTuple(FieldIndex offsetOfLastTuple);

    std::optional<StagedBuffer> tryClaimSpanningTuple(ABAItNo abaItNumber);

    [[nodiscard]] bool isCurrentEntryUsedUp(ABAItNo abaItNumber) const;

    void setBuffersAndOffsets(const StagedBuffer& indexedBuffer);

    bool trySetWithDelimiter(ABAItNo abaItNumber, const StagedBuffer& indexedBuffer);
    bool trySetWithoutDelimiter(ABAItNo abaItNumber, const StagedBuffer& indexedBuffer);

    void claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, size_t spanningTupleIdx);

    void claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, size_t spanningTupleIdx);

    [[nodiscard]] EntryState getEntryState(ABAItNo expectedABAItNo) const;

    [[nodiscard]] bool validateFinalState(
        SpanningTupleBufferIdx bufferIdx, const PlainSpanningTupleBufferEntry& nextEntry, SpanningTupleBufferIdx lastIdxOfBuffer) const;

private:
    TupleBuffer leadingBufferRef;
    TupleBuffer trailingBufferRef;
    PlainState plainState;
    uint32_t firstDelimiterOffset = 0;
    uint32_t lastDelimiterOffset = 0;
};

}
