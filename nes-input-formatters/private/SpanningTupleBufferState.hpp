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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <ostream>
#include <span>
#include <Runtime/TupleBuffer.hpp>
#include <RawTupleBuffer.hpp>

#include <Identifiers/NESStrongType.hpp>

namespace NES
{

using SpanningTupleBufferIdx = NESStrongType<uint32_t, struct SpanningTupleBufferIdx_, 0, 1>;
using ABAItNo = NESStrongType<uint32_t, struct ABAItNo_, 0, 1>;

class SpanningTupleBufferEntry;

/// (Implementation detail of the SpanningTupleBufferEntry)
/// The AtomicState consists a 64-bit-wide atomic bitmap that allows the SpanningTupleBuffer to thread-safely determine whether a specific
/// SpanningTupleBufferEntry may overwrite a prior entry or whether it connects to a neighbour-entry and whether the connecting entries form an SpanningTuple
/// Central to the AtomicState are the first 32 bits, which represent the 'ABA iteration number' (abaItNo)
/// The abaItNo protects against the ABA problem, but more importantly, it allows the SpanningTupleBuffer to determine which iteration an SpanningTupleBufferEntry
/// belongs to (see header documentation of SpanningTupleBuffer)
/// Furthermore, the bitmap denotes whether an entry (with a specific abaItNo) has a delimiter, whether it was claimed as the first buffer
/// in a spanning tuple, and whether the leading/trailing buffer of the SpanningTupleBufferEntry were moved out already
/// If both the 'usedLeadingBufferBit' and the 'usedTrailingBufferBit' are set, then the buffer has no more uses and it is safe to replace
/// it with the buffer that has a sequence number that corresponds to the same SpanningTupleBufferEntry and that has the next higher abaItNo
/// (Planned) Some of the (currently) 28 unused bits will be used to model uncertainty. If a delimiter is larger than a byte a thread may
///           be uncertain whether the first/last few bytes of its buffer form a valid tuple delimiter. Additionally, it may be impossible
///           for a thread to determine if its buffer starts in an escape sequence or not. We can model these additional pieces of
///           information as states using the remaning bits and (potentially) perform state transitions as (atomic) bitwise operations.
class AtomicState
{
    friend SpanningTupleBufferEntry;

    /// 33:   000000000000000000000000000000100000000000000000000000000000000
    static constexpr uint64_t hasTupleDelimiterBit = (1ULL << 32ULL); /// NOLINT(readability-magic-numbers)
    /// 34:   000000000000000000000000000001000000000000000000000000000000000
    static constexpr uint64_t claimedSpanningTupleBit = (1ULL << 33ULL); /// NOLINT(readability-magic-numbers)
    /// 35:   000000000000000000000000000010000000000000000000000000000000000
    static constexpr uint64_t usedLeadingBufferBit = (1ULL << 34ULL); /// NOLINT(readability-magic-numbers)
    /// 36:   000000000000000000000000000100000000000000000000000000000000000
    static constexpr uint64_t usedTrailingBufferBit = (1ULL << 35ULL); /// NOLINT(readability-magic-numbers)
    /// 36:   000000000000000000000000001000000000000000000000000000000000000
    static constexpr uint64_t hasValidLastDelimiterOffsetBit = (1ULL << 36ULL); /// NOLINT(readability-magic-numbers)
    ///       000000000000000000000000000110000000000000000000000000000000000
    static constexpr uint64_t usedLeadingAndTrailingBufferBits = (usedLeadingBufferBit | usedTrailingBufferBit);

    /// The SpanningTupleBuffer initializes all SpanningTupleBufferEntries, except for the very first entry, with the 'defaultState'
    /// Tag: 0, HasTupleDelimiter: True, ClaimedSpanningTuple: True, UsedLeading: True, UsedTrailing: True
    static constexpr uint64_t defaultState = (0ULL | claimedSpanningTupleBit | usedLeadingBufferBit | usedTrailingBufferBit);
    /// The SpanningTupleBuffer initializes the very first entry with a dummy buffer and a matching dummy state to trigger the first leading SpanningTuple
    /// Tag: 1, HasTupleDelimiter: True, ClaimedSpanningTuple: False, UsedLeading: True, UsedTrailing: False, HasValidLastDelimiterOffset: True
    static constexpr uint64_t firstEntryDummy = (1ULL | hasTupleDelimiterBit | usedLeadingBufferBit | hasValidLastDelimiterOffsetBit);

    /// [1-32] : Iteration Tag:        protects against ABA and tells threads whether buffer is from the same iteration during SpanningTuple search
    /// [33]   : HasTupleDelimiter:    when set, threads stop spanning tuple (SpanningTuple) search, since the buffer represents a possible start/end
    /// [34]   : ClaimedSpanningTuple: signals to threads that the corresponding buffer was already claimed to start an SpanningTuple by another thread
    /// [35]   : UsedLeadingBuffer:    signals to threads that the leading buffer use was consumed already
    /// [36]   : UsedTrailingBuffer:   signals to threads that the trailing buffer use was consumed already
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

        void updateLeading(const BitmapState other) { this->state |= (other.getBitmapState() & usedLeadingBufferBit); }

        void claimSpanningTuple() { this->state |= claimedSpanningTupleBit; }

    private:
        friend class AtomicState;

        [[nodiscard]] uint64_t getBitmapState() const { return state; }

        uint64_t state;
    };

    AtomicState() : state(defaultState) { };
    ~AtomicState() = default;

    /// CAS loop that attempts to set the 'ClaimedSpanningTuple', if and only if the current atomic state has the expected abaItNumber and
    /// the 'ClaimedSpanningTuple' bit is not set yet. Thus, guarantees that only one thread can claim a spanning tuple
    bool tryClaimSpanningTuple(ABAItNo abaItNumber);

    [[nodiscard]] ABAItNo getABAItNo() const { return static_cast<ABAItNo>(state.load()); }

    [[nodiscard]] BitmapState getState() const { return BitmapState{this->state.load()}; }

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

    friend std::ostream& operator<<(std::ostream& os, const AtomicState& atomicBitmapState);

    std::atomic<uint64_t> state;
};

/// Entry of spanning tuple buffer (SpanningTupleBuffer). Is cache-line-size-aligned (64B) to avoid false sharing and contains:
/// - (48B): two buffer references, buffers with delimiters can be used to complete both a leading and a trailing spanning tuple
///     - since only one thread can find an SpanningTuple, threads can safely move their buffer reference out of the SpanningTupleBufferEntry without synchronization
/// - (8B) : the atomic state of the entry, which allows the SpanningTupleBuffer to determine valid spanning tuples and to synchronize between threads
/// - (4B) : the offset of the first delimiter in the buffer, which the InputFormatterTask uses to construct a spanning tuple
/// - (4B) : the offset of the last delimiter in the buffer, which the InputFormatterTask uses to construct a spanning tuple
class alignas(std::hardware_destructive_interference_size) SpanningTupleBufferEntry /// NOLINT(readability-magic-numbers)
{
public:
    struct EntryState
    {
        bool hasCorrectABA = false;
        bool hasDelimiter = false;
        bool hasValidTrailingDelimiterOffset = false;
    };

    SpanningTupleBufferEntry() = default;

    /// Sets the state of the first entry to a value that allows triggering the first leading spanning tuple of a stream
    void setStateOfFirstIndex(TupleBuffer dummyBuffer);

    /// Overwrites an invalid offset to the trailing SpanningTuple and sets flips the AtomicState-bit that represents a valid trailing offset
    /// After calling this function, any thread can find an SpanningTuple starting with this entry
    void setOffsetOfTrailingSpanningTuple(FieldIndex offsetOfLastTuple);

    /// A thread can claim a spanning tuple by claiming the first buffer of the spanning tuple (SpanningTuple).
    /// Multiple threads may concurrently call 'tryClaimSpanningTuple()', but only one thread can successfully claim the SpanningTuple.
    /// Only the succeeding thread receives a valid 'StagedBuffer', all other threads receive a 'nullopt'.
    std::optional<StagedBuffer> tryClaimSpanningTuple(ABAItNo abaItNumber);

    /// Checks if the current entry was used to construct both a leading and trailing spanning tuple (given it matches abaItNumber - 1)
    [[nodiscard]] bool isCurrentEntryUsedUp(ABAItNo abaItNumber) const;

    /// Sets the TupleBuffer of the staged buffer for both uses (leading/spanning) and copies the offsets
    void setBuffersAndOffsets(const StagedBuffer& indexedBuffer);

    /// Sets the indexed buffer as the new entry, if the prior entry is used up and its expected ABA iteration number is (abaItNumber - 1)
    bool trySetWithDelimiter(ABAItNo abaItNumber, const StagedBuffer& indexedBuffer);
    bool trySetWithoutDelimiter(ABAItNo abaItNumber, const StagedBuffer& indexedBuffer);

    /// Claim a buffer without a delimiter (that connects two buffers with delimiters), taking both buffers and atomically setting both uses at once
    void claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, size_t spanningTupleIdx);

    /// Claim the leading use of a buffer with a delimiter, only taking the leading buffer atomically setting the leading use
    void claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, size_t spanningTupleIdx);

    /// Atomically loads the state of an entry, checks if its ABA iteration number matches the expected and if it has a tuple delimiter
    [[nodiscard]] EntryState getEntryState(ABAItNo expectedABAItNo) const;

    /// Iterates over all SpanningTupleBufferEntries, checking that they don't hold any buffer references if they should not and that their atomic
    /// bitmap state is correct. Logs errors and returns 'false' if at least one entry is in an invalid state
    [[nodiscard]] bool validateFinalState(
        SpanningTupleBufferIdx bufferIdx, const SpanningTupleBufferEntry& nextEntry, SpanningTupleBufferIdx lastIdxOfBuffer) const;

private:
    /// 24 Bytes (TupleBuffer)
    TupleBuffer leadingBufferRef;
    /// 48 Bytes (TupleBuffer)
    TupleBuffer trailingBufferRef;
    /// 56 Bytes (Atomic state)
    AtomicState atomicState;
    /// 60 Bytes (meta data)
    uint32_t firstDelimiterOffset = 0;
    /// 64 Bytes (meta data)
    uint32_t lastDelimiterOffset = 0;
};

static_assert(sizeof(SpanningTupleBufferEntry) % std::hardware_destructive_interference_size == 0);
static_assert(alignof(SpanningTupleBufferEntry) % std::hardware_destructive_interference_size == 0);
}
