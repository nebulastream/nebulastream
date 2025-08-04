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
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <RawTupleBuffer.hpp>

#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

class STBufferEntry;

/// (Implementation detail of the STBufferEntry)
/// The AtomicState consists a 64-bit-wide atomic bitmap that allows the STBuffer to thread-safely determine whether a specific
/// STBufferEntry may overwrite a prior entry or whether it connects to a neighbour-entry and whether the connecting entries form an ST
/// Central to the AtomicState are the first 32 bits, which represent the 'ABA iteration number' (abaItNo)
/// The abaItNo protects against the ABA problem, but more importantly, it allows the STBuffer to determine which iteration an STBufferEntry
/// belongs to (see header documentation of STBuffer)
/// Furthermore, the bitmap denotes whether an entry (with a specific abaItNo) has a delimiter, whether it was claimed as the first buffer
/// in a spanning tuple, and whether the leading/trailing buffer of the STBufferEntry were moved out already
/// If both the 'usedLeadingBufferBit' and the 'usedTrailingBufferBit' are set, then the buffer has no more uses and it is safe to replace
/// it with the buffer that has a sequence number that corresponds to the same STBufferEntry and that has the next higher abaItNo
/// (Planned) Some of the (currently) 28 unused bits will be used to model uncertainty. If a delimiter is larger than a byte a thread may
///           be uncertain whether the first/last few bytes of its buffer form a valid tuple delimiter. Additionally, it may be impossible
///           for a thread to determine if its buffer starts in an escape sequence or not. We can model these additional pieces of
///           information as states using the remaning bits and (potentially) perform state transitions as (atomic) bitwise operations.
class AtomicState
{
    friend STBufferEntry;

    /// 33:   000000000000000000000000000000100000000000000000000000000000000
    static constexpr uint64_t hasTupleDelimiterBit = 4294967296ULL;
    /// 34:   000000000000000000000000000001000000000000000000000000000000000
    static constexpr uint64_t claimedSpanningTupleBit = 8589934592ULL;
    /// 35:   000000000000000000000000000010000000000000000000000000000000000
    static constexpr uint64_t usedLeadingBufferBit = 17179869184ULL;
    /// 36:   000000000000000000000000000100000000000000000000000000000000000
    static constexpr uint64_t usedTrailingBufferBit = 34359738368ULL;
    ///       000000000000000000000000000110000000000000000000000000000000000
    static constexpr uint64_t usedLeadingAndTrailingBufferBits = 51539607552ULL;
    /// Tag: 0, HasTupleDelimiter: True, ClaimedSpanningTuple: True, UsedLeading: True, UsedTrailing: True
    static constexpr uint64_t defaultState = 60129542144ULL;
    /// Tag: 1, HasTupleDelimiter: True, ClaimedSpanningTuple: False, UsedLeading: True, UsedTrailing: False
    static constexpr uint64_t initialDummyEntry = 21474836481ULL;

    /// [1-32] : Iteration Tag:        protects against ABA and tells threads whether buffer is from the same iteration during ST search
    /// [33]   : HasTupleDelimiter:    when set, threads stop spanning tuple (ST) search, since the buffer represents a possible start/end
    /// [34]   : ClaimedSpanningTuple: signals to threads that the corresponding buffer was already claimed to start an ST by another thread
    /// [35]   : UsedLeadingBuffer:    signals to threads that the leading buffer use was consumed already
    /// [36]   : UsedTrailingBuffer:   signals to threads that the trailing buffer use was consumed already
    class BitmapState
    {
    public:
        explicit BitmapState(const uint64_t state) : state(state) { };
        [[nodiscard]] uint32_t getABAItNo() const { return static_cast<uint32_t>(state); }
        [[nodiscard]] bool hasTupleDelimiter() const { return (state & hasTupleDelimiterBit) == hasTupleDelimiterBit; }
        [[nodiscard]] bool hasUsedLeadingBuffer() const { return (state & usedLeadingBufferBit) == usedLeadingBufferBit; }
        [[nodiscard]] bool hasUsedTrailingBuffer() const { return (state & usedTrailingBufferBit) == usedTrailingBufferBit; }
        [[nodiscard]] bool hasClaimedSpanningTuple() const { return (state & claimedSpanningTupleBit) == claimedSpanningTupleBit; }

        void updateLeading(const BitmapState other) { this->state |= (other.getBitmapState() & usedLeadingBufferBit); }

        void claimSpanningTuple() { this->state |= claimedSpanningTupleBit; }

    private:
        friend class AtomicState;
        [[nodiscard]] uint64_t getBitmapState() const { return state; }
        uint64_t state;
    };

    AtomicState() : state(defaultState) { };
    ~AtomicState() = default;

    bool tryClaimSpanningTuple(uint32_t abaItNumber);

    [[nodiscard]] uint32_t getABAItNo() const { return static_cast<uint32_t>(state.load()); }
    [[nodiscard]] BitmapState getState() const { return BitmapState{this->state.load()}; }

    void setStateOfFirstEntry() { this->state = initialDummyEntry; }
    void setHasTupleDelimiterState(const size_t abaItNumber) { this->state = (hasTupleDelimiterBit | abaItNumber); }
    void setNoTupleDelimiterState(const size_t abaItNumber) { this->state = abaItNumber; }
    void setUsedLeadingBuffer() { this->state |= usedLeadingBufferBit; }
    void setUsedTrailingBuffer() { this->state |= usedTrailingBufferBit; }
    void setUsedLeadingAndTrailingBuffer() { this->state |= usedLeadingAndTrailingBufferBits; }

    friend std::ostream& operator<<(std::ostream& os, const AtomicState& atomicBitmapState);

    std::atomic<uint64_t> state;
};

/// Cache-line-size-aligned entry of spanning tuple buffer (STBuffer) that contains:
/// - (48B): two buffer references, since buffers with delimeters can be used to complete both a leading and a trailing spanning tuple (ST)
///     - since only one thread can find an ST, threads can safely move the buffer reference out of the STBufferEntry without synchronization
/// - (8B) : the atomic state of the entry, that allows the STBuffer to determine valid spanning tuples and to synchronize between threads
/// - (4B) : the offset of first delimiter in buffer, which the InputFormatterTask uses to construct a spanning tuple
/// - (4B) : offset of last delimiter in buffer, which the InputFormatterTask uses to construct a spanning tuple
class alignas(64) STBufferEntry /// NOLINT(readability-magic-numbers)
{
public:
    struct EntryState
    {
        bool hasCorrectABA = false;
        bool hasDelimiter = false;
    };

    STBufferEntry() : atomicState(AtomicState{}) { };

    /// Sets the state of the first entry to a value that allows triggering the first leading spanning tuple of a stream
    void setStateOfFirstIndex();

    /// A thread can claim a spanning tuple by claiming the first buffer of the spanning tuple (ST).
    /// Multiple threads may concurrently call 'tryClaimSpanningTuple()', but only one thread can successfully claim the ST.
    /// Only the succeeding thread receives a valid 'StagedBuffer', all other threads receive a 'nullopt'.
    std::optional<StagedBuffer> tryClaimSpanningTuple(uint32_t abaItNumber);

    /// Checks if the current entry was used to construct both a leading and trailing spanning tuple (given it matches abaItNumber - 1)
    [[nodiscard]] bool isCurrentEntryUsedUp(size_t abaItNumber) const;

    /// Sets the TupleBuffer of the staged buffer for both uses (leading/spanning) and copies the offsets
    void setBuffersAndOffsets(const StagedBuffer& indexedBuffer);

    /// Sets the indexed buffer as the new entry, if the prior entry is used up and its expected ABA iteration number is (abaItNumber - 1)
    bool trySetWithDelimiter(size_t abaItNumber, const StagedBuffer& indexedBuffer);
    bool trySetWithoutDelimiter(size_t abaItNumber, const StagedBuffer& indexedBuffer);

    /// Claim a buffer without a delimiter (that connects two buffers with delimiters), taking both buffers and atomically setting both uses at once
    void claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, size_t spanningTupleIdx);

    /// Claim the leading use of a buffer with a delimiter, only taking the leading buffer atomically setting the leading use
    void claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, size_t spanningTupleIdx);

    /// Atomically loads the state of an entry, checks if its ABA iteration number matches the expected and if it has a tuple delimiter
    [[nodiscard]] EntryState getEntryState(size_t expectedABAItNo) const;

    [[nodiscard, maybe_unused]] uint32_t getFirstDelimiterOffset() const { return this->firstDelimiterOffset; }
    [[nodiscard, maybe_unused]] uint32_t getLastDelimiterOffset() const { return this->lastDelimiterOffset; }

    [[nodiscard]] bool validateFinalState(size_t bufferIdx, const STBufferEntry& nextEntry, size_t lastIdxOfBuffer) const;

private:
    // 24 Bytes (TupleBuffer)
    Memory::TupleBuffer leadingBufferRef;
    // 48 Bytes (TupleBuffer)
    Memory::TupleBuffer trailingBufferRef;
    // 56 Bytes (Atomic state)
    AtomicState atomicState;
    // 60 Bytes (meta data)
    uint32_t firstDelimiterOffset{};
    // 64 Bytes (meta data)
    uint32_t lastDelimiterOffset{};
};
}