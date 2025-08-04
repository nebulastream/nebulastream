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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>
#include "Util/Ranges.hpp"

#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

class SSMetaData;

class AtomicBitmapState
{
    // Todo: try to not expose the AtomicBitmapState out of SSMetaData and then make functions public (instead of friend)
    friend class SSMetaData;

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
        friend class AtomicBitmapState;
        [[nodiscard]] uint64_t getBitmapState() const { return state; }
        uint64_t state;
    };
    AtomicBitmapState() : state(defaultState) { };
    ~AtomicBitmapState() = default;

    AtomicBitmapState(const AtomicBitmapState&) = delete;
    AtomicBitmapState(const AtomicBitmapState&&) = delete;
    void operator=(const AtomicBitmapState& other) = delete;
    void operator=(const AtomicBitmapState&& other) = delete;

    bool tryClaimSpanningTuple(uint32_t abaItNumber);

    [[nodiscard]] uint32_t getABAItNo() const { return static_cast<uint32_t>(state.load()); }
    [[nodiscard]] BitmapState getState() const { return BitmapState{this->state.load()}; }

    void setStateOfFirstEntry() { this->state = initialDummyEntry; }
    void setHasTupleDelimiterState(const size_t abaItNumber) { this->state = (hasTupleDelimiterBit | abaItNumber); }
    void setNoTupleDelimiterState(const size_t abaItNumber) { this->state = abaItNumber; }
    void setUsedLeadingBuffer() { this->state |= usedLeadingBufferBit; }
    void setUsedTrailingBuffer() { this->state |= usedTrailingBufferBit; }
    void setUsedLeadingAndTrailingBuffer() { this->state |= usedLeadingAndTrailingBufferBits; }

    friend std::ostream& operator<<(std::ostream& os, const AtomicBitmapState& atomicBitmapState);

    std::atomic<uint64_t> state;
};

// Todo: rename <-- try to get rid of SequenceShredder -> SS acronym in general (stringstream is worse enough already)
class SSMetaData
{
public:
    struct EntryState
    {
        bool isCorrectABA = false;
        bool hasDelimiter = false;
    };
    SSMetaData() { };

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

    // [[nodiscard]] AtomicBitmapState::BitmapState getState() const { return this->atomicBitmapState.getState(); }
    [[nodiscard, maybe_unused]] uint32_t getFirstDelimiterOffset() const { return this->firstDelimiterOffset; }
    [[nodiscard, maybe_unused]] uint32_t getLastDelimiterOffset() const { return this->lastDelimiterOffset; }

    [[nodiscard]] bool validateFinalState(const size_t idx, const SSMetaData& nextEntry, const size_t lastIdxOfRB) const
    {
        bool isValidFinalState = true;
        const auto state = this->atomicBitmapState.getState();
        if (not state.hasUsedLeadingBuffer())
        {
            isValidFinalState = false;
            NES_ERROR("Buffer at index {} does still claim to own leading buffer", idx);
        }
        if (this->leadingBufferRef.getBuffer() != nullptr)
        {
            isValidFinalState = false;
            NES_ERROR("Buffer at index {} still owns a leading buffer reference", idx);
        }

        /// Add '1' to the ABA iteration number, if the current entry is the last index of the ring buffer and the next entry wraps around
        if (state.getABAItNo() + static_cast<size_t>(idx == lastIdxOfRB) == nextEntry.atomicBitmapState.getABAItNo())
        {
            if (not state.hasUsedTrailingBuffer())
            {
                isValidFinalState = false;
                NES_ERROR("Buffer at index {} does still claim to own leading buffer", idx);
            }
            if (this->trailingBufferRef.getBuffer() != nullptr)
            {
                isValidFinalState = false;
                NES_ERROR("Buffer at index {} still owns a trailing buffer reference", idx);
            }
        }
        return isValidFinalState;
    }

private:
    // 24 Bytes (TupleBuffer)
    Memory::TupleBuffer leadingBufferRef;
    // 48 Bytes (TupleBuffer)
    Memory::TupleBuffer trailingBufferRef;
    // 56 Bytes (Atomic state)
    AtomicBitmapState atomicBitmapState;
    // 60 Bytes (meta data)
    uint32_t firstDelimiterOffset{};
    // 64 Bytes (meta data)
    uint32_t lastDelimiterOffset{};
};

// Todo: rename, is not a ring buffer anymore
class SequenceRingBuffer
{
    /// Result of trying to claim a buffer (with a specific SN) as the start of a spanning tuple.
    /// Multiple threads may try to claim a specific buffer. Only the thread that wins the race gets 'firstBuffer' with a value.
    struct ClaimedSpanningTuple
    {
        std::optional<StagedBuffer> firstBuffer;
        SequenceNumberType snOfLastBuffer{};
    };

public:
    // Todo: rename into 'ClaimSingleST' and 'ClaimTwoSTs' or similar
    struct NonClaimingSearchResult
    {
        enum class State : uint8_t
        {
            NONE,
            LEADING_AND_TRAILING_ST
        };

        State state = State::NONE;
        std::optional<StagedBuffer> startBuffer = std::nullopt;
        SequenceNumberType startBufferSN{};
        SequenceNumberType endBufferSN{};
    };

    struct ClaimingSearchResult
    {
        enum class State : uint8_t
        {
            NONE,
            LEADING_ST,
            TRAILING_ST,
            LEADING_AND_TRAILING_ST
        };

        State state = State::NONE;
        std::optional<StagedBuffer> leadingStartBuffer;
        std::optional<StagedBuffer> trailingStartBuffer;
        SequenceNumberType leadingStartSN{};
        SequenceNumberType trailingStartSN{};
    };

    explicit SequenceRingBuffer(size_t initialSize);

    /// Searches for spanning tuples both in leading and trailing direction of the 'pivotSN'
    /// Does not try to claim the spanning tuples, but simply returns their SNs.
    [[nodiscard]] NonClaimingSearchResult
    searchWithoutClaimingBuffers(size_t pivotSN, size_t abaItNumber, SequenceNumberType sequenceNumber);

    /// Searches for spanning tuples both in leading and trailing direction of the 'pivotSN'
    /// Tries to claim the start of a spanning tuple (and thereby the ST) if it finds a valid ST.
    [[nodiscard]] ClaimingSearchResult searchAndClaimBuffers(SequenceNumberType sequenceNumber);

    /// Claims all trailing buffers of a STuple (all buffers except the first, which the thread must have claimed already to claim the rest)
    void claimSTupleBuffers(size_t sTupleStartSN, std::span<StagedBuffer> spanningTupleBuffers);

    /// Checks if the current entry at the ring buffer index has the prior aba iteration number and if both its leading and trailing buffer
    /// were used already. Returns false, if it is not the case, indicating that the caller needs to try again later
    [[nodiscard]] bool trySetNewBufferWithDelimiter(size_t sequenceNumber, const StagedBuffer& indexedRawBuffer);
    [[nodiscard]] bool trySetNewBufferWithOutDelimiter(size_t sequenceNumber, const StagedBuffer& indexedRawBuffer);

    [[nodiscard]] bool validate() const;

    friend std::ostream& operator<<(std::ostream& os, const SequenceRingBuffer& sequenceRingBuffer);

private:
    std::vector<SSMetaData> ringBuffer;

    [[nodiscard]] std::pair<SSMetaData::EntryState, size_t> searchLeading(size_t rbIdxOfSN, size_t abaItNumber) const;

    [[nodiscard]] std::optional<uint32_t>
    nonClaimingLeadingDelimiterSearch(size_t rbIdxOfSN, size_t abaItNumber, SequenceNumberType sequenceNumber);

    /// Assumes spanningTupleEndSN as the end of a potential spanning tuple.
    /// Searches in leading direction (smaller SNs) for a buffer with a delimiter that can start the spanning tuple.
    /// Aborts as soon as it finds a non-connecting ABA iteration number (different and not first/last element of ring buffer).
    /// On finding a valid starting buffer, threads compete to claim that buffer (and thereby all buffers of the ST).
    /// Only one thread can succeed in claiming the first buffer (ClaimedSpanningTuple::firstBuffer != nullopt).
    [[nodiscard]] ClaimedSpanningTuple
    claimingLeadingDelimiterSearch(SequenceNumberType spanningTupleEndSN);

    [[nodiscard]] std::pair<SSMetaData::EntryState, size_t> searchTrailing(size_t rbIdxOfSN, size_t abaItNumber);

    [[nodiscard]] std::optional<uint32_t>
    nonClaimingTrailingDelimiterSearch(size_t rbIdxOfSN, size_t abaItNumber, SequenceNumberType currentSequenceNumber);

    /// Assumes spanningTupleStartSN as the start of a potential spanning tuple.
    /// Searches in trailing direction (larger SNs) for a buffer with a delimiter that terminates the spanning tuple.
    /// Aborts as soon as it finds a non-connecting ABA iteration number (different and not first/last element of ring buffer).
    /// On finding a valid terminating buffer, threads compete to claim the first buffer of the ST(spanningTupleStartSN) and thereby the ST.
    /// Only one thread can succeed in claiming the first buffer (ClaimedSpanningTuple::firstBuffer != nullopt).
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumberType spanningTupleStartSN);
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumberType spanningTupleStartSN, SequenceNumberType searchStartSN);
};
}

FMT_OSTREAM(NES::InputFormatters::SequenceRingBuffer);