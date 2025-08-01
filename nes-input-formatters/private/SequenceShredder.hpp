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
#include <string_view>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>

#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

/// A staged buffer represents a raw buffer together with the locations of the first and last tuple delimiter.
/// Thus, the SequenceShredder can determine the spanning tuple(s) starting/ending in or containing the StagedBuffer.
struct StagedBuffer
{
private:
    friend class SequenceShredder;

public:
    StagedBuffer() = default;
    StagedBuffer(RawTupleBuffer rawTupleBuffer, const uint32_t offsetOfFirstTupleDelimiter, const uint32_t offsetOfLastTupleDelimiter)
        : rawBuffer(std::move(rawTupleBuffer))
        , sizeOfBufferInBytes(this->rawBuffer.getNumberOfBytes())
        , offsetOfFirstTupleDelimiter(offsetOfFirstTupleDelimiter)
        , offsetOfLastTupleDelimiter(offsetOfLastTupleDelimiter) { };
    StagedBuffer(
        RawTupleBuffer rawTupleBuffer,
        const size_t sizeOfBufferInBytes,
        const uint32_t offsetOfFirstTupleDelimiter,
        const uint32_t offsetOfLastTupleDelimiter)
        : rawBuffer(std::move(rawTupleBuffer))
        , sizeOfBufferInBytes(sizeOfBufferInBytes)
        , offsetOfFirstTupleDelimiter(offsetOfFirstTupleDelimiter)
        , offsetOfLastTupleDelimiter(offsetOfLastTupleDelimiter) { };

    [[nodiscard]] std::string_view getBufferView() const { return rawBuffer.getBufferView(); }
    /// Returns the _first_ bytes of a staged buffer that were not processed by another thread yet.
    /// Typically, these are the bytes of a spanning tuple that _ends_ in the staged buffer.
    [[nodiscard]] std::string_view getLeadingBytes() const { return rawBuffer.getBufferView().substr(0, offsetOfFirstTupleDelimiter); }

    /// Returns the _last_ bytes of a staged buffer that were not processed by another thread yet.
    /// Typically, these are the bytes of spanning tuple that _starts_ in the staged buffer.
    [[nodiscard]] std::string_view getTrailingBytes(const size_t sizeOfTupleDelimiter) const
    {
        const auto sizeOfTrailingSpanningTuple = sizeOfBufferInBytes - (offsetOfLastTupleDelimiter + sizeOfTupleDelimiter);
        const auto startOfTrailingSpanningTuple = offsetOfLastTupleDelimiter + sizeOfTupleDelimiter;
        return rawBuffer.getBufferView().substr(startOfTrailingSpanningTuple, sizeOfTrailingSpanningTuple);
    }

    [[nodiscard]] uint32_t getOffsetOfFirstTupleDelimiter() const { return offsetOfFirstTupleDelimiter; }
    [[nodiscard]] uint32_t getOffsetOfLastTupleDelimiter() const { return offsetOfLastTupleDelimiter; }
    [[nodiscard]] size_t getSizeOfBufferInBytes() const { return this->sizeOfBufferInBytes; }
    [[nodiscard]] const RawTupleBuffer& getRawTupleBuffer() const { return rawBuffer; }

    [[nodiscard]] bool isValidRawBuffer() const { return rawBuffer.getRawBuffer().getBuffer() != nullptr; }

    void setSpanningTuple(const std::string_view spanningTuple) { rawBuffer.setSpanningTuple(spanningTuple); }


protected:
    RawTupleBuffer rawBuffer;
    size_t sizeOfBufferInBytes{};
    uint32_t offsetOfFirstTupleDelimiter{};
    uint32_t offsetOfLastTupleDelimiter{};
};

struct SequenceShredderResult
{
    bool isInRange;
    size_t indexOfInputBuffer;
    std::vector<StagedBuffer> spanningBuffers;
};


/// Forward declare to make friend
class SSMetaData;
class AtomicBitmapState
{
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

    [[nodiscard]] BitmapState getState() const { return BitmapState{this->state.load()}; }

    friend SSMetaData;
    bool tryClaimSpanningTuple(const uint32_t abaItNumber)
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

    void setStateOfFirstEntry() { this->state = initialDummyEntry; }
    void setHasTupleDelimiterState(const size_t abaItNumber) { this->state = (hasTupleDelimiterBit | abaItNumber); }
    void setNoTupleDelimiterState(const size_t abaItNumber) { this->state = abaItNumber; }

    [[nodiscard]] uint32_t getABAItNo() const
    {
        /// The first 32 bits are the ABA number
        return static_cast<uint32_t>(state.load());
    }

    void setUsedLeadingBuffer() { this->state |= usedLeadingBufferBit; }
    void setUsedTrailingBuffer() { this->state |= usedTrailingBufferBit; }
    void setUsedLeadingAndTrailingBuffer() { this->state |= usedLeadingAndTrailingBufferBits; }


    friend std::ostream& operator<<(std::ostream& os, const AtomicBitmapState& atomicBitmapState)
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

    std::atomic<uint64_t> state;
};

struct Interval
{
    uint32_t lo;
    uint32_t hi;

    bool operator<(const Interval& other) const { return this->lo < other.lo; }

    friend std::ostream& operator<<(std::ostream& os, const Interval& interval)
    {
        return os << '[' << interval.lo << ',' << interval.hi << ']';
    }
};

class SSMetaData
{
public:
    SSMetaData() { };

    template <bool HasTupleDelimiter>
    bool isInRange(const size_t abaItNumber, const StagedBuffer& indexedBuffer)
    {
        // Todo: claiming the entry with a single CAS is difficult, since it is difficult to set all bits correctly
        // -> did the prior entry have a delimiter, what was the other meta data (if we have more meta data in the future)
        const auto currentEntry = this->getState();
        const auto currentABAItNo = currentEntry.getABAItNo();
        const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
        const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
        const auto priorEntryIsUsed = currentABAItNo == (abaItNumber - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
        if (priorEntryIsUsed)
        {
            if (HasTupleDelimiter)
            {
                this->leadingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
                this->trailingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
                this->firstDelimiterOffset = indexedBuffer.getOffsetOfFirstTupleDelimiter();
                this->lastDelimiterOffset = indexedBuffer.getOffsetOfLastTupleDelimiter();
                this->atomicBitmapState.setHasTupleDelimiterState(abaItNumber);
            }
            else
            {
                this->leadingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
                this->trailingBufferRef = indexedBuffer.getRawTupleBuffer().getRawBuffer();
                this->firstDelimiterOffset = std::numeric_limits<uint32_t>::max();
                this->lastDelimiterOffset = std::numeric_limits<uint32_t>::max();
                this->atomicBitmapState.setNoTupleDelimiterState(abaItNumber);
            }
        }
        return priorEntryIsUsed;
    }

    // Todo: make private or similar


    std::optional<StagedBuffer> tryClaimSpanningTuple(const uint32_t abaItNumber)
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
    void setStateOfFirstIndex()
    {
        /// The first entry is a dummy that makes sure that we can resolve the first tuple in the first buffer
        this->atomicBitmapState.setStateOfFirstEntry();
        this->firstDelimiterOffset = std::numeric_limits<uint32_t>::max();
        this->lastDelimiterOffset = 0;
    }

    void claimNoDelimiterBuffer(std::vector<StagedBuffer>& spanningTupleVector, const size_t spanningTupleIdx)
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

    void claimLeadingBuffer(std::vector<StagedBuffer>& spanningTupleVector, const size_t spanningTupleIdx)
    {
        INVARIANT(this->leadingBufferRef.getBuffer() != nullptr, "Tried to claim a leading buffer with a nullptr");
        spanningTupleVector[spanningTupleIdx]
            = StagedBuffer(RawTupleBuffer{std::move(this->leadingBufferRef)}, firstDelimiterOffset, lastDelimiterOffset);
        this->atomicBitmapState.setUsedLeadingBuffer();
    }

    [[nodiscard]] AtomicBitmapState::BitmapState getState() const { return this->atomicBitmapState.getState(); }
    [[nodiscard, maybe_unused]] uint32_t getFirstDelimiterOffset() const { return this->firstDelimiterOffset; }
    [[nodiscard, maybe_unused]] uint32_t getLastDelimiterOffset() const { return this->lastDelimiterOffset; }

    // Todo: validation functions
    bool isLeadingBufferRefNull() const { return this->leadingBufferRef.getBuffer() == nullptr; }
    bool isTrailingBufferRefNull() const { return this->trailingBufferRef.getBuffer() == nullptr; }

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

class SequenceShredder
{
    static constexpr size_t SIZE_OF_RING_BUFFER = 1024;

public:
    using SequenceNumberType = SequenceNumber::Underlying;

    explicit SequenceShredder() : ringBuffer(std::vector<SSMetaData>(SIZE_OF_RING_BUFFER)) { ringBuffer[0].setStateOfFirstIndex(); };
    ~SequenceShredder();

    /// Thread-safely checks if the buffer represented by the sequence number completes spanning tuples.
    /// Returns a sequence of tuple buffers that represent either 0, 1 or 2 SpanningTuples.
    /// For details on the inner workings of this function, read the description of the class above.
    template <bool HasTupleDelimiter>
    SequenceShredderResult processSequenceNumber(StagedBuffer indexedRawBuffer, SequenceNumberType sequenceNumber);

    friend std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder);

private:
    // Todo: make 'RingBuffer' its own class (in its own file) <-- SequenceShredder should be thin wrapper around RingBuffer (if possible, with a single function)
    std::vector<SSMetaData> ringBuffer;
};

}

FMT_OSTREAM(NES::InputFormatters::SequenceShredder);
