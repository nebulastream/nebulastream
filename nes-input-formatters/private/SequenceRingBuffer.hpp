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

class SSMetaData
{
public:
    SSMetaData() { };

    void setStateOfFirstIndex();

    // Todo: documentation on functions
    [[nodiscard]] bool isPriorEntryUsed(size_t abaItNumber) const;

    void copyFromStagedBuffer(const StagedBuffer& indexedBuffer);

    bool tryClaimWithDelimiter(size_t abaItNumber, const StagedBuffer& indexedBuffer);
    bool tryClaimWithoutDelimiter(size_t abaItNumber, const StagedBuffer& indexedBuffer);

    std::optional<StagedBuffer> tryClaimSpanningTuple(uint32_t abaItNumber);
    void claimNoDelimiterBuffer(std::vector<StagedBuffer>& spanningTupleVector, size_t spanningTupleIdx);
    void claimLeadingBuffer(std::vector<StagedBuffer>& spanningTupleVector, size_t spanningTupleIdx);

    [[nodiscard]] AtomicBitmapState::BitmapState getState() const { return this->atomicBitmapState.getState(); }
    [[nodiscard, maybe_unused]] uint32_t getFirstDelimiterOffset() const { return this->firstDelimiterOffset; }
    [[nodiscard, maybe_unused]] uint32_t getLastDelimiterOffset() const { return this->lastDelimiterOffset; }

    /// Functions used for validation
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

template <size_t N>
class SequenceRingBuffer
{
    // Todo: make private functions free functions
    template <bool IsLeading>
    bool hasNoTD(const size_t snRBIdx, const size_t abaItNumber, const size_t distance)
    {
        const auto adjustedSN = (IsLeading) ? snRBIdx - distance : snRBIdx + distance;
        const auto tdsIdx = adjustedSN % N;
        const auto bitmapState = ringBuffer[tdsIdx].getState();
        if (IsLeading)
        {
            const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < distance);
            return not(bitmapState.hasTupleDelimiter()) and bitmapState.getABAItNo() == adjustedAbaItNumber;
        }
        const auto adjustedAbaItNumber = abaItNumber + static_cast<size_t>((snRBIdx + distance) >= N);
        return not(bitmapState.hasTupleDelimiter()) and bitmapState.getABAItNo() == adjustedAbaItNumber;
    }

    template <bool IsLeading>
    bool hasTD(const size_t snRBIdx, const size_t abaItNumber, const size_t distance)
    {
        const auto adjustedSN = (IsLeading) ? snRBIdx - distance : snRBIdx + distance;
        const auto tdsIdx = adjustedSN % N;
        const auto bitmapState = ringBuffer[tdsIdx].getState();
        if (IsLeading)
        {
            const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < distance);
            return bitmapState.hasTupleDelimiter() and bitmapState.getABAItNo() == adjustedAbaItNumber;
        }
        const auto adjustedAbaItNumber = abaItNumber + static_cast<size_t>((snRBIdx + distance) >= N);
        return bitmapState.hasTupleDelimiter() and bitmapState.getABAItNo() == adjustedAbaItNumber;
    }

    // Todo: could also just return TupleBuffer (and get SN from buffer), but initial dummy buffer is problem
    // -> could honestly think about creating struct that mimics buffer (and control block) and reinterpet_casting it to TupleBuffer
    std::optional<uint32_t>
    nonClaimingLeadingDelimiterSearch(const size_t snRBIdx, const size_t abaItNumber, const SequenceNumberType currentSequenceNumber)
    {
        size_t leadingDistance = 1;
        while (hasNoTD<true>(snRBIdx, abaItNumber, leadingDistance))
        {
            ++leadingDistance;
        }
        return hasTD<true>(snRBIdx, abaItNumber, leadingDistance) ? std::optional{currentSequenceNumber - leadingDistance} : std::nullopt;
    }
    std::optional<uint32_t>
    nonClaimingTrailingDelimiterSearch(const size_t snRBIdx, const size_t abaItNumber, const SequenceNumberType currentSequenceNumber)
    {
        size_t trailingDistance = 1;
        while (hasNoTD<false>(snRBIdx, abaItNumber, trailingDistance))
        {
            ++trailingDistance;
        }
        return hasTD<false>(snRBIdx, abaItNumber, trailingDistance) ? std::optional{currentSequenceNumber + trailingDistance}
                                                                    : std::nullopt;
    };

    std::pair<std::optional<StagedBuffer>, SequenceNumberType>
    leadingDelimiterSearch(const size_t snRBIdx, const size_t abaItNumber, const SequenceNumberType currentSequenceNumber)
    {
        size_t leadingDistance = 1;
        while (hasNoTD<true>(snRBIdx, abaItNumber, leadingDistance))
        {
            ++leadingDistance;
        }
        if (hasTD<true>(snRBIdx, abaItNumber, leadingDistance))
        {
            const auto spanningTupleStartSN = currentSequenceNumber - leadingDistance;
            const auto spanningTupleStartIdx = spanningTupleStartSN % ringBuffer.size();
            // Todo: adjust aba number while iterating
            const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < leadingDistance);
            return std::make_pair(ringBuffer[spanningTupleStartIdx].tryClaimSpanningTuple(adjustedAbaItNumber), spanningTupleStartSN);
        }
        return std::make_pair(std::nullopt, 0);
    };

    std::pair<std::optional<StagedBuffer>, SequenceNumberType> trailingDelimiterSearch(
        const size_t snRBIdx,
        const size_t abaItNumber,
        const SequenceNumberType currentSequenceNumber,
        const size_t spanningTupleStartIdx,
        const size_t abaItNumberSpanningTupleStart)
    {
        size_t trailingDistance = 1;

        while (hasNoTD<false>(snRBIdx, abaItNumber, trailingDistance))
        {
            ++trailingDistance;
        }
        if (hasTD<false>(snRBIdx, abaItNumber, trailingDistance))
        {
            const auto leadingSequenceNumber = currentSequenceNumber + trailingDistance;

            return std::make_pair(
                ringBuffer[spanningTupleStartIdx].tryClaimSpanningTuple(abaItNumberSpanningTupleStart), leadingSequenceNumber);
        }
        return std::make_pair(std::nullopt, 0);
    };

public:
    enum class NonClaimingRangeSearchState : uint8_t
    {
        NONE,
        LEADING_AND_TRAILING_ST
    };
    enum class RangeSearchState : uint8_t
    {
        NONE,
        LEADING_ST,
        TRAILING_ST,
        LEADING_AND_TRAILING_ST
    };

    struct NonClaimingRangeSearchResult
    {
        NonClaimingRangeSearchState state = RangeSearchState::NONE;
        SequenceNumberType leadingStartSN{};
        SequenceNumberType trailingStartSN{};
    };

    struct RangeSearchResult
    {
        RangeSearchState state = RangeSearchState::NONE;
        std::optional<StagedBuffer> leadingStartBuffer;
        std::optional<StagedBuffer> trailingStartBuffer;
        SequenceNumberType leadingStartSN{};
        SequenceNumberType trailingStartSN{};
    };

    SequenceRingBuffer() { ringBuffer[0].setStateOfFirstIndex(); }

    NonClaimingRangeSearchResult
    searchWithoutClaimingBuffers(const size_t snRBIdx, const size_t abaItNumber, const SequenceNumberType sequenceNumber)
    {
        if (const auto firstBufferSN = nonClaimingLeadingDelimiterSearch(snRBIdx, abaItNumber, sequenceNumber))
        {
            if (const auto secondBufferSN = nonClaimingTrailingDelimiterSearch(snRBIdx, abaItNumber, sequenceNumber))
            {
                return NonClaimingRangeSearchResult{
                    .state = NonClaimingRangeSearchState::LEADING_AND_TRAILING_ST,
                    .leadingStartSN = firstBufferSN.value(),
                    .trailingStartSN = secondBufferSN.value()};
            }
        }
        return NonClaimingRangeSearchResult{.state = NonClaimingRangeSearchState::NONE};
    }

    RangeSearchResult searchAndClaimBuffers(const size_t snRBIdx, const size_t abaItNumber, const SequenceNumberType sequenceNumber)
    {
        const auto [firstBuffer, firstBufferSN] = leadingDelimiterSearch(snRBIdx, abaItNumber, sequenceNumber);
        const auto [secondBuffer, secondBufferSN] = trailingDelimiterSearch(snRBIdx, abaItNumber, sequenceNumber, snRBIdx, abaItNumber);
        const auto rangeState = static_cast<RangeSearchState>(
            (static_cast<uint8_t>(secondBuffer.has_value()) << 1UL) + static_cast<uint8_t>(firstBuffer.has_value()));
        return RangeSearchResult{
            .state = rangeState,
            .leadingStartBuffer = std::move(firstBuffer),
            .trailingStartBuffer = std::move(secondBuffer),
            .leadingStartSN = firstBufferSN,
            .trailingStartSN = secondBufferSN};
    }

    void setCompletedFlags(
        const size_t firstDelimiter, const size_t lastDelimiter, std::vector<StagedBuffer>& spanningTupleBuffers, size_t spanningTupleIdx)
    {
        size_t nextDelimiter = (firstDelimiter + 1) % ringBuffer.size();
        while (nextDelimiter != lastDelimiter)
        {
            ringBuffer[nextDelimiter].claimNoDelimiterBuffer(spanningTupleBuffers, spanningTupleIdx);
            ++spanningTupleIdx;
            nextDelimiter = (nextDelimiter + 1) % ringBuffer.size();
        }
        ringBuffer[lastDelimiter].claimLeadingBuffer(spanningTupleBuffers, spanningTupleIdx);
    }

    bool rangeCheckWithDelimiter(const size_t snRBIdx, const uint32_t abaItNumber, const StagedBuffer& indexedRawBuffer)
    {
        return ringBuffer[snRBIdx].tryClaimWithDelimiter(abaItNumber, indexedRawBuffer);
    }

    bool rangeCheckWithoutDelimiter(const size_t snRBIdx, const uint32_t abaItNumber, const StagedBuffer& indexedRawBuffer)
    {
        return ringBuffer[snRBIdx].tryClaimWithoutDelimiter(abaItNumber, indexedRawBuffer);
    }

    std::optional<StagedBuffer> tryClaimSpanningTuple(const size_t firstDelimiterIdx, const uint32_t abaItNumberOfFirstDelimiter)
    {
        return ringBuffer[firstDelimiterIdx].tryClaimSpanningTuple(abaItNumberOfFirstDelimiter);
    }

    void validate() const
    {
        for (const auto& [idx, metaData] : ringBuffer | NES::views::enumerate)
        {
            const auto state = metaData.getState();
            INVARIANT(state.hasUsedLeadingBuffer() == true, "Buffer at index {} does still claim to own leading buffer", idx);
            INVARIANT(metaData.isLeadingBufferRefNull() == true, "Buffer at index {} still owns a leading buffer reference", idx);

            const auto& nextEntryRef = ringBuffer.at((idx + 1) % ringBuffer.size());
            const auto nextEntryRefState = nextEntryRef.getState();
            const auto adjustment = static_cast<size_t>(static_cast<size_t>(idx + 1) == ringBuffer.size());
            if (state.getABAItNo() == nextEntryRefState.getABAItNo() - adjustment)
            {
                INVARIANT(state.hasUsedTrailingBuffer() == true, "Buffer at index {} does still claim to own leading buffer", idx);
                INVARIANT(metaData.isTrailingBufferRefNull() == true, "Buffer at index {} still owns a trailing buffer reference", idx);
            }
        }
    }
private:
    std::array<SSMetaData, N> ringBuffer{};
};

}
