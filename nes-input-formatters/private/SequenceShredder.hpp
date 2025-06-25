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
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>

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
    StagedBuffer(
        RawTupleBuffer rawTupleBuffer,
        const size_t sizeOfBufferInBytes,
        const uint32_t offsetOfFirstTupleDelimiter,
        const uint32_t offsetOfLastTupleDelimiter)
        : rawBuffer(std::move(rawTupleBuffer))
        , sizeOfBufferInBytes(sizeOfBufferInBytes)
        , offsetOfFirstTupleDelimiter(offsetOfFirstTupleDelimiter)
        , offsetOfLastTupleDelimiter(offsetOfLastTupleDelimiter) { };

    StagedBuffer(StagedBuffer&& other) noexcept = default;
    StagedBuffer& operator=(StagedBuffer&& other) noexcept = default;
    StagedBuffer(const StagedBuffer& other) = default;
    StagedBuffer& operator=(const StagedBuffer& other) = default;

    [[nodiscard]] std::string_view getBufferView() const { return rawBuffer.getBufferView(); }
    /// Returns the _first_ bytes of a staged buffer that were not processed by another thread yet.
    /// Typically, these are the bytes of a spanning tuple that _ends_ in the staged buffer.
    [[nodiscard]] std::string_view getLeadingBytes() const { return rawBuffer.getBufferView().substr(0, offsetOfFirstTupleDelimiter); }

    /// Returns the _last_ bytes of a staged buffer that were not processed by another thread yet.
    /// Typically, these are the bytes of spanning tuple that _starts_ in the staged buffer.
    [[nodiscard]] std::string_view getTrailingBytes(const size_t sizeOfTupleDelimiter) const
    {
        if (sizeOfBufferInBytes == sizeOfTupleDelimiter)
        {
            return "";
        }
        const auto sizeOfTrailingSpanningTuple = sizeOfBufferInBytes - (offsetOfLastTupleDelimiter + sizeOfTupleDelimiter);
        const auto startOfTrailingSpanningTuple = offsetOfLastTupleDelimiter + sizeOfTupleDelimiter;
        return rawBuffer.getBufferView().substr(startOfTrailingSpanningTuple, sizeOfTrailingSpanningTuple);
    }

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

/// The SequenceShredder's main job is to find spanning tuples (during concurrent processing of raw input buffers).
/// A spanning tuple is a tuple that spans over at least two raw input buffers.
/// Given an input buffer without a delimiter, the SequenceShredder may find one spanning tuple at most (the raw input buffer connects
/// two raw input buffers with tuple delimiters).
/// Given an input buffer with a tuple delimiter, the SequenceShredder may find up to two spanning tuples. A first, starting in a raw input
/// buffer with a lower sequence number, ending in the raw input buffer. A second, starting in the raw input buffer, ending in a raw
/// input buffer with a higher sequence number.
struct SpanningTuple
{
    SequenceNumber::Underlying spanStart = 0;
    SequenceNumber::Underlying spanEnd = 0;
    bool isStartValid = false;
    bool isEndValid = false;
};

struct CriticalSequenceNumberEntry;

class SequenceShredder
{
public:
    using BitmapType = uint64_t;
    using SequenceNumberType = SequenceNumber::Underlying;
    static constexpr size_t SIZE_OF_BITMAP_IN_BITS = sizeof(BitmapType) * 8; /// 8 bits in one byte
    static constexpr size_t INITIAL_NUM_BITMAPS = 8;
    using BitmapVectorType = std::vector<BitmapType>;

    struct SpanningTupleBuffers
    {
        size_t indexOfProcessedSequenceNumber;
        std::vector<StagedBuffer> stagedBuffers;
    };

private:
    static constexpr size_t MAX_VALUE = std::numeric_limits<SequenceNumberType>::max();
    static constexpr size_t BITMAP_SIZE_BIT_SHIFT = std::countr_zero(SIZE_OF_BITMAP_IN_BITS); /// log2 of size of bitmaps
    static constexpr size_t BITMAP_SIZE_MODULO = (SIZE_OF_BITMAP_IN_BITS - 1);
    static constexpr SequenceNumberType FIRST_BIT_MASK = 1;
    static constexpr SequenceNumberType LAST_BIT_MASK = MAX_VALUE - 1;
    static constexpr SequenceNumberType INVALID_SEQUENCE_NUMBER = SequenceNumber::INVALID;
    static constexpr size_t MAX_NUMBER_OF_BITMAPS = 2048;
    static constexpr size_t SHIFT_TO_SECOND_BIT = 1;
    static constexpr size_t SHIFT_TO_THIRD_BIT = 2;
    static constexpr size_t MIN_NUMBER_OF_RESIZE_REQUESTS_BEFORE_INCREMENTING = 5;

public:
    explicit SequenceShredder(size_t sizeOfTupleDelimiter);
    explicit SequenceShredder(size_t sizeOfTupleDelimiter, size_t initialNumBitmaps);
    ~SequenceShredder();

    /// Return
    [[nodiscard]] size_t getTail() const { return this->tail; }

    /// Check if the sequence number is in the allowed range of the ring buffer.
    /// Example: 4 bitmaps, tail is 4, then the allowed range for sequence numbers is 256-511: [256,319][320,383][384,447][448,511]
    [[nodiscard]] bool isInRange(SequenceNumberType sequenceNumber);

    /// Logs the sequence number range of the SequenceShredder. Additionally, iterates over all bitmaps and detects whether the
    /// SequenceShredder is in a valid state for sequence number (each bit represents a sequence number). Logs all invalid sequence numbers.
    bool validateState() noexcept;

    /// Thread-safely checks if the buffer represented by the sequence number completes spanning tuples.
    /// Returns a sequence of tuple buffers that represent either 0, 1 or 2 SpanningTuples.
    /// For details on the inner workings of this function, read the description of the class above.
    template <bool HasTupleDelimiter>
    SpanningTupleBuffers processSequenceNumber(StagedBuffer stagedBufferOfSequenceNumber, SequenceNumberType sequenceNumber);

    friend std::ostream& operator<<(std::ostream& os, SequenceShredder& sequenceShredder);

private:
    std::mutex readWriteMutex; /// protects all member variable below, except for 'stagedBuffers' and 'stagedBufferUses'
    SequenceNumberType tail; /// represents the total number of times that the tail-bitmap changed
    BitmapVectorType tupleDelimiterBitmaps;
    BitmapVectorType seenAndUsedBitmaps;
    size_t numberOfBitmaps;
    size_t numberOfBitmapsModulo;
    size_t resizeRequestCount;
    /// The SequenceShredder owns staged buffers that must still become part of spanning tuples.
    std::vector<StagedBuffer> stagedBuffers;
    /// Keeps track of how often a specific buffer was used in spanning tuples
    /// If it reaches 0, we move the buffer out of the stagedBuffers, taking ownership away again
    std::vector<int8_t> stagedBufferUses;
    bool isFirstTuple = true;

    struct BitmapSnapshot
    {
        size_t numberOfBitmapsModulo;
        BitmapType tupleDelimiterBitmapSnapshot;
        BitmapType seenAndUsedBitmapSnapshot;
    };
    struct BitmapVectorSnapshot
    {
        size_t tail;
        size_t numberOfBitmapsModulo;
        BitmapVectorType tupleDelimiterVectorSnapshot;
        BitmapVectorType seenAndUsedVectorSnapshot;
    };
    /// The unique_ptr avoids allocating space for all bitmaps (BitmapVectorSnapshot) in the common case, which is 'BitmapSnapshot'
    using Snapshot = std::variant<BitmapSnapshot, std::unique_ptr<BitmapVectorSnapshot>>;

    enum class WrappingMode : uint8_t
    {
        NO_WRAPPING = 0, /// 00: checking the bitmap of the sequence number suffices (common case)
        CHECK_WRAPPING_TO_LOWER = 1, /// 01: the sequence number could complete a spanning tuple (ST) ending in a lower bitmap
        CHECK_WRAPPING_TO_HIGHER = 2, /// 10: the sequence number could complete an ST starting in a higher bitmap
        CHECK_WRAPPING_TO_LOWER_AND_HIGHER = 3, /// 11: ... could complete an ST starting in a lower bitmap and ending in a higher bitmap
    };

    /// @Note Must be called why holding the readWriteMutex
    /// Called when a thread completed the tail bitmap. The tail bitmap is complete, if the seenAndUsedBitmap pointed to by the tail contains only '1's.
    /// Resets the tail bitmaps and increments the tail. If new tail bitmaps are complete, repeats the process, until the new tail bitmaps are not complete.
    /// Increases the size of the ring buffer if enough threads issue 'resizeRequests'.
    void incrementTail();

    /// Determines whether the buffer of the sequence number connects to the buffer of a smaller sequence number with a tuple delimiter,
    /// meaning that the buffer with the smaller sequence number starts a spanning tuple that the buffer of the sequence number is a part of.
    std::pair<SequenceNumberType, bool> tryGetSpanningTupleStart(
        SequenceNumberType sequenceNumberBitIndex,
        SequenceNumberType sequenceNumberBitmapOffset,
        const SequenceNumberType& tupleDelimiterBitmap,
        const SequenceNumberType& seenAndUsedBitmap);

    /// Determines whether the buffer of the sequence number connects to a buffer of a higher sequence number with a tuple delimiter,
    /// meaning that the buffer with the higher sequence number ends a spanning tuple that the buffer of the sequence number is a part of.
    std::pair<SequenceNumberType, bool> tryGetSpanningTupleEnd(
        SequenceNumberType sequenceNumberBitIndex,
        SequenceNumberType sequenceNumberBitmapOffset,
        const SequenceNumberType& tupleDelimiterBitmap,
        const SequenceNumberType& seenAndUsedBitmap);

    /// Called by a thread that needs to search for the start of a spanning tuple in a lower bitmap than the one that its sequence number maps to.
    /// Skips over buffers that were seen, but did not contain a tuple delimiter, represented by a '0' in the tuple delimiter bitmap and
    /// a '1' in the seen and used bitmap. If the first 'non-0-1-buffer' contains a tuple delimiter, the function returns the sequence number
    /// representing the start of a spanning tuple and a bool that indicates whether it is a valid start.
    std::pair<SequenceNumberType, bool> tryToFindLowerWrappingSpanningTuple(
        size_t sequenceNumberBitmapOffset, size_t currentBitmapIndex, const BitmapVectorSnapshot& bitmapSnapshot);

    /// Called by a thread that needs to search for the end of a spanning tuple in a higher bitmap than the one that its sequence number maps to.
    /// Skips over buffers that were seen, but did not contain a tuple delimiter, represented by a '0' in the tuple delimiter bitmap and a '1'
    /// in the seen and used bitmap. If the first 'non-0-1-buffer' contains a tuple delimiter, the function returns the sequence number
    /// representing the end of a spanning tuple and a bool that indicates whether it is a valid end.
    std::pair<SequenceNumberType, bool> tryToFindHigherWrappingSpanningTuple(
        size_t sequenceNumberBitmapOffset, size_t currentBitmapIndex, const BitmapVectorSnapshot& bitmapSnapshot);

    /// If a buffer does not contain a tuple delimiter, it is only possible to find one spanning tuple.
    /// Both, the start and the end of the spanning tuple must be valid.
    SpanningTupleBuffers checkSpanningTupleWithoutTupleDelimiter(
        const SpanningTuple& spanningTuple, SequenceNumberType sequenceNumber, SequenceNumberType numberOfBitmapsModuloSnapshot);

    /// If a buffer contains a tuple delimiter, it is possible to find two spanning tuples, one ending in the sequence number and one starting with it.
    SpanningTupleBuffers checkSpanningTupleWithTupleDelimiter(
        SpanningTuple spanningTuple,
        SequenceNumberType sequenceNumber,
        SequenceNumberType numberOfBitmapsModuloSnapshot,
        StagedBuffer stagedBufferOfSequenceNumber);
};

}

FMT_OSTREAM(NES::InputFormatters::SequenceShredder);
FMT_OSTREAM(NES::InputFormatters::CriticalSequenceNumberEntry);
