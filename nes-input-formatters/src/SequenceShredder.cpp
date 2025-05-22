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

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <ostream>
#include <ranges>
#include <sstream>
#include <utility>
#include <vector>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

#include <ErrorHandling.hpp>
#include <SequenceShredder.hpp>

namespace NES::InputFormatters
{
/// Enable for detailed bitmap prints
constexpr bool VERBOSE_DEBUG_BITMAP_PRINTING = false;

SequenceShredder::SequenceShredder(const size_t sizeOfTupleDelimiter) : SequenceShredder(sizeOfTupleDelimiter, INITIAL_NUM_BITMAPS)
{
}

SequenceShredder::SequenceShredder(const size_t sizeOfTupleDelimiter, const size_t initialNumBitmaps)
    : tail(0)
    , tupleDelimiterBitmaps(BitmapVectorType(initialNumBitmaps))
    , seenAndUsedBitmaps(BitmapVectorType(initialNumBitmaps))
    , numberOfBitmaps(initialNumBitmaps)
    , numberOfBitmapsModulo(initialNumBitmaps - 1)
    , resizeRequestCount(0)
    , stagedBuffers(std::vector<StagedBuffer>(numberOfBitmaps << BITMAP_SIZE_BIT_SHIFT))
    , stagedBufferUses(std::vector<int8_t>(numberOfBitmaps << BITMAP_SIZE_BIT_SHIFT))
{
    this->tupleDelimiterBitmaps.shrink_to_fit();
    this->seenAndUsedBitmaps.shrink_to_fit();

    this->tupleDelimiterBitmaps[0] |= static_cast<SequenceNumberType>(1);
    this->stagedBuffers[0]
        = {.buffer = NES::Memory::TupleBuffer{},
           .sizeOfBufferInBytes = sizeOfTupleDelimiter,
           .offsetOfFirstTupleDelimiter = 0,
           .offsetOfLastTupleDelimiter = 0};
    this->stagedBufferUses[0] = 1;
};

bool SequenceShredder::isInRange(const SequenceNumberType sequenceNumber)
{
    const auto targetBitmap = (sequenceNumber >> BITMAP_SIZE_BIT_SHIFT);
    const std::scoped_lock lock(this->readWriteMutex); /// protects: write(resizeRequestCount), read(tail,numberOfBitmaps)
    /// The target bitmap is in range, if it does not exceed the current head (tail + number of bitmaps - 1)
    if (targetBitmap < (this->tail + this->numberOfBitmaps))
    {
        return true;
    }
    ++this->resizeRequestCount;
    return false;
}

std::pair<SequenceShredder::SpanningTupleBuffers, SequenceShredder::SequenceNumberType> SequenceShredder::flushFinalPartialTuple()
{
    /// protect: write(resizeRequestCount), read(tail,numberOfBitmaps)
    {
        std::unique_lock lock(this->readWriteMutex);
        isLastTuple = true;
        for (size_t offsetToTail = 1; offsetToTail <= this->numberOfBitmaps; ++offsetToTail)
        {
            const auto bitmapIndex = (this->tail + (this->numberOfBitmaps - offsetToTail)) & this->numberOfBitmapsModulo;
            const auto seenAndUsedBitmap = this->seenAndUsedBitmaps[bitmapIndex];
            const auto tupleDelimiterBitmap = this->tupleDelimiterBitmaps[bitmapIndex];
            /// Reverse-search bitmaps, until a bitmap is not 0 and therefore represents a buffer that is in the stagedBuffers vector
            if ((seenAndUsedBitmap | tupleDelimiterBitmap) != 0)
            {
                /// If the last buffer contains a buffer that does not contain a delimiter, we need to check for a spanning tuple
                /// We construct a dummy staged buffer and set its sequence number to exactly one higher than the largest seen sequence number.
                /// The dummy staged buffer flushes out all prior buffers that still depended on a tuple delimiter that did not appear,
                /// because an EOF/EOS is not a tuple delimiter.
                const auto firstSequenceNumberOfTail = this->tail * SIZE_OF_BITMAP_IN_BITS;
                const auto sequenceNumberOffsetOfBitmap = ((this->numberOfBitmaps - offsetToTail) & this->numberOfBitmapsModulo)
                    << BITMAP_SIZE_BIT_SHIFT;
                const auto firstSequenceNumberOfBitmap = firstSequenceNumberOfTail + sequenceNumberOffsetOfBitmap;
                const auto numberOfNotSeenSequenceNumbersInBitmap = std::countl_zero(seenAndUsedBitmap | tupleDelimiterBitmap);
                const auto offsetToNextLargerSequenceNumber = SIZE_OF_BITMAP_IN_BITS - numberOfNotSeenSequenceNumbersInBitmap;
                const auto nextLargestSequenceNumber = firstSequenceNumberOfBitmap + offsetToNextLargerSequenceNumber;
                auto dummyStagedBuffer = StagedBuffer{
                    .buffer = NES::Memory::TupleBuffer{},
                    .sizeOfBufferInBytes = 0,
                    .offsetOfFirstTupleDelimiter = 0,
                    .offsetOfLastTupleDelimiter = 0};

                /// Determine whether the formatter produced a buffer using the largest sequence number already.
                /// If that is the case, if the buffer of the largest sequence number contains a tuple delimiter and it was used alread (uses != 2)
                const auto largestSequenceNumber = nextLargestSequenceNumber - 1;
                const auto bitOfLastSequenceNumber = FIRST_BIT_MASK << (offsetToNextLargerSequenceNumber - 1);
                const auto hasTupleDelimiter = static_cast<bool>(tupleDelimiterBitmap & bitOfLastSequenceNumber);
                const auto bufferIdxOfLargestSequenceNumber = largestSequenceNumber & (this->stagedBuffers.size() - 1);
                const auto numUsesOfLastSequenceNumber = this->stagedBufferUses[bufferIdxOfLargestSequenceNumber];
                const auto largestSequenceNumberProducedBufferAlready = hasTupleDelimiter and numUsesOfLastSequenceNumber != 2;
                /// We can safely use the next larger sequence number, if the there is at least one formatted buffer, with the current largest sequence number
                const auto sequenceNumberToUseForFlushedTuple
                    = (largestSequenceNumberProducedBufferAlready) ? nextLargestSequenceNumber : largestSequenceNumber;

                lock.unlock();
                return std::make_pair(
                    processSequenceNumber<true>(std::move(dummyStagedBuffer), nextLargestSequenceNumber),
                    sequenceNumberToUseForFlushedTuple);
            }
        }
    }
    return std::make_pair(SpanningTupleBuffers{}, 0);
}

template <bool HasTupleDelimiter>
SequenceShredder::SpanningTupleBuffers
SequenceShredder::processSequenceNumber(StagedBuffer stagedBufferOfSequenceNumber, const SequenceNumberType sequenceNumber)
{
    /// Calculate how many bitmaps preceed the bitmap that the sequence number maps to (sequenceNumber / SIZE_OF_BITMAP_IN_BITS).
    /// running example (sequenceNumber = 67, size of bitmaps: 64 bits): 67 / 64 (or 67 >> 6) = 1 <--- sequence number maps to the second bitmap
    const auto sequenceNumberBitmapCount = (sequenceNumber >> BITMAP_SIZE_BIT_SHIFT);
    /// Calculate the sequenceNumber that the first bit of the bitmap that the sequence number belongs to represents
    /// running example: 1 * 64 (or 1 << 6) = 64
    const auto sequenceNumberBitmapOffset = sequenceNumberBitmapCount << BITMAP_SIZE_BIT_SHIFT;
    /// Determine the correct index of the bit in its bitmap and shift a '1' to it
    /// running example: 67 % 64 (or 67 & 63) = 3 <-- sequence number 67 maps to the third bit of the second bitmap
    const auto sequenceNumberBitIndex = sequenceNumber & BITMAP_SIZE_MODULO;
    const auto sequenceNumberBit = FIRST_BIT_MASK << sequenceNumberBitIndex;

    /// Create two masks, one where all bits lower than the bit of the sequence number are '1's and one in which
    /// all the higher bits are '1's, e.g.: (00100) -> (00011 and 11000)
    const auto lowerBitsMask = sequenceNumberBit - 1;
    const auto higherBitsMask = MAX_VALUE ^ (lowerBitsMask | sequenceNumberBit);

    Snapshot snapshot;
    bool needToCheckForWrappingToLower = false;
    bool needToCheckForWrappingToHigher = false;
    SequenceNumberType sequenceNumberBufferPosition{};
    SequenceNumberType sequenceNumberBitmapIndex{};
    /// protect: read(tail,numberOfBitmapsModulo), read(tupleDelimiterBitmaps, seenAndUsedBitmaps)
    {
        const std::scoped_lock lock(this->readWriteMutex);
        sequenceNumberBufferPosition = sequenceNumber & (this->stagedBuffers.size() - 1);
        /// The SequenceShredder takes ownership of the staged buffer and returns it, once its uses reaches '0'
        this->stagedBuffers[sequenceNumberBufferPosition] = stagedBufferOfSequenceNumber;
        sequenceNumberBitmapIndex
            = sequenceNumberBitmapCount & this->numberOfBitmapsModulo; /// Needs protection because numBitsModule is variable

        /// Set the bit in the correct bitmap, depending on whether it contains a tuple delimiter or not
        if constexpr (HasTupleDelimiter)
        {
            /// A buffer with a delimiter has three uses. To construct the leading, and to construct the trailing spanning tuple,
            /// and to return the buffer, in case it contains full tuples
            this->stagedBufferUses[sequenceNumberBufferPosition] = 3;
            this->tupleDelimiterBitmaps[sequenceNumberBitmapIndex] |= sequenceNumberBit;
        }
        else
        {
            /// A buffer without a delimiter can only construct a single spanning tuple.
            this->stagedBufferUses[sequenceNumberBufferPosition] = 1;
            this->seenAndUsedBitmaps[sequenceNumberBitmapIndex] |= sequenceNumberBit;
        }

        /// The wrappingCheckBitmap contains '1's if we saw a sequence number, but it did not have a tuple delimiter:
        /// 0011 <-- seenAndUsedBitmap, 0101 <-- tupleDelimitersBitmap
        /// 0101 | 0011 = 0111 and 0111 ^ 0101 = 0010 <-- the only '1' is exactly where only seen and used had a '1'
        /// Thus, the wrappingCheckBitmap represents possible paths from one tuple delimiter to another.
        const auto wrappingCheckBitmap
            = (this->tupleDelimiterBitmaps[sequenceNumberBitmapIndex] | this->seenAndUsedBitmaps[sequenceNumberBitmapIndex])
            ^ this->tupleDelimiterBitmaps[sequenceNumberBitmapIndex];
        /// If the wrappingCheckBitmap contains all '1's of the lowerBitsMask, prior threads processed all sequence numbers
        /// with smaller sequence numbers that map to the same bitmap and none of the corresponding buffers contained a tuple delimiter.
        /// We need to check if there is a tuple delimiter in a prior bitmap (analog process for higher)
        /// (We could check if the next higher bitmap is the tail, but it does not seem worth it)
        needToCheckForWrappingToLower = ((lowerBitsMask | wrappingCheckBitmap) == wrappingCheckBitmap);
        needToCheckForWrappingToHigher = ((higherBitsMask | wrappingCheckBitmap) == wrappingCheckBitmap);

        /// If neither end is 'reachable' from the bit-index of the sequence number, it suffices to search spanning tuples in
        /// the bitmap of the sequence number. Otherwise, we need to potentially check all other bitmaps.
        /// The thread takes a snapshot of the required bitmap(s). The snapshot allows the thread to look for spanning tuples
        /// without locking and allows it to determine exactly which spanning tuples it completed (see class description).
        if (needToCheckForWrappingToLower or needToCheckForWrappingToHigher)
        { /// assigning using a ternary operator with a cast is slower than simply branching
            snapshot = std::make_unique<BitmapVectorSnapshot>(
                this->tail, this->numberOfBitmapsModulo, this->tupleDelimiterBitmaps, this->seenAndUsedBitmaps);
        }
        else
        {
            snapshot = BitmapSnapshot{
                .numberOfBitmapsModulo = this->numberOfBitmapsModulo,
                .tupleDelimiterBitmapSnapshot = this->tupleDelimiterBitmaps[sequenceNumberBitmapIndex],
                .seenAndUsedBitmapSnapshot = this->seenAndUsedBitmaps[sequenceNumberBitmapIndex]};
        }
    }
    SpanningTuple spanningTuple{};
    /// Determine which kind of wrapping (to other bitmaps) is necessary. The common case should be NO_WRAPPING.
    const auto wrappingMode = static_cast<WrappingMode>(needToCheckForWrappingToLower + (needToCheckForWrappingToHigher << FIRST_BIT_MASK));
    switch (wrappingMode)
    {
        case WrappingMode::NO_WRAPPING: {
            /// Checking for wrapping is not necessary to either lower or higher. We just need to check the current bitmap.
            const auto bitmapSnapshot = std::get<BitmapSnapshot>(snapshot);
            /// Try to find the start and end of the spanning tuple in the bitmap of the sequence number.
            const auto [spanningTupleStart, isStartValid] = tryGetSpanningTupleStart(
                sequenceNumberBitIndex,
                sequenceNumberBitmapOffset,
                bitmapSnapshot.tupleDelimiterBitmapSnapshot,
                bitmapSnapshot.seenAndUsedBitmapSnapshot);
            const auto [spanningTupleEnd, isEndValid] = tryGetSpanningTupleEnd(
                sequenceNumberBitIndex,
                sequenceNumberBitmapOffset,
                bitmapSnapshot.tupleDelimiterBitmapSnapshot,
                bitmapSnapshot.seenAndUsedBitmapSnapshot);

            spanningTuple = SpanningTuple{
                .spanStart = spanningTupleStart, .spanEnd = spanningTupleEnd, .isStartValid = isStartValid, .isEndValid = isEndValid};
            break;
        }
        case WrappingMode::CHECK_WRAPPING_TO_LOWER: {
            const auto bitmapSnapshot = *std::get<std::unique_ptr<BitmapVectorSnapshot>>(snapshot);
            auto [spanningTupleEnd, isEndValid] = tryGetSpanningTupleEnd(
                sequenceNumberBitIndex,
                sequenceNumberBitmapOffset,
                bitmapSnapshot.tupleDelimiterVectorSnapshot[sequenceNumberBitmapIndex],
                bitmapSnapshot.seenAndUsedVectorSnapshot[sequenceNumberBitmapIndex]);

            /// If the buffer of the sequence number has tuple delimiter, we always need to check for wrapping, since
            /// there are two possible spanning tuples, one starting at the sequence number.
            /// If it does not have a tuple delimiter, we can abort early, because start and end must both be valid.
            if (isEndValid or HasTupleDelimiter)
            {
                const auto [spanningTupleStart, isStartValid]
                    = tryToFindLowerWrappingSpanningTuple(sequenceNumberBitmapOffset, sequenceNumberBitmapIndex, bitmapSnapshot);
                spanningTuple = SpanningTuple{
                    .spanStart = spanningTupleStart, .spanEnd = spanningTupleEnd, .isStartValid = isStartValid, .isEndValid = isEndValid};
            }
            else
            {
                spanningTuple = SpanningTuple{
                    .spanStart = INVALID_SEQUENCE_NUMBER, .spanEnd = spanningTupleEnd, .isStartValid = false, .isEndValid = isEndValid};
            }
            break;
        }
        case WrappingMode::CHECK_WRAPPING_TO_HIGHER: {
            const auto bitmapSnapshot = *std::get<std::unique_ptr<BitmapVectorSnapshot>>(snapshot);
            auto [spanningTupleStart, isStartValid] = tryGetSpanningTupleStart(
                sequenceNumberBitIndex,
                sequenceNumberBitmapOffset,
                bitmapSnapshot.tupleDelimiterVectorSnapshot[sequenceNumberBitmapIndex],
                bitmapSnapshot.seenAndUsedVectorSnapshot[sequenceNumberBitmapIndex]);

            /// If the buffer of the sequence number has tuple delimiter, we always need to check for wrapping, since
            /// the there are two possible spanning tuples, one ending with the sequence number.
            /// If it does not have a tuple delimiter, we can abort early, because start and end must both be valid.
            if (spanningTupleStart or HasTupleDelimiter)
            {
                const auto [spanningTupleEnd, isEndValid]
                    = tryToFindHigherWrappingSpanningTuple(sequenceNumberBitmapOffset, sequenceNumberBitmapIndex, bitmapSnapshot);
                spanningTuple = SpanningTuple{
                    .spanStart = spanningTupleStart, .spanEnd = spanningTupleEnd, .isStartValid = isStartValid, .isEndValid = isEndValid};
            }
            else
            {
                spanningTuple = SpanningTuple{
                    .spanStart = spanningTupleStart, .spanEnd = INVALID_SEQUENCE_NUMBER, .isStartValid = isStartValid, .isEndValid = false};
            }
            break;
        }
        case WrappingMode::CHECK_WRAPPING_TO_LOWER_AND_HIGHER: {
            const auto bitmapSnapshot = *std::get<std::unique_ptr<BitmapVectorSnapshot>>(snapshot);
            const auto [spanningTupleStart, isStartValid]
                = tryToFindLowerWrappingSpanningTuple(sequenceNumberBitmapOffset, sequenceNumberBitmapIndex, bitmapSnapshot);
            if (spanningTupleStart or HasTupleDelimiter)
            {
                const auto [spanningTupleEnd, isEndValid]
                    = tryToFindHigherWrappingSpanningTuple(sequenceNumberBitmapOffset, sequenceNumberBitmapIndex, bitmapSnapshot);
                spanningTuple = SpanningTuple{
                    .spanStart = spanningTupleStart, .spanEnd = spanningTupleEnd, .isStartValid = isStartValid, .isEndValid = isEndValid};
            }
            else
            {
                spanningTuple = SpanningTuple{
                    .spanStart = spanningTupleStart, .spanEnd = INVALID_SEQUENCE_NUMBER, .isStartValid = isStartValid, .isEndValid = false};
            }
            break;
        }
    }

    /// Check whether the start/end of the spanning tuple is valid and determine how many STs to return and whether to increment the tail
    const auto numberOfBitmapsModuloSnapshot = (std::holds_alternative<BitmapSnapshot>(snapshot))
        ? std::get<BitmapSnapshot>(snapshot).numberOfBitmapsModulo
        : std::get<std::unique_ptr<BitmapVectorSnapshot>>(std::move(snapshot))->numberOfBitmapsModulo;
    if constexpr (HasTupleDelimiter)
    {
        /// If two other threads already completed the leading and spanning tuple starting/ending in 'stagedBufferOfSequenceNumber', the
        /// stagedBuffers vector might not contain stagedBufferOfSequenceNumber anymore, in that case, the SequenceShredder returns the
        /// original 'stagedBufferOfSequenceNumber'
        return checkSpanningTupleWithTupleDelimiter(
            spanningTuple, sequenceNumber, numberOfBitmapsModuloSnapshot, std::move(stagedBufferOfSequenceNumber));
    }
    else
    {
        const auto spanningTupleIsValid = spanningTuple.isStartValid and spanningTuple.isEndValid;
        if (not spanningTupleIsValid)
        {
            return SpanningTupleBuffers{.indexOfProcessedSequenceNumber = 0, .stagedBuffers = {}};
        }
        return checkSpanningTupleWithoutTupleDelimiter(spanningTuple, sequenceNumber, numberOfBitmapsModuloSnapshot);
    }
}

/// Instantiate processSequenceNumber for both 'true' and 'false' so that the linker knows which templates to generate.
template SequenceShredder::SpanningTupleBuffers SequenceShredder::processSequenceNumber<true>(StagedBuffer, SequenceNumberType);
template SequenceShredder::SpanningTupleBuffers SequenceShredder::processSequenceNumber<false>(StagedBuffer, SequenceNumberType);

void SequenceShredder::incrementTail()
{
    bool hasCompletedTailBitmap = true;
    bool tailWrappedAround = false;
    auto tailBitmapIndex = this->tail & this->numberOfBitmapsModulo;
    while (hasCompletedTailBitmap)
    {
        /// Can't read/write from/to tail or bitmaps, because of torn-reads/writes
        this->tupleDelimiterBitmaps[tailBitmapIndex] = 0;
        this->seenAndUsedBitmaps[tailBitmapIndex] = 0;
        ++this->tail;
        tailWrappedAround |= (tailBitmapIndex == 0);
        tailBitmapIndex = this->tail & this->numberOfBitmapsModulo;
        hasCompletedTailBitmap = (this->seenAndUsedBitmaps[tailBitmapIndex] == MAX_VALUE);
    }

    /// We use the number of bitmaps to map a sequence number to a bitmap (sequenceNumberBitmapIndex = sequenceNumber / SIZE_OF_BITMAP % numberOfBitmaps)
    /// If numberOfBitmaps changes, the bitmap that the SequenceShredder maps a sequence number to may change:
    /// Say the tail is currently 3 and the number of bitmaps is 4. The next bigger number of bitmaps is 8, which
    /// correctly maps sequence numbers in bitmap 3 to [193,256], but '8' does not map sequence numbers [257,320] to the same bitmap that '4' does:
    /// (257-1) / 64 % 4 = 0 and (259-1) / 64 % 4 = 0, but (258-1) / 64 % 8 = 4 (257 and 259 in bitmap 0 would never be connected with 258 in bitmap 4)
    /// Thus, we need to make sure that the current and the next bigger number of bitmaps map all sequence numbers that fit in the current bitmaps
    /// to exactly the same bitmaps.
    /// We guarantee this by only increasing the number of bitmaps if:
    /// 1. The current and the next bigger number of bitmaps map the tail to the same bitmap, which in extension means
    /// that the increase also preserves the mapping of sequence numbers to the bitmaps between the tail and the last bitmap index.
    /// 2. The tail just wrapped around, ensuring that all bitmaps in front of the tail are clean (all 0s, no conflict possible).
    const auto isResizeRequestCountLimitReached = this->resizeRequestCount >= MIN_NUMBER_OF_RESIZE_REQUESTS_BEFORE_INCREMENTING;
    if (isResizeRequestCountLimitReached and tailWrappedAround)
    {
        /// Double the current number of bitmaps
        const auto nextNumberOfBitmaps = this->numberOfBitmaps << 1;
        /// Check if the increase preserves the mapping to the tail bitmap
        const auto nextSizePreservesPlacementsOfTail = ((this->tail) & (nextNumberOfBitmaps - 1)) == tailBitmapIndex;
        /// Check that the increase would not go beyond the maximum number of bitmaps
        const auto isResizeInAllowedRange = (nextNumberOfBitmaps <= MAX_NUMBER_OF_BITMAPS);
        if (nextSizePreservesPlacementsOfTail and isResizeInAllowedRange)
        {
            NES_WARNING("Resizing number of bitmaps from {} to {}", this->numberOfBitmaps, (this->numberOfBitmaps << FIRST_BIT_MASK));
            this->numberOfBitmaps = nextNumberOfBitmaps;
            this->numberOfBitmapsModulo = numberOfBitmaps - 1;
            this->tupleDelimiterBitmaps.resize(numberOfBitmaps);
            this->seenAndUsedBitmaps.resize(numberOfBitmaps);
            this->stagedBuffers.resize(numberOfBitmaps << BITMAP_SIZE_BIT_SHIFT);
            this->stagedBufferUses.resize(numberOfBitmaps << BITMAP_SIZE_BIT_SHIFT);
            this->tupleDelimiterBitmaps.shrink_to_fit();
            this->seenAndUsedBitmaps.shrink_to_fit();
            this->stagedBuffers.shrink_to_fit();
            this->resizeRequestCount = 0;
        }
    }
}

std::pair<SequenceShredder::SequenceNumberType, bool> SequenceShredder::tryGetSpanningTupleStart(
    const SequenceNumberType sequenceNumberBitIndex,
    const SequenceNumberType sequenceNumberBitmapOffset,
    const SequenceNumberType& tupleDelimiterBitmap,
    const SequenceNumberType& seenAndUsedBitmap)
{
    /// 0. Since we mark start indexes of spanning tuples in the seenAndUsedBitmap, it must never occur that a start index
    ///    that already has '1's both in the seenAndUsedBitmap and the tupleDelimiteBitmap. Since, that would mean finding the spanning tuple again.
    ///    We can count the number of consecutive '1's in the lower bits of the seenAndUsedBitmap.
    ///    If the index of the first '0' (following the sequence of '1's) is a tuple delimiter, it marks a valid start of a spanning tuple.
    /// 1. align seen and used so that the highest (left-most) bit is the bit to the right of the sequenceNumberBitIndex
    ///                                                  bit index:      543210                        543210
    ///    example (given: sequenceNumberBitIndex: 3, seenAndUsedBitmap: 000110, tupleDelimiterBitmap: 000001):
    ///    00-0-110 -- shift seenAndUsedBitmapby 3(=6-3) -->  110000
    const auto alignedSeenAndUsed = seenAndUsedBitmap << (SIZE_OF_BITMAP_IN_BITS - sequenceNumberBitIndex);
    /// 2. count the leading number of ones (example: countl_one(110000) = 2), which represents the next reachable tuple delimiter
    const auto offsetToClosestReachableTupleDelimiter = std::countl_one(alignedSeenAndUsed);
    /// 3. calculate the index in the tuple delimiter bitmap (example: 3 - (2+1) = 0)
    const auto indexOfClosestReachableTupleDelimiter = sequenceNumberBitIndex - (offsetToClosestReachableTupleDelimiter + 1);
    /// 4. add the offset of the bitmap the index and add 1 to get the correct sequence number of the tuple delimiter
    const auto sequenceNumberOfTupleClosestReachableTupleDelimiter = sequenceNumberBitmapOffset + indexOfClosestReachableTupleDelimiter;
    /// 5. check if the tuple delimiter bitmap contains a '1' at the index (running example: bool(1 << 0(=0000001) & 0000001) = true)
    const bool isTupleDelimiter = static_cast<bool>((FIRST_BIT_MASK << indexOfClosestReachableTupleDelimiter) & tupleDelimiterBitmap);
    /// 6. return the index of of the closest reachable tuple delimiter and a flag indicating whether it is valid
    return std::make_pair(sequenceNumberOfTupleClosestReachableTupleDelimiter, isTupleDelimiter);
}

std::pair<SequenceShredder::SequenceNumberType, bool> SequenceShredder::tryGetSpanningTupleEnd(
    const SequenceNumberType sequenceNumberBitIndex,
    const SequenceNumberType sequenceNumberBitmapOffset,
    const SequenceNumberType& tupleDelimiterBitmap,
    const SequenceNumberType& seenAndUsedBitmap)
{
    /// 0. We need to look for the first index that is 'reachable' via seen sequence numbers that has a tuple delimiter. That index might either
    ///    have a '1' in both in the seenAndUsedBitmap and the tupleDelimiterBitmap or only the tupleDelimiterBitmap. This is ok, since we only
    ///    mark the indexes with a tuple delimiter as 'seenAndUsed' if they represent the start of a spanning tuple, not the end.
    ///    Thus, we first need to prepare a bitmap, that only contains '1's at indexes, where the seenAndUsedBitmap is '1' and the tupleDelimiterBitmap
    ///    is '0' (all other combinations (0-0, 0-1, 1-1) are '0's (negated implication: not(x -> y) == not(not(x) or y) == x and not(y))
    const auto onlySeenIsOne = seenAndUsedBitmap & ~(tupleDelimiterBitmap);
    /// 1. align seen and used so that the lowest (right-most) bit is the bit to the left of the sequenceNumberBitIndex
    ///                                                  bit index:      543210                        543210
    ///    example (given: sequenceNumberBitIndex: 2, seenAndUsedBitmap: 011000, tupleDelimiterBitmap: 100000):
    ///    011-0-00 -- shift right by 3(=2+1) -->  0000011
    const auto alignedSeenAndUsed = onlySeenIsOne >> (sequenceNumberBitIndex + 1);
    /// 2. count the trailing number of ones + 1 (example: countl_one(000011) + 1 = 2 + 1 = 3), representing the offset of the closest reachable tuple delimiter
    const auto offsetToClosestReachableTupleDelimiter = std::countr_one(alignedSeenAndUsed) + 1;
    /// 3. calculate the index in the tuple delimiter bitmap (example: 2 + 3 = 5)
    const auto indexOfClosestReachableTupleDelimiter = sequenceNumberBitIndex + offsetToClosestReachableTupleDelimiter;
    /// 4. add the offset of the bitmap the index and add 1 to get the correct sequence number of the tuple delimiter
    const auto sequenceNumberOfTupleClosestReachableTupleDelimiter = sequenceNumberBitmapOffset + indexOfClosestReachableTupleDelimiter;
    /// 5. check if the tuple delimiter bitmap contains a '1' at the index (example: bool(1 << 5(=100000) & 100000) = true)
    const bool isTupleDelimiter = (FIRST_BIT_MASK << indexOfClosestReachableTupleDelimiter) & tupleDelimiterBitmap;
    /// 6. return the index of of the closest reachable tuple delimiter and a flag indicating whether it is valid
    return std::make_pair(sequenceNumberOfTupleClosestReachableTupleDelimiter, isTupleDelimiter);
}

std::pair<SequenceShredder::SequenceNumberType, bool> SequenceShredder::tryToFindLowerWrappingSpanningTuple(
    const size_t sequenceNumberBitmapOffset, const size_t currentBitmapIndex, const BitmapVectorSnapshot& bitmapSnapshot)
{
    size_t bitmapIndex = currentBitmapIndex;
    size_t bitmapIndexOffset = 0;
    bool allBuffersSeenButNoDelimiter = true;
    /// Skip bitmaps that consist of only seen buffers without a tuple delimiter
    /// The while loop can never iterate past the current tail, because the tail must contain at least one tuple delimiter
    /// Additionally, if there is still a path to a tuple delimiter, there is still an open spanning tuple start that only this thread can
    /// detect. Thus, no other thread can move the tail past the bitmap that contains that tuple delimiter
    while (allBuffersSeenButNoDelimiter)
    {
        ++bitmapIndexOffset;
        bitmapIndex = (currentBitmapIndex - bitmapIndexOffset) & bitmapSnapshot.numberOfBitmapsModulo;
        allBuffersSeenButNoDelimiter = (bitmapSnapshot.seenAndUsedVectorSnapshot[bitmapIndex] == MAX_VALUE)
            and (bitmapSnapshot.tupleDelimiterVectorSnapshot[bitmapIndex] == 0);
    }

    /// We determine the number of zeros to the first delimiter that must be covered by 1s in the seenAndUsedBitmap for a valid sequence
    const auto potentialStart = std::countl_one(bitmapSnapshot.seenAndUsedVectorSnapshot[bitmapIndex]) + 1;
    const auto indexOfClosestReachableTupleDelimiter = SIZE_OF_BITMAP_IN_BITS - potentialStart;
    const auto sequenceNumberOfClosestReachableTupleDelimiter
        = (sequenceNumberBitmapOffset - (bitmapIndexOffset << BITMAP_SIZE_BIT_SHIFT) + indexOfClosestReachableTupleDelimiter);
    const bool isTupleDelimiter
        = (FIRST_BIT_MASK << indexOfClosestReachableTupleDelimiter) & bitmapSnapshot.tupleDelimiterVectorSnapshot[bitmapIndex];
    return std::make_pair(sequenceNumberOfClosestReachableTupleDelimiter, isTupleDelimiter);
}

std::pair<SequenceShredder::SequenceNumberType, bool> SequenceShredder::tryToFindHigherWrappingSpanningTuple(
    const size_t sequenceNumberBitmapOffset, const size_t currentBitmapIndex, const BitmapVectorSnapshot& bitmapSnapshot)
{
    /// Skip bitmaps that consist of only seen buffers without a tuple delimiter
    /// The while loop can iterate into the tail, if prior threads already processed all buffers in the last bitmap and none of
    /// these buffers contains a tuple delimiter.
    /// Also, the tail might have moved since the thread took the snapshot, allowing for new bitmaps that the spanning tuple might wrap to.
    /// However it is the responsibility of the other threads that write to these new bitmaps to detect the spanning tuple, since only they can
    /// see the entire spanning tuple and they are guaranteed to see the bit corresponding to the sequence number processed by this thread.
    size_t bitmapIndex = currentBitmapIndex;
    size_t bitmapIndexOffset = 0;
    bool allBuffersSeenButNoDelimiter = true;
    while (allBuffersSeenButNoDelimiter)
    {
        ++bitmapIndexOffset;
        bitmapIndex = (currentBitmapIndex + bitmapIndexOffset) & bitmapSnapshot.numberOfBitmapsModulo;
        allBuffersSeenButNoDelimiter = (bitmapSnapshot.seenAndUsedVectorSnapshot[bitmapIndex] == MAX_VALUE)
            and (bitmapSnapshot.tupleDelimiterVectorSnapshot[bitmapIndex] == 0);
    }
    /// Just like 'tryGetSpanningTupleEnd()', bit a different sequence number calculation and an extra tail check
    const auto onlySeenIsOne
        = bitmapSnapshot.seenAndUsedVectorSnapshot[bitmapIndex] & ~(bitmapSnapshot.tupleDelimiterVectorSnapshot[bitmapIndex]);

    const auto indexOfClosestReachableTupleDelimiter = std::countr_one(onlySeenIsOne);
    const auto sequenceNumberOfClosestReachableTupleDelimiter
        = (sequenceNumberBitmapOffset + (bitmapIndexOffset << BITMAP_SIZE_BIT_SHIFT) + indexOfClosestReachableTupleDelimiter);
    const bool isTupleDelimiter = FIRST_BIT_MASK
        << (indexOfClosestReachableTupleDelimiter)&bitmapSnapshot.tupleDelimiterVectorSnapshot[bitmapIndex];
    const auto tailBitmapIndex = bitmapSnapshot.tail & bitmapSnapshot.numberOfBitmapsModulo;
    const auto isNotTailBitmap = bitmapIndex != tailBitmapIndex;
    return std::make_pair(sequenceNumberOfClosestReachableTupleDelimiter, (isTupleDelimiter and isNotTailBitmap));
}

SequenceShredder::SpanningTupleBuffers SequenceShredder::checkSpanningTupleWithoutTupleDelimiter(
    const SpanningTuple& spanningTuple, const SequenceNumberType sequenceNumber, const SequenceNumberType numberOfBitmapsModuloSnapshot)
{
    /// Determine bitmap count, bitmap index and position in bitmap of start of spanning tuple
    const auto bitmapOfSpanningTupleStart = (spanningTuple.spanStart) >> BITMAP_SIZE_BIT_SHIFT;
    const auto bitmapIndexOfSpanningTupleStart = bitmapOfSpanningTupleStart & numberOfBitmapsModuloSnapshot;
    const auto positionOfSpanningTupleStart = ((spanningTuple.spanStart) & BITMAP_SIZE_MODULO);
    /// Check that both the start and the end of the spanning tuple are valid
    /// If both are valid, move a '1' to the bit position of the start of the spanning tuple
    const auto validatedSpanningTupleStartBit = FIRST_BIT_MASK << positionOfSpanningTupleStart;

    /// Collect all buffers that contribute to the spanning tuple.
    std::vector<StagedBuffer> spanningTupleBuffers{};
    const auto numberOfBitmapsSnapshot = numberOfBitmapsModuloSnapshot + 1;
    const auto stagedBufferSizeModulo = (numberOfBitmapsSnapshot << BITMAP_SIZE_BIT_SHIFT) - 1;

    /// protect: read/write(tail,numberOfBitmaps,numberOfBitmapsModulo, seenAndUsedBitmaps), write(tupleDelimiterBitmaps, seenAndUsedBitmaps)
    {
        const std::scoped_lock lock(this->readWriteMutex);
        for (auto spanningTupleIndex = spanningTuple.spanStart; spanningTupleIndex <= spanningTuple.spanEnd; ++spanningTupleIndex)
        {
            const auto adjustedSpanningTupleIndex = spanningTupleIndex & stagedBufferSizeModulo;
            /// A buffer with a tuple delimiter has two uses. One for starting and one for ending a SpanningTuple.
            const auto newUses = --this->stagedBufferUses[adjustedSpanningTupleIndex];
            auto returnBuffer = (newUses == 0) ? std::move(this->stagedBuffers[adjustedSpanningTupleIndex])
                                               : this->stagedBuffers[adjustedSpanningTupleIndex];
            spanningTupleBuffers.emplace_back(std::move(returnBuffer));
        }
        /// Mark the spanning tuple as completed, by setting the start of the spanning tuple to 1 (if it is valid)
        this->seenAndUsedBitmaps[bitmapIndexOfSpanningTupleStart] |= validatedSpanningTupleStartBit;
        /// Check if the spanning tuple completed a bitmap (set the last bit in corresponding the seenAndUsed bitmap)
        const auto completedBitmap = (this->seenAndUsedBitmaps[bitmapIndexOfSpanningTupleStart] == MAX_VALUE);

        /// Check if the bitmap is the current tail-bitmap, if it is, the current thread needs to increment the tail
        if (completedBitmap and (bitmapOfSpanningTupleStart == this->tail))
        {
            incrementTail();
        }
        isFirstTuple = false;
    }

    const size_t sequenceNumberIndex = sequenceNumber - spanningTuple.spanStart;
    return SpanningTupleBuffers{.indexOfProcessedSequenceNumber = sequenceNumberIndex, .stagedBuffers = std::move(spanningTupleBuffers)};
}

SequenceShredder::SpanningTupleBuffers SequenceShredder::checkSpanningTupleWithTupleDelimiter(
    SpanningTuple spanningTuple,
    const SequenceNumberType sequenceNumber,
    const SequenceNumberType numberOfBitmapsModuloSnapshot,
    StagedBuffer stagedBufferOfSequenceNumber)
{
    /// Determine bitmap count, bitmap index and position in bitmap of start of spanning tuple
    const auto bitmapOfSpanningTupleStart = (spanningTuple.spanStart) >> BITMAP_SIZE_BIT_SHIFT;
    const auto bitmapIndexOfSpanningTupleStart = bitmapOfSpanningTupleStart & numberOfBitmapsModuloSnapshot;
    const auto positionOfSpanningTupleStart = ((spanningTuple.spanStart) & BITMAP_SIZE_MODULO);
    /// Determine bitmap count, bitmap index and position in bitmap of ssequence number
    const auto bitmapOfSequenceNumber = sequenceNumber >> BITMAP_SIZE_BIT_SHIFT;
    const auto bitmapIndexOfSpanningSequenceNumber = bitmapOfSequenceNumber & numberOfBitmapsModuloSnapshot;
    const auto positionOfSequenceNumber = (sequenceNumber & BITMAP_SIZE_MODULO);
    /// If they are valid, move the corresponding '1's to the bit positions of the starts of the respective spanning tuples
    const auto firstValidatedSpanningTupleStartBit = (static_cast<SequenceNumberType>(spanningTuple.isStartValid))
        << positionOfSpanningTupleStart;
    const auto secondValidatedSpanningTupleStartBit = (static_cast<SequenceNumberType>(spanningTuple.isEndValid))
        << positionOfSequenceNumber;

    std::vector<StagedBuffer> returnBuffers{};
    const auto startIndex = (spanningTuple.isStartValid) ? spanningTuple.spanStart : sequenceNumber;
    const auto endIndex = (spanningTuple.isEndValid) ? spanningTuple.spanEnd : sequenceNumber;
    const auto usingBufferForLeadingSpanningTuple = static_cast<int8_t>(startIndex < sequenceNumber);
    const auto usingBufferForTrailingSpanningTuple = static_cast<int8_t>(sequenceNumber < endIndex);
    const auto numberOfBitmapsSnapshot = numberOfBitmapsModuloSnapshot + 1;
    const auto stagedBufferSizeModulo = (numberOfBitmapsSnapshot * SIZE_OF_BITMAP_IN_BITS) - 1;

    /// protect: read/write(tail,numberOfBitmaps,numberOfBitmapsModulo, seenAndUsedBitmaps), write(tupleDelimiterBitmaps, seenAndUsedBitmaps)
    {
        const std::scoped_lock lock(this->readWriteMutex);
        /// If the sequenceNumber is behind the current tail, two other threads completed the leading/trailing spanning tuples starting/ending
        /// in 'stagedBufferOfSequenceNumber'. Thus, the 'stagedBufferOfSequenceNumber' has no more uses and we can safely return it.
        if (const auto minSequenceNumber = this->tail << BITMAP_SIZE_BIT_SHIFT; sequenceNumber < minSequenceNumber)
        {
            const auto adjustedSpanningTupleIndex = sequenceNumber & stagedBufferSizeModulo;
            const auto sequenceShredderStillOwnsBuffer = stagedBuffers[adjustedSpanningTupleIndex].buffer.getBuffer() != nullptr
                and (stagedBuffers[adjustedSpanningTupleIndex].buffer.getSequenceNumber()
                     == stagedBufferOfSequenceNumber.buffer.getSequenceNumber());
            /// If the sequence shredder still owns the 'stagedBufferOfSequenceNumber', return its ownerhip
            auto returnBuffer = (sequenceShredderStillOwnsBuffer) ? std::move(this->stagedBuffers[adjustedSpanningTupleIndex])
                                                                  : std::move(stagedBufferOfSequenceNumber);
            returnBuffers.emplace_back(std::move(returnBuffer));
            return SpanningTupleBuffers{.indexOfProcessedSequenceNumber = 0, .stagedBuffers = std::move(returnBuffers)};
        }
        for (auto spanningTupleIndex = startIndex; spanningTupleIndex <= endIndex; ++spanningTupleIndex)
        {
            const auto adjustedSpanningTupleIndex = spanningTupleIndex & stagedBufferSizeModulo;
            /// A buffer with a tuple delimiter has up to three uses:
            /// 1. Formatting the tuples in the buffer, 2. Formatting the leading spanning tuple, 3. Formatting the trailing spanning tuple
            const int8_t uses = (spanningTupleIndex != sequenceNumber)
                ? static_cast<int8_t>(1)
                : static_cast<int8_t>(1) + usingBufferForLeadingSpanningTuple + usingBufferForTrailingSpanningTuple;
            this->stagedBufferUses[adjustedSpanningTupleIndex] -= uses;
            const auto newUses = this->stagedBufferUses[adjustedSpanningTupleIndex];
            INVARIANT(newUses >= 0, "Uses can never be negative");
            auto returnBuffer = (newUses == 0) ? std::move(this->stagedBuffers[adjustedSpanningTupleIndex])
                                               : this->stagedBuffers[adjustedSpanningTupleIndex];
            returnBuffers.emplace_back(std::move(returnBuffer));
        }
        /// Mark the spanning tuple as completed, by setting the start of the spanning tuple to 1 (if it is valid)
        this->seenAndUsedBitmaps[bitmapIndexOfSpanningTupleStart] |= firstValidatedSpanningTupleStartBit;
        this->seenAndUsedBitmaps[bitmapIndexOfSpanningSequenceNumber] |= secondValidatedSpanningTupleStartBit;
        /// Check if either of the two spanning tuples completed a bitmap (set the last bit in corresponding the seenAndUsed bitmap)
        const auto firstSpanningTupleCompletedBitmap
            = ((seenAndUsedBitmaps[bitmapIndexOfSpanningTupleStart] == MAX_VALUE) and spanningTuple.isStartValid);
        const auto secondSpanningTupleCompletedBitmap
            = ((seenAndUsedBitmaps[bitmapIndexOfSpanningSequenceNumber] == MAX_VALUE) and spanningTuple.isEndValid);
        /// Check if either of the two bitmaps is the current tail-bitmap
        const bool firstSpanningTupleCompletedTailBitmap
            = firstSpanningTupleCompletedBitmap and ((bitmapOfSpanningTupleStart) == this->tail);
        const bool secondSpanningTupleCompletedTailBitmap = secondSpanningTupleCompletedBitmap and ((bitmapOfSequenceNumber) == this->tail);

        /// If one of the two spanning tuples completed a bitmap that is the current tail bitmap, the thread needs to increment the tail
        if (firstSpanningTupleCompletedTailBitmap or secondSpanningTupleCompletedTailBitmap)
        {
            incrementTail();
        }
        isFirstTuple = false;
    }

    const size_t sequenceNumberIndex = sequenceNumber - startIndex;
    return SpanningTupleBuffers{.indexOfProcessedSequenceNumber = sequenceNumberIndex, .stagedBuffers = std::move(returnBuffers)};
}

void bitmapToString(const SequenceShredder::SequenceNumberType bitmap, std::ostream& os)
{
    constexpr SequenceShredder::SequenceNumberType numberOfBitsInByte = 8;
    for (SequenceShredder::SequenceNumberType i = 0; i < (sizeof(SequenceShredder::SequenceNumberType) * numberOfBitsInByte); ++i)
    {
        os << ((bitmap >> i) & 1);
    }
}

namespace
{
std::string bitmapsToString(const std::vector<SequenceShredder::SequenceNumberType>& bitmaps)
{
    if (bitmaps.empty())
    {
        return "";
    }

    std::stringstream ss;
    bitmapToString(bitmaps.front(), ss);
    for (const auto& bitmap : bitmaps | std::views::drop(1))
    {
        ss << "-";
        bitmapToString(bitmap, ss);
    }
    return ss.str();
}
}

std::ostream& operator<<(std::ostream& os, SequenceShredder& sequenceShredder)
{
    const std::scoped_lock lock(sequenceShredder.readWriteMutex);

    if (VERBOSE_DEBUG_BITMAP_PRINTING)
    {
        os << fmt::format(
            "SequenceShredder(number of bitmaps: {}, resize request count: {}, tail: {}, tupleDelimiterBitmaps: {}, seenAndUsedBitmaps: "
            "{})",
            sequenceShredder.numberOfBitmaps,
            sequenceShredder.resizeRequestCount,
            sequenceShredder.tail,
            bitmapsToString(sequenceShredder.tupleDelimiterBitmaps),
            bitmapsToString(sequenceShredder.seenAndUsedBitmaps));
    }
    else
    {
        os << fmt::format(
            "SequenceShredder(number of bitmaps: {}, resize request count: {}, tail: {})",
            sequenceShredder.numberOfBitmaps,
            sequenceShredder.resizeRequestCount,
            sequenceShredder.tail);
    }

    return os;
}
}
