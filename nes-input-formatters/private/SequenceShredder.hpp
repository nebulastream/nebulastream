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

#include <algorithm>
#include <array>
#include <bit>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>


namespace NES::InputFormatters
{
// Todo: does the design brake with big/little endian?
// Todo: handle sequence number wrapping (prevent SNs to reach MAX_VALUE and make sure that re-starting is seamless)
//  -> write test

/*
     The SequenceShredder is a thread-safe data structure that assigns responsibilities to threads that
     concurrently process input formatter tasks originating from the same source.

     ---- The Problem
     Sources produce unformatted buffers containing raw bytes. Commonly, tuples span over at least two, but possibly many buffers.
     Example (A tuple is just one string, each buffer contains 4 bytes, TN means thread with id N, and Xms means the offset
              in milliseconds that a thread checks for spanning tuples in comparison to the thread that performs the check first)
           1     2     3     4     5     6     7     (<-- sequence numbers (SNs))
         ["Thi][s is][ a s][pann][ing ][tupl][e!"\n] (<-- '\n' is a tuple delimiter (ASCII))
           |     |     |     |     |     |     |
          T3(0ms)|    T2(7ms)|    T3(5ms)|    T1(14ms)
                T1(10ms)      T4(9ms)  T3(16ms)
     A single tuple may span over multiple buffers. Threads may process each of the buffers that forms the tuple in arbitrary order.
     If a thread processes the first sequence number (T3(0ms)) or sees a tuple delimiter, it knows that
     it starts a tuple, but it does not necessarily know where it ends.
     If a thread sees a tuple delimiter (T1(14ms)), it knows that it terminates tuple, but it does not necessarily know where it starts.
     If a thread sees no tuple delimiter, it does neither know where the tuple that the buffer belongs to starts, nor where it ends.
     Thus, we need to synchronize between threads. At least one thread must see the start and the end of the spanning tuple.
     Exactly one thread must format the tuple and pass it to the next operator to avoid duplication.

     ---- The Solution
     Given that NebulaStream must perform the above synchronization for every single source and that a fundamental
     design goal of NebulaStream is to support as many sources as possible on a single node, we opt for a design that:
     I) has small memory footprint, II) can handle arbitrary many sequence numbers, III) synchronization does not bottleneck the entire query
     I: Fundamentally, a buffer may either contain a tuple delimiter or not. A spanning tuple is a tuple that starts in
        buffer with a tuple delimiter, spans over an arbitrary amount of tuples without a tuple delimiter and ends in buffer
        with a tuple delimiter. Thus, it is sufficient to remember for each buffer, whether it has a tuple delimiter and
        whether we saw and used it. We can achieve this using two bitmaps. One denotes whether a tuple buffer contains a
        delimiter with a set '1', the other denotes whether we saw a buffer and made complete use of it with a '1'. If a
        buffer does not contain a tuple delimiter we immediately set a '1' in this bitmap. If a buffer contains a tuple
        delimiter, we set a '1', only when we complete the tuple that starts in the buffer.
             1000001 <--- (15ms) tuple delimiters for the example above
             0111100 <--- (15ms) saw and used for the example above (buffer 6 is still missing)
             1000001 <--- (16ms) tuple delimiters for example above
             1111110 <--- (16ms) T3 processes buffer 6, it sees that it can reach buffer 1 and 7, which both have tuple delimiters)
        A buffer can reach a tuple delimiter, if all buffers between the buffer and the buffer containing the tuple delimiter
        were seen and used already, but do not contain a tuple delimiter. That is, there is a sequence of 0s in the tuple
        delimiter bitmap, exactly contrasted by a sequence of 1s in the corresponding seen and used bitmap.
        This representation allows us to find all spanning tuples using just 2 bits per buffer. Two bitmaps of size 64 bit
        represent 64 buffers. The default size of buffer in NebulaStream is the page size (4096 KB). A single pair of bitmaps
        (128 bit) can therefore represent 64 * 4096KB = 262144 KB worth of buffer data, meaning the bitmaps are cache friendly.
        The SequenceShredder starts with 8 pairs of bitmaps. That is, 8 * 128 = 1024 bits. These 1024 bits represent
        512 buffers, which means 512 * 4096KB = 2097152KB ~= 2GB. Representing 1 million sources, takes 1.000.000 * 1024 bits ~= 500KB.
     II: To address the concern of arbitrarily increasing sequence numbers, the SequenceShredder implements a dynamically increasing ring buffer.
         Each slot in the ring buffer represents two bitmaps. If a thread requests to process a sequence number that is not
         in the range of the ring buffer, the thread puts its task back to the queue. The SequenceShredder keeps track of
         these events and increases the size of the ring buffer if a missed sequence number occurred often enough.
         The SequenceShredder can also handle sequence numbers that wrap back to the initial value of 1.
     III: The SequenceShredder needs to synchronize between concurrently executing threads. Every buffer, produced by the
          source that the SequenceShredder belongs to, must pass through the sequence shredder. The SequenceShredder
          may slow down an entire query, if synchronization takes too long. Additionally, the design should be robust, since
          it effects (almost) every single value processed by NebulaStream.
          We use 3 locks four synchronization, aiming to keep the instructions in each protected section as simple as possible.
          The first lock is to safely read (avoiding torn-reads) the current tail and determine whether the sequence number
          is in range.
          The second lock is the first lock in 'processSequenceNumber()'. In its protected section, the thread first
          denotes in the bitmaps, whether it found a tuple delimiter in its buffer or not. Second, it determines
          if it is sufficient to just look at the bitmap that its sequence number maps too, or whether it may find a spanning tuple
          that spans to another bitmap. It then either takes a snapshot(copy) of only its own bitmap, or of all bitmaps (copying
          all bitmaps is the uncommon case, but still fast). This snapshot has two major advantages. First, all bits set
          in the bitmap were set by threads that arrived in the lock-protected section earlier
          and that consequently did not see the bit just set by the thread.
             T1   T2        T1   T2
             X    |    or   |    X   <--- T1/T2 is first, sets its bit, and does not see the bit set by the other thread
             |    X         X    |   <--- T2/T1 then comes second, sets its bit, and sees the bit of the other thread
          Thus, if a thread sees that it just added the last bit which completes a spanning tuple, it knows that no other
          thread that came before saw this last bit and since all sequence numbers belonging to that spanning tuple were
          seen already, it knows that no later thread will find the same spanning tuple. Thus, the thread knows that it is
          the only thread that was able to find the spanning tuple and can therefore safely forward it.
          The second major advantage is that the thread can search for spanning tuples using its snapshot, without need for
          locking.
          When a thread finds a spanning tuple, it requires a third lock. It sets a bit in the seen and used bitmap to
          tell the other threads that the tuple delimiter that started the bitmap was now used. It then determines whether
          it just completed the tail bitmap and therefore has to move the tail. A thread completes a bitmap if it flips
          the last non-'1' bit in the seen and used bitmap to '1'. It means that no more spanning tuple can start in this bitmap.
          If the bitmap happens to be the tail bitmap, it means that no prior bitmap can still start a spanning tuple, which
          means that no more spanning tuples can end in the bitmap. It is safe to reset it and move the tail.
          If a thread detects that it completed the tail bitmap, it keeps the lock and increases the tail, until it finds
          a bitmap that is not yet complete (all bits in the seen and used bitmap set to '1').
*/

class SequenceShredder
{
public:
    using SequenceNumberType = uint64_t;
    static constexpr size_t SIZE_OF_BITMAP_IN_BITS = sizeof(SequenceNumberType) * 8;
    static constexpr size_t INITIAL_NUM_BITMAPS = 4;
    using BitmapVectorType = std::vector<SequenceNumberType>;

    struct StagedBuffer
    {
        NES::Memory::TupleBuffer buffer;
        size_t sizeOfBufferInBytes;
        size_t offsetOfFirstTupleDelimiter;
        size_t offsetOfLastTupleDelimiter;
        uint8_t uses;
    };

    struct StagedBufferResult //Todo: rename
    {
        size_t indexOfSequenceNumberInStagedBuffers;
        std::vector<StagedBuffer> stagedBuffers;
    };

private:
    static constexpr SequenceNumberType MAX_VALUE = std::numeric_limits<SequenceNumberType>::max();
    static constexpr size_t BITMAP_SIZE_BIT_SHIFT = std::countr_zero(SIZE_OF_BITMAP_IN_BITS); //log2 of size of bitmaps
    static constexpr size_t BITMAP_SIZE_MODULO = (SIZE_OF_BITMAP_IN_BITS - 1);
    static constexpr SequenceNumberType FIRST_BIT_MASK = 1;
    static constexpr SequenceNumberType LAST_BIT_MASK = (MAX_VALUE - 1);
    static constexpr SequenceNumberType INVALID_SEQUENCE_NUMBER = 0;
    static constexpr size_t MAX_NUMBER_OF_BITMAPS = 256;
    static constexpr size_t SHIFT_TO_SECOND_BIT = 1;
    static constexpr size_t SHIFT_TO_THIRD_BIT = 2;
    static constexpr size_t MIN_NUMBER_OF_RESIZE_REQUESTS_BEFORE_INCREMENTING = 5;

    /// The sequence shredder returns 0, 1, or 2 SpanningTuples
    /// The spanning tuple(s) tell the thread that called 'processSequenceNumber()', the range of buffers it needs to format.
    struct SpanningTuple
    {
        SequenceNumberType spanStart = 0;
        SequenceNumberType spanEnd = 0;
        bool isStartValid = false;
        bool isEndValid = false;
    };
public:
    explicit SequenceShredder(const size_t sizeOfTupleDelimiter)
        : tail(0)
        , tupleDelimiterBitmaps(BitmapVectorType(INITIAL_NUM_BITMAPS))
        , seenAndUsedBitmaps(BitmapVectorType(INITIAL_NUM_BITMAPS))
        , numberOfBitmaps(INITIAL_NUM_BITMAPS)
        , numberOfBitmapsModulo(INITIAL_NUM_BITMAPS - 1)
        , resizeRequestCount(0)
        , stagedBuffers(std::vector<StagedBuffer>(numberOfBitmaps << BITMAP_SIZE_BIT_SHIFT))
    {
        this->tupleDelimiterBitmaps.shrink_to_fit();
        this->seenAndUsedBitmaps.shrink_to_fit();

        // Todo: with the current approach, the max sequence number(18446744073709551615) would map exactly to the last bit of the largest possible bitmap
        // Todo: problem:
        // - there is one invalid sequence number, which means that there will never be possible to cleanly fill all bits and wrap around with the largest SN
        // - potential solution: initialize (and skip) first bit, detect wrapping around (max SN), and initialize again
        //      - first initialization: tupDel, with no data (implicit tupDel of first buffer)
        //      - other initializations: noTupDel, no data (connects next buffer to prior buffer, without interfering)
        tupleDelimiterBitmaps[0] |= static_cast<SequenceNumberType>(1);
        stagedBuffers[0] = StagedBuffer{NES::Memory::TupleBuffer{}, sizeOfTupleDelimiter, 0, 0, 1};
    };

    explicit SequenceShredder(const size_t initialTail, const size_t sizeOfTupleDelimiter)
        : tail(initialTail)
        , tupleDelimiterBitmaps(BitmapVectorType(INITIAL_NUM_BITMAPS))
        , seenAndUsedBitmaps(BitmapVectorType(INITIAL_NUM_BITMAPS))
        , numberOfBitmaps(INITIAL_NUM_BITMAPS)
        , numberOfBitmapsModulo(INITIAL_NUM_BITMAPS - 1)
        , resizeRequestCount(0)
        , stagedBuffers(std::vector<StagedBuffer>(numberOfBitmaps << BITMAP_SIZE_BIT_SHIFT))
    {
        this->tupleDelimiterBitmaps.shrink_to_fit();
        this->seenAndUsedBitmaps.shrink_to_fit();

        tupleDelimiterBitmaps[0] |= static_cast<SequenceNumberType>(1);
        stagedBuffers[0] = StagedBuffer{NES::Memory::TupleBuffer{}, sizeOfTupleDelimiter, 0, 0, 1};
    }

    [[nodiscard]] size_t getTail() const { return this->tail; }

    /// Check if the sequence number is in the allowed range of the ring buffer.
    /// Example: 4 bitmaps, tail is 0, then the allowed range for sequence numbers is 1-256: [1,64][65,128][129,192][193,256]
    [[nodiscard]] bool isSequenceNumberInRange(const SequenceNumberType sequenceNumber)
    {
        const auto targetBitmap = (sequenceNumber >> BITMAP_SIZE_BIT_SHIFT);
        const std::scoped_lock lock(readWriteMutex); /// protects: write(resizeRequestCount), read(tail,numberOfBitmaps)
        const auto isInRange = targetBitmap < (this->tail + numberOfBitmaps);
        if (isInRange)
        {
            return true;
        }
        ++resizeRequestCount;
        return false;
    }

    /// Since EoF/EoS is not a symbol that allows the parser to tell that the final tuple just ended, we require a function
    /// that inserts an artificial tuple delimiter that completes the last tuple in the final buffer and flushes it.
    /// The artificial tuple is a buffer with a sequence number (SN) that is exactly one larger than the largest seen SN.
    /// We configure the buffer to 'contain' a tuple delimiter as its first and only content.
    [[nodiscard]] StagedBufferResult flushFinalPartialTuple()
    {
        // Todo: improve locking
        readWriteMutex.lock(); /// protects: write(resizeRequestCount), read(tail,numberOfBitmaps)
        for (size_t offsetToTail = 1; offsetToTail <= numberOfBitmaps; ++offsetToTail)
        {
            const auto bitmapIndex = (this->tail + (numberOfBitmaps - offsetToTail)) & numberOfBitmapsModulo;
            const auto seenAndUsedBitmap = seenAndUsedBitmaps[bitmapIndex];
            const auto tupleDelimiterBitmap = tupleDelimiterBitmaps[bitmapIndex];
            /// Reverse-search bitmaps, until a bitmap is not 0, meaning, it represents a buffer that is in the stagedBuffers vector
            if ((seenAndUsedBitmap | tupleDelimiterBitmap) != 0)
            {
                /// If the last buffer contains a buffer that does not contain a delimiter, we need to check for a spanning tuple
                /// We construct a dummy staged buffer and set its sequence number to exactly one higher than the largest seen sequence number.
                /// The dummy staged buffer flushes out all prior buffers that still depended on a tuple delimiter that did not appear,
                /// because an EOF/EOS is not a tuple delimiter.
                const auto firstSequenceNumberOfTail = this->tail * SIZE_OF_BITMAP_IN_BITS;
                const auto sequenceNumberOffsetOfBitmap = ((numberOfBitmaps - offsetToTail) & numberOfBitmapsModulo)
                    << BITMAP_SIZE_BIT_SHIFT;
                const auto firstSequenceNumberOfBitmap = firstSequenceNumberOfTail + sequenceNumberOffsetOfBitmap;
                const auto numberOfNotSeenSequenceNumbersInBitmap = std::countl_zero(seenAndUsedBitmap | tupleDelimiterBitmap);
                const auto offsetToNextLargerSequenceNumber = SIZE_OF_BITMAP_IN_BITS - numberOfNotSeenSequenceNumbersInBitmap;
                const auto nextLargestSequenceNumber = firstSequenceNumberOfBitmap + offsetToNextLargerSequenceNumber;
                const auto dummyStagedBuffer = StagedBuffer{
                    .buffer = NES::Memory::TupleBuffer{},
                    .sizeOfBufferInBytes = 0,
                    .offsetOfFirstTupleDelimiter = 0,
                    .offsetOfLastTupleDelimiter = 0,
                    .uses = 1};
                readWriteMutex.unlock();
                // Todo: test correctness, when next largest sequence number is in next bitmap
                return processSequenceNumber<true>(dummyStagedBuffer, nextLargestSequenceNumber);
            }
        }
        readWriteMutex.unlock();
        return StagedBufferResult{};
    }

    /// Thread-safely checks if the buffer represented by the sequence number completes spanning tuples.
    /// Returns a vector with either zero, one or two spanning tuples if the sequence number is in range.
    /// Returns a vector with three empty spanning tuples if the sequence number is out of range.
    /// For details on the inner workings of this function, read the description of the class above.
    template <bool HasTupleDelimiter>
    StagedBufferResult
    // Todo: get rid of sequence number type?
    processSequenceNumber(StagedBuffer stagedBufferOfSequenceNumber, const SequenceNumberType sequenceNumber)
    {
        /// Calculate how many bitmaps preceed the bitmap that the sequence number maps to ((sequenceNumber -1) / SIZE_OF_BITMAP_IN_BITS).
        const auto sequenceNumberBitmapCount = (sequenceNumber >> BITMAP_SIZE_BIT_SHIFT);
        /// Calculate the sequenceNumber (-1) that the first bit of the bitmap that the sequence number belongs to represents
        const auto sequenceNumberBitmapOffset = sequenceNumberBitmapCount << BITMAP_SIZE_BIT_SHIFT;
        /// Determine the correct index of the bit in its bitmap and shift a '1' to it
        const auto sequenceNumberBitIndex = sequenceNumber & BITMAP_SIZE_MODULO;
        const auto sequenceNumberBit = FIRST_BIT_MASK << sequenceNumberBitIndex;

        /// Create two masks, one where all bits lower than the bit of the sequence number are '1's and one in which
        /// all the higher bits are '1's, e.g.: (00100) -> (00011 and 11000)
        const auto lowerBitsMask = sequenceNumberBit - 1;
        const auto higherBitsMask = MAX_VALUE ^ (lowerBitsMask | sequenceNumberBit);

        Snapshot snapshot;
        readWriteMutex.lock(); /// protects: read(tail,numberOfBitmapsModulo), read(tupleDelimiterBitmaps, seenAndUsedBitmaps)
        const auto sequenceNumberBufferPosition = sequenceNumber & (stagedBuffers.size() - 1);
        stagedBuffers.at(sequenceNumberBufferPosition) = std::move(stagedBufferOfSequenceNumber);
        const auto sequenceNumberBitmapIndex
            = sequenceNumberBitmapCount & numberOfBitmapsModulo; /// Needs protection because numBitsModule is variable

        /// Set the bit in the correct bitmap, depending on whether it contains a tuple delimiter or not.
        if constexpr (HasTupleDelimiter)
        {
            tupleDelimiterBitmaps[sequenceNumberBitmapIndex] |= sequenceNumberBit;
        }
        else
        {
            seenAndUsedBitmaps[sequenceNumberBitmapIndex] |= sequenceNumberBit;
        }

        /// The wrappingCheckBitmap contains '1's if we saw sequence number, but it did not have a tuple delimiter:
        /// 0011 <-- seen and used, 0101 <-- tuple delimiters
        /// 0101 | 0011 = 0111 and 0111 ^ 0101 = 0010 <-- the only '1' is exactly where only seen and used had a '1'
        /// Thus, the wrappingCheckBitmap represents possible paths from one tuple delimiter to another.
        const auto wrappingCheckBitmap = (tupleDelimiterBitmaps[sequenceNumberBitmapIndex] | seenAndUsedBitmaps[sequenceNumberBitmapIndex])
            ^ tupleDelimiterBitmaps[sequenceNumberBitmapIndex];
        /// If the wrappingCheckBitmap contais all '1's of the lowerBitsMask, prior threads processed all sequence numbers
        /// with smaller sequence numbers that map to the same bitmap and none of the corresponding buffers contained a tuple delimiter.
        /// We need to check if there is a tuple delimiter in a prior bitmap (analog for higher)
        /// (We could check if the next higher bitmap is the tail, but it does not seem worth it)
        const auto needToCheckForWrappingToLower = ((lowerBitsMask | wrappingCheckBitmap) == wrappingCheckBitmap);
        const auto needToCheckForWrappingToHigher = ((higherBitsMask | wrappingCheckBitmap) == wrappingCheckBitmap);

        /// If neither end is 'reachable' from the bit-index of the sequence number, it suffices to search spanning tuples in
        /// the bitmap of the sequnece number. Otherwise, we need potentially check all other bitmaps.
        /// The thread takes a snapshot of the required bitmap(s). The snapshot allows the thread to look for spanning tuples
        /// without locking and allows it to determine exactly which spanning tuples it completed (see class description).
        if (needToCheckForWrappingToLower or needToCheckForWrappingToHigher)
        { /// assign with ternary operator with cast seems to be slower than branch
            snapshot = std::make_unique<BitmapVectorSnapshot>(this->tail, numberOfBitmapsModulo, tupleDelimiterBitmaps, seenAndUsedBitmaps);
        }
        else
        {
            snapshot = BitmapSnapshot{
                .numberOfBitmapsModulo = numberOfBitmapsModulo,
                .tupleDelimiterBitmapSnapshot = tupleDelimiterBitmaps[sequenceNumberBitmapIndex],
                .seenAndUsedBitmapSnapshot = seenAndUsedBitmaps[sequenceNumberBitmapIndex]};
        }
        readWriteMutex.unlock();

        SpanningTuple spanningTuple{};
        /// Determine which kind of wrapping (to other bitmaps) is necessary. The common case should be NO_WRAPPING.
        const auto wrappingMode = static_cast<WrappingMode>(needToCheckForWrappingToLower + (needToCheckForWrappingToHigher << 1));
        switch (wrappingMode)
        {
            case WrappingMode::NO_WRAPPING: {
                /// Checking for wrapping is not necessary to either lower or higher. We just need to check the current bitmap.
                const auto bitmapSnapshot = std::get<BitmapSnapshot>(snapshot);
                /// Try to find the start and end of the spanning tuple in the bitmap of the sequence number.
                /// Sets start/end to INVALID_SEQUENCE_NUMBER (0), if it could not find start/end.
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

                spanningTuple = SpanningTuple{spanningTupleStart, spanningTupleEnd, isStartValid, isEndValid};
                break;
            }
            case WrappingMode::CHECK_WRAPPING_TO_LOWER: {
                const auto bitmapSnapshot = *std::get<std::unique_ptr<BitmapVectorSnapshot>>(std::move(snapshot));
                auto [spanningTupleEnd, isEndValid] = tryGetSpanningTupleEnd(
                    sequenceNumberBitIndex,
                    sequenceNumberBitmapOffset,
                    bitmapSnapshot.tupleDelimiterVectorSnapshot[sequenceNumberBitmapIndex],
                    bitmapSnapshot.seenAndUsedVectorSnapshot[sequenceNumberBitmapIndex]);

                /// If the buffer of the sequence number has tuple delimiter, we always need to check for wrapping, since
                /// the there are two possible spanning tuples, one starting at the sequence number.
                /// If it does not have a tuple delimiter, we can abort early, because start and end must both be valid.
                if (isEndValid or HasTupleDelimiter)
                {
                    const auto [spanningTupleStart, isStartValid]
                        = tryToFindLowerWrappingSpanningTuple(sequenceNumberBitmapOffset, sequenceNumberBitmapIndex, bitmapSnapshot);
                    spanningTuple = SpanningTuple{spanningTupleStart, spanningTupleEnd, isStartValid, isEndValid};
                }
                else
                {
                    spanningTuple = SpanningTuple{INVALID_SEQUENCE_NUMBER, spanningTupleEnd, false, isEndValid};
                }
                break;
            }
            case WrappingMode::CHECK_WRAPPING_TO_HIGHER: {
                const auto bitmapSnapshot = *std::get<std::unique_ptr<BitmapVectorSnapshot>>(std::move(snapshot));
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
                    spanningTuple = SpanningTuple{spanningTupleStart, spanningTupleEnd, isStartValid, isEndValid};
                }
                else
                {
                    spanningTuple = SpanningTuple{spanningTupleStart, INVALID_SEQUENCE_NUMBER, isStartValid, false};
                }
                break;
            }
            // Todo: test this case specifically!
            case WrappingMode::CHECK_WRAPPING_TO_LOWER_AND_HIGHER: {
                const auto bitmapSnapshot = *std::get<std::unique_ptr<BitmapVectorSnapshot>>(std::move(snapshot));
                const auto [spanningTupleStart, isStartValid]
                    = tryToFindLowerWrappingSpanningTuple(sequenceNumberBitmapOffset, sequenceNumberBitmapIndex, bitmapSnapshot);
                if (spanningTupleStart or HasTupleDelimiter)
                {
                    const auto [spanningTupleEnd, isEndValid]
                        = tryToFindHigherWrappingSpanningTuple(sequenceNumberBitmapOffset, sequenceNumberBitmapIndex, bitmapSnapshot);
                    spanningTuple = SpanningTuple{spanningTupleStart, spanningTupleEnd, isStartValid, isEndValid};
                }
                else
                {
                    spanningTuple = SpanningTuple{spanningTupleStart, INVALID_SEQUENCE_NUMBER, isStartValid, false};
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
            return checkResultsWithTupleDelimiter(
                spanningTuple, sequenceNumber, sequenceNumberBufferPosition, numberOfBitmapsModuloSnapshot);
        }
        else
        {
            const auto spanningTupleIsValid = spanningTuple.isStartValid and spanningTuple.isEndValid;
            if (not spanningTupleIsValid)
            {
                return StagedBufferResult{.indexOfSequenceNumberInStagedBuffers = 0, .stagedBuffers = {}};
            }
            return checkResultsWithoutTupleDelimiter(
                spanningTuple, sequenceNumberBufferPosition, numberOfBitmapsModuloSnapshot);
        }
    }

private:
    std::mutex readWriteMutex; /// protects all member variable below
    SequenceNumberType tail; /// represents the total number of times that the tail-bitmap changed
    BitmapVectorType tupleDelimiterBitmaps;
    BitmapVectorType seenAndUsedBitmaps;
    size_t numberOfBitmaps;
    size_t numberOfBitmapsModulo;
    size_t resizeRequestCount;
    std::vector<StagedBuffer> stagedBuffers;

    struct BitmapSnapshot
    {
        size_t numberOfBitmapsModulo;
        SequenceNumberType tupleDelimiterBitmapSnapshot;
        SequenceNumberType seenAndUsedBitmapSnapshot;
    };
    struct BitmapVectorSnapshot
    {
        size_t tail;
        size_t numberOfBitmapsModulo;
        BitmapVectorType tupleDelimiterVectorSnapshot;
        BitmapVectorType seenAndUsedVectorSnapshot;
    };
    /// The unique_ptr avoids allocating space for the entire BitmapVectorSnapshot in the common case, which is 'BitmapSnapshot'
    using Snapshot = std::variant<BitmapSnapshot, std::unique_ptr<BitmapVectorSnapshot>>;

    enum class WrappingMode : uint8_t
    {
        NO_WRAPPING = 0, /// 00: checking the bitmap of the sequence number suffices (common case)
        CHECK_WRAPPING_TO_LOWER = 1, /// 01: the sequence number could complete a spanning tuple ending in a lower bitmap
        CHECK_WRAPPING_TO_HIGHER = 2, /// 01: the sequence number could complete a spanning tuple starting in a higher bitmap
        CHECK_WRAPPING_TO_LOWER_AND_HIGHER
        = 3, /// 11: ... could complete a spanning tuple starting in a lower bitmap and ending in a higher bitmap
    };

    /// Called when a thread that it completed the tail bitmaps. The tail bitmaps are complete, if the seen and used bitmap pointed to by the tail is complete.
    /// Resets the tail bitmaps and increments the tail. If new tail bitmaps are complete, repeats the process, until the new tail bitmaps are not complete.
    void incrementTail()
    {
        bool hasCompletedTailBitmap = true;
        bool tailWrappedAround = false;
        auto tailBitmapIndex = this->tail & numberOfBitmapsModulo; // used numberOfBitmapsModulo prior (also below)
        while (hasCompletedTailBitmap)
        {
            /// Can't read/write from/to tail or bitmaps, because of torn-reads/writes
            tupleDelimiterBitmaps[tailBitmapIndex] = 0;
            seenAndUsedBitmaps[tailBitmapIndex] = 0;
            ++this->tail;
            tailWrappedAround |= (tailBitmapIndex == 0);
            tailBitmapIndex = this->tail & numberOfBitmapsModulo;
            hasCompletedTailBitmap = (seenAndUsedBitmaps[tailBitmapIndex] == MAX_VALUE);
        }

        /// We use the number of bitmaps to map a sequence number to a bitmap: sequenceNumber / SIZE_OF_BITMAP % numberOfBitmaps.
        /// If numberOfBitmaps changes the bitmap that the SequenceShredder maps a sequence number to may change:
        /// Say the tail is currently 3 and the number of bitmaps is 4. The next bigger number if bitmaps is 8, which
        /// correctly maps sequence numbers in bitmap 3 to [193,256], but there might already be sequence numbers in the next bitmap, and:
        /// (257-1) / 64 % 4 = 0 and (259-1) / 64 % 4 = 0, but (258-1) / 64 % 8 = 4 (257 and 259 in bitmap 0 would never be connected with 258 in bitmap 4)
        /// Thus, we need to make sure that next bigger number of bitmaps maps all sequence numbers that fit in the current bitmaps
        /// to exactly the same bitmaps.
        /// We guarantee this by only increasing the number of bitmaps if:
        /// 1. The current and the next bigger number of bitmaps map the tail to the same bitmap, which in extension means
        /// that the increase also preserves the mapping of sequence numbers to the bitmaps between the tail and the last bitmap index.
        /// 2. The tail just wrapped around, ensuring that all bitmaps in front of the tail are clean (all 0s, no conflict possible).
        const auto isResizeRequestCountLimitReached = resizeRequestCount >= MIN_NUMBER_OF_RESIZE_REQUESTS_BEFORE_INCREMENTING;
        if (isResizeRequestCountLimitReached and tailWrappedAround)
        {
            /// Double the current number of bitmaps
            const auto nextNumberOfBitmaps = numberOfBitmaps << 1;
            /// Check if the increase preserves the mapping to the tail bitmap
            const auto nextSizePreservesPlacementsOfTail = ((this->tail) & (nextNumberOfBitmaps - 1)) == tailBitmapIndex;
            /// Check that the increase would not go beyond the maximum number of bitmaps
            const auto isResizeInAllowedRange = (nextNumberOfBitmaps <= MAX_NUMBER_OF_BITMAPS);
            if (nextSizePreservesPlacementsOfTail and isResizeInAllowedRange)
            {
                std::cout << "Resizing: " << numberOfBitmaps << " -> " << (numberOfBitmaps << 1) << std::endl;
                numberOfBitmaps = nextNumberOfBitmaps;
                numberOfBitmapsModulo = numberOfBitmaps - 1;
                this->tupleDelimiterBitmaps.resize(numberOfBitmaps);
                this->seenAndUsedBitmaps.resize(numberOfBitmaps);
                this->stagedBuffers.resize(numberOfBitmaps << BITMAP_SIZE_BIT_SHIFT); //Todo: no need to erase buffers?
                this->tupleDelimiterBitmaps.shrink_to_fit();
                this->seenAndUsedBitmaps.shrink_to_fit();
                this->stagedBuffers.shrink_to_fit();
                resizeRequestCount = 0;
            }
        }
    }

    /// Determines whether the buffer of the sequence number connects to buffer with tuple delimiter, which starts a spanning tuple that spans
    /// into the buffer of the sequence number.
    std::pair<SequenceNumberType, bool> tryGetSpanningTupleStart(
        const SequenceNumberType sequenceNumberBitIndex,
        const SequenceNumberType sequenceNumberBitmapOffset,
        const SequenceNumberType& tupleDelimiterBitmap,
        const SequenceNumberType& seenAndUsedBitmap)
    {
        /// 0. It can never occur in a start index that has '1's both in the seenAndUsedBitmap and the tupleDelimiteBitmap, since that would mean finding the spanning tuple again
        ///    Thus, we can safely shift the bitmap just beyond the sequenceNumberBitIndex and count the number of successive '1's in the seenAndUsed bitmap
        ///    The first zero, if it is a tuple delimiter, marks the start of the spanning tuple.
        /// 1. align seen and used so that the highest (left-most) bit is the bit to the right of the sequenceNumberBitIndex
        ///    example (given: sequenceNumberBitIndex: 3, seenAndUsedBitmap: 0000110, tupleDelimiterBitmap: 0000001):
        ///    000-0-110 -- shift by 4(=7-3) -->  1110000
        const auto alignedSeenAndUsed = seenAndUsedBitmap << (SIZE_OF_BITMAP_IN_BITS - sequenceNumberBitIndex);
        /// 2. count the leading number of ones (example: countl_one(1110000) = 3), which represents the next reachable tuple delimiter
        const auto offsetToClosestReachableTupleDelimiter = std::countl_one(alignedSeenAndUsed);
        /// 3. calculate the index in the tuple delimiter bitmap (example: 3 - (3-1) = 1)
        const auto indexOfClosestReachableTupleDelimiter = sequenceNumberBitIndex - (offsetToClosestReachableTupleDelimiter + 1);
        /// 4. add the offset of the bitmap the index and add 1 to get the correct sequence number of the tuple delimiter
        const auto sequenceNumberOfTupleClosestReachableTupleDelimiter = sequenceNumberBitmapOffset + indexOfClosestReachableTupleDelimiter;
        /// 5. check if the tuple delimiter bitmap contains a '1' at the index (example: bool((1 << 1=)0000001 & 0000001) = true)
        const bool isTupleDelimiter = (FIRST_BIT_MASK << indexOfClosestReachableTupleDelimiter) & tupleDelimiterBitmap;
        /// 6. set the return value to 0, if the tuple delimiter bitmap contained a '0' at the index, otherwise preserve it
        return std::make_pair(sequenceNumberOfTupleClosestReachableTupleDelimiter, isTupleDelimiter);
    }

    /// Determines whether the buffer of the sequence number connects to buffer with tuple delimiter, which ends a spanning tuple that originates
    /// from the buffer of the sequence number, if it has a tuple delimiter, or in an earlier buffer.
    std::pair<SequenceNumberType, bool> tryGetSpanningTupleEnd(
        const SequenceNumberType sequenceNumberBitIndex,
        const SequenceNumberType sequenceNumberBitmapOffset,
        const SequenceNumberType& tupleDelimiterBitmap,
        const SequenceNumberType& seenAndUsedBitmap)
    {
        /// 0. It can never occur in a start index that has '1's both in the seenAndUsedBitmap and the tupleDelimiteBitmap, since that would mean finding the spanning tuple again
        ///    However, it is ok to find an end index that has '1's both in the seenAndUsedBitmap and the tupleDelimiteBitmap, because we don't
        ///    mark tuple delimiters with a '1' in the seenAndUsedBitmap, if they represented the end of a spanning tuple
        ///    Thus, to follow the procedure of 'tryGetSpanningTupleStart', we first need prepare a bitmap, that only contains '1's at indexes,
        ///    where the seenAndUsedBitmap is '1' and the tupleDelimiterBitmap is '0' (all other combinations (0-0, 0-1, 1-1) to to zeros
        const auto onlySeenIsOne = seenAndUsedBitmap & ~(tupleDelimiterBitmap);
        /// 1. align seen and used so that the highest (left-most) bit is the bit to the right of the sequenceNumberBitIndex
        ///    example (given: sequenceNumberBitIndex: 3, seenAndUsedBitmap: 0110000, tupleDelimiterBitmap: 1000000):
        ///    011-0-000 -- shift by 4(=3+1) -->  0000011
        const auto alignedSeenAndUsed = onlySeenIsOne >> (sequenceNumberBitIndex + 1);
        /// 2. count the trailing number of ones + 1 (example: countl_one(0000011) + 1 = 2+1 = 3), representing the offset of the closest reachable tuple delimiter
        const auto offsetToClosestReachableTupleDelimiter = std::countr_one(alignedSeenAndUsed) + 1;
        /// 3. calculate the index in the tuple delimiter bitmap (example: 3 + 3 = 6)
        const auto indexOfClosestReachableTupleDelimiter = sequenceNumberBitIndex + offsetToClosestReachableTupleDelimiter;
        /// 4. add the offset of the bitmap the index and add 1 to get the correct sequence number of the tuple delimiter
        const auto sequenceNumberOfTupleClosestReachableTupleDelimiter = sequenceNumberBitmapOffset + indexOfClosestReachableTupleDelimiter;
        /// 5. check if the tuple delimiter bitmap contains a '1' at the index (example: bool((1 << 6=)1000000 & 1000000) = true)
        const bool isTupleDelimiter = (FIRST_BIT_MASK << indexOfClosestReachableTupleDelimiter) & tupleDelimiterBitmap;
        /// 6. set the return value to 0, if the tuple delimiter bitmap contained a '0' at the index, otherwise preserve it
        return std::make_pair(sequenceNumberOfTupleClosestReachableTupleDelimiter, isTupleDelimiter);
    }

    /// Called by a thread that needs to search for the start of a spanning tuple in a lower bitmap than the one that its sequence number maps to.
    /// Skips over buffers that were seen, but did not contain a tuple delimiter, represented by a '0' in the tuple delimiter bitmap and
    /// a '1' in the seen and used bitmap. If the first 'non-0-1-buffer' contains a tuple delimiter, the function returns the sequence number
    /// representing the start of a spanning tuple, otherwise it returns 0 (the INVALID_SEQUENCE_NUMBER).
    std::pair<SequenceNumberType, bool> tryToFindLowerWrappingSpanningTuple(
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
        // Todo: example:
        // SN: 1 -> index '1', second bit
        // 00....10
        // potentialStart = 63 (62 + 1)
        // index: 64 - 63 = 1
        // isTupleDelimiter: 1 << 1 (..10)
        // countl_one can never be 64, 63 is the max that should represent 0/64/128
        // Example for 63:
        // potentialStart: 0 + 1 = 1
        // index: 63
        // Example for 64:
        // potentialStart: 63 + 1 = 64
        // index: 0
        const auto potentialStart = std::countl_one(bitmapSnapshot.seenAndUsedVectorSnapshot[bitmapIndex]) + 1; //Todo: remove - 1?
        const auto indexOfClosestReachableTupleDelimiter = SIZE_OF_BITMAP_IN_BITS - potentialStart;
        const auto sequenceNumberOfClosestReachableTupleDelimiter
            = (sequenceNumberBitmapOffset - (bitmapIndexOffset << BITMAP_SIZE_BIT_SHIFT) + indexOfClosestReachableTupleDelimiter);
        const bool isTupleDelimiter
            = (FIRST_BIT_MASK << indexOfClosestReachableTupleDelimiter) & bitmapSnapshot.tupleDelimiterVectorSnapshot[bitmapIndex];
        return std::make_pair(sequenceNumberOfClosestReachableTupleDelimiter, isTupleDelimiter);
    }

    // - while checking bitmaps, the tail can move
    //      - main observations
    //          - the tail might change up to the current bitmap, BUT NEVER BEYOND
    //              - thus, all ST ends that we find in higher bitmaps remain valid!
    //              - there might be new bitmaps, that we miss, because the tail increased, but if we 'missed' a path to a specific new bitmap
    //                  the thread processing that new bitmap is guaranteed to find that path too and will complete the ST

    /// Called by a thread that needs to search for the end of a spanning tuple in a higher bitmap than the one that its sequence number maps to.
    /// Skips over buffers that were seen, but did not contain a tuple delimiter, represented by a '1' in the tuple delimiter bitmap and a '0'
    /// seen and used bitmap. If the first 'non-0-1-buffer' contains a tuple delimiter, the function returns the sequence number
    /// representing the end of a spanning tuple, otherwise it returns 0 (the INVALID_SEQUENCE_NUMBER).
    std::pair<SequenceNumberType, bool> tryToFindHigherWrappingSpanningTuple(
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
        // Todo: rethink address calculation
        // example 1: (...10 (seen: ...01)
        // index: 1
        const auto indexOfClosestReachableTupleDelimiter = std::countr_one(onlySeenIsOne);
        const auto sequenceNumberOfClosestReachableTupleDelimiter
            = (sequenceNumberBitmapOffset + (bitmapIndexOffset << BITMAP_SIZE_BIT_SHIFT) + indexOfClosestReachableTupleDelimiter);
        const bool isTupleDelimiter = FIRST_BIT_MASK
            << (indexOfClosestReachableTupleDelimiter)&bitmapSnapshot.tupleDelimiterVectorSnapshot[bitmapIndex];
        const auto tailBitmapIndex = bitmapSnapshot.tail & bitmapSnapshot.numberOfBitmapsModulo;
        const auto isNotTailBitmap = bitmapIndex != tailBitmapIndex;
        return std::make_pair(sequenceNumberOfClosestReachableTupleDelimiter, (isTupleDelimiter and isNotTailBitmap));
    }

    /// Checks results in the absence of a tuple delimiter.
    /// If a buffer does not contain a tuple delimiter, it is only possible to find one spanning tuple.
    /// Both, the start and the end of the spanning tuple must be valid.
    StagedBufferResult checkResultsWithoutTupleDelimiter(
        SpanningTuple spanningTuple,
        const size_t sequenceNumberBufferPosition,
        const SequenceNumberType numberOfBitmapsModuloSnapshot)
    {
        /// Determine bitmap count, bitmap index and position in bitmap of start of spanning tuple
        const auto bitmapOfSpanningTupleStart = (spanningTuple.spanStart) >> BITMAP_SIZE_BIT_SHIFT;
        const auto bitmapIndexOfSpanningTupleStart = bitmapOfSpanningTupleStart & numberOfBitmapsModuloSnapshot;
        const auto positionOfSpanningTupleStart = ((spanningTuple.spanStart) & BITMAP_SIZE_MODULO);
        /// Check that both the start and the end of the spanning tuple are valid
        /// If both are valid, move a '1' to the bit position of the start of the spanning tuple
        const auto validatedSpanningTupleStartBit = FIRST_BIT_MASK << positionOfSpanningTupleStart;
        std::vector<StagedBuffer> returnBuffers{}; //Todo: initialize size of array?

        readWriteMutex
            .lock(); /// protects: read/write(tail,numberOfBitmaps,numberOfBitmapsModulo, seenAndUsedBitmaps), write(tupleDelimiterBitmaps, seenAndUsedBitmaps)
        /// Mark the spanning tuple as completed, by setting the start of the spanning tuple to 1 (if it is valid)
        seenAndUsedBitmaps[bitmapIndexOfSpanningTupleStart] |= validatedSpanningTupleStartBit;
        /// Check if the spanning tuple completed a bitmap (set the last bit in corresponding the seenAndUsed bitmap)
        const auto completedBitmap = (seenAndUsedBitmaps[bitmapIndexOfSpanningTupleStart] == MAX_VALUE);

        // Todo: remove calculation of exact size (in lock!)? <---there does not seem to be any dependency
        //Todo: if we can take snapshot, we can move it out of lock
        const auto stagedBufferSizeModulo = stagedBuffers.size() - 1;
        for (auto spanningTupleIndex = spanningTuple.spanStart; spanningTupleIndex <= spanningTuple.spanEnd; ++spanningTupleIndex)
        {
            const auto adjustedSpanningTupleIndex = spanningTupleIndex & stagedBufferSizeModulo;
            auto currentBuffer = stagedBuffers[adjustedSpanningTupleIndex];
            /// A buffer with a tuple delimiter has two uses. One for starting and one for ending a SpanningTuple.
            --currentBuffer.uses;
            auto returnBuffer = (currentBuffer.uses == 0) ? std::move(currentBuffer) : currentBuffer;
            returnBuffers.emplace_back(std::move(returnBuffer));
        }

        /// Check if the bitmap is the current tail-bitmap, if it is, the current thread needs to increment the tail
        if (completedBitmap and (bitmapOfSpanningTupleStart == this->tail))
        {
            incrementTail();
        }
        readWriteMutex.unlock();

        const size_t sequenceNumberIndex = sequenceNumberBufferPosition - spanningTuple.spanStart;
        return StagedBufferResult{.indexOfSequenceNumberInStagedBuffers = sequenceNumberIndex, .stagedBuffers = std::move(returnBuffers)};
    }

    /// Checks results in the presence of a tuple delimiter.
    /// If a buffer contains a tuple delimiter, it is possible to find two spanning tuples, one ending in the sequence number and one starting with it.
    StagedBufferResult checkResultsWithTupleDelimiter(
        SpanningTuple spanningTuple,
        const SequenceNumberType sequenceNumber,
        const size_t sequenceNumberBufferPosition,
        const SequenceNumberType numberOfBitmapsModuloSnapshot)
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

        readWriteMutex
            .lock(); /// protects: read/write(tail,numberOfBitmaps,numberOfBitmapsModulo, seenAndUsedBitmaps), write(tupleDelimiterBitmaps, seenAndUsedBitmaps)
        /// Mark the spanning tuple as completed, by setting the start of the spanning tuple to 1 (if it is valid)
        seenAndUsedBitmaps[bitmapIndexOfSpanningTupleStart] |= firstValidatedSpanningTupleStartBit;
        seenAndUsedBitmaps[bitmapIndexOfSpanningSequenceNumber] |= secondValidatedSpanningTupleStartBit;
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
        std::vector<StagedBuffer> returnBuffers{};
        const auto startIndex = (spanningTuple.isStartValid) ? spanningTuple.spanStart : sequenceNumber;
        const auto endIndex = (spanningTuple.isEndValid) ? spanningTuple.spanEnd : sequenceNumber;
        const auto returningMoreThanOnlyBufferOfSequenceNumber = startIndex < endIndex;

        //Todo: if we can take snapshot, we can move it out of lock
        const auto stagedBufferSizeModulo = stagedBuffers.size() - 1;
        for (auto spanningTupleIndex = startIndex; spanningTupleIndex <= endIndex; ++spanningTupleIndex) //Todo: since sequenceNumbers now 1-1 map to stagedBuffers, we need to include endIndex
        {
            const auto adjustedSpanningTupleIndex = spanningTupleIndex & stagedBufferSizeModulo;
            auto currentBuffer = stagedBuffers[adjustedSpanningTupleIndex];
            /// A buffer with a tuple delimiter has two uses. One for starting and one for ending a SpanningTuple.
            currentBuffer.uses -= static_cast<uint8_t>(returningMoreThanOnlyBufferOfSequenceNumber);
            auto returnBuffer = (currentBuffer.uses == 0) ? std::move(currentBuffer) : currentBuffer;
            returnBuffers.emplace_back(std::move(returnBuffer));
        }

        if (firstSpanningTupleCompletedTailBitmap or secondSpanningTupleCompletedTailBitmap)
        {
            incrementTail();
        }
        readWriteMutex.unlock();

        const size_t sequenceNumberIndex = sequenceNumberBufferPosition - startIndex;
        return StagedBufferResult{.indexOfSequenceNumberInStagedBuffers = sequenceNumberIndex, .stagedBuffers = std::move(returnBuffers)};
    }

    /// Utility for testing
    friend class SequenceShredderTest;
    struct BitmapConfig
    {
        uint64_t bitmapIndex;
        std::pair<std::string, std::string> bitmap;
    };
    SequenceShredder(const std::vector<BitmapConfig>& bitmapConfigs, const size_t initialTail, const size_t numberOfBitmaps)
        : tail(initialTail)
        , tupleDelimiterBitmaps(BitmapVectorType(numberOfBitmaps))
        , seenAndUsedBitmaps(BitmapVectorType(numberOfBitmaps))
        , numberOfBitmaps(numberOfBitmaps)
        , numberOfBitmapsModulo(numberOfBitmaps - 1)
        , resizeRequestCount(0)
        , stagedBuffers(std::vector<StagedBuffer>(numberOfBitmaps << BITMAP_SIZE_BIT_SHIFT))
    {
        this->tupleDelimiterBitmaps.shrink_to_fit();
        this->seenAndUsedBitmaps.shrink_to_fit();
        for (const auto& [bitmapIndex, bitmap] : bitmapConfigs)
        {
            this->tupleDelimiterBitmaps[bitmapIndex] = std::bitset<SIZE_OF_BITMAP_IN_BITS>(bitmap.second).to_ulong();
            this->seenAndUsedBitmaps[bitmapIndex] = std::bitset<SIZE_OF_BITMAP_IN_BITS>(bitmap.first).to_ulong();
        }
        this->tail = initialTail;
    };
};
}