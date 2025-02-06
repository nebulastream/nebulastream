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

#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
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
    // Todo: get rid of sequence number type?
    using SequenceNumberType = uint64_t;
    static constexpr size_t SIZE_OF_BITMAP_IN_BITS = sizeof(SequenceNumberType) * 8; /// 8 bits in one byte
    static constexpr size_t INITIAL_NUM_BITMAPS = 4;
    using BitmapVectorType = std::vector<SequenceNumberType>;

    struct StagedBuffer
    {
        Memory::TupleBuffer buffer;
        size_t sizeOfBufferInBytes;
        uint32_t offsetOfFirstTupleDelimiter;
        uint32_t offsetOfLastTupleDelimiter;
        int uses = 1;
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
    explicit SequenceShredder(size_t sizeOfTupleDelimiter);
    explicit SequenceShredder(size_t sizeOfTupleDelimiter, size_t initialNumBitmaps);

    [[nodiscard]] size_t getTail() const { return this->tail; }

    /// Check if the sequence number is in the allowed range of the ring buffer.
    /// Example: 4 bitmaps, tail is 0, then the allowed range for sequence numbers is 1-256: [1,64][65,128][129,192][193,256]
    [[nodiscard]] bool isSequenceNumberInRange(SequenceNumberType sequenceNumber);

    /// Since EoF/EoS is not a symbol that allows the parser to tell that the final tuple just ended, we require a function
    /// that inserts an artificial tuple delimiter that completes the last tuple in the final buffer and flushes it.
    /// The artificial tuple is a buffer with a sequence number (SN) that is exactly one larger than the largest seen SN.
    /// We configure the buffer to 'contain' a tuple delimiter as its first and only content.
    [[nodiscard]] StagedBufferResult flushFinalPartialTuple();

    /// Thread-safely checks if the buffer represented by the sequence number completes spanning tuples.
    /// Returns a vector with either zero, one or two spanning tuples if the sequence number is in range.
    /// Returns a vector with three empty spanning tuples if the sequence number is out of range.
    /// For details on the inner workings of this function, read the description of the class above.
    template <bool HasTupleDelimiter>
    StagedBufferResult
    processSequenceNumber(StagedBuffer stagedBufferOfSequenceNumber, SequenceNumberType sequenceNumber);

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
    void incrementTail();

    /// Determines whether the buffer of the sequence number connects to buffer with tuple delimiter, which starts a spanning tuple that spans
    /// into the buffer of the sequence number.
    std::pair<SequenceNumberType, bool> tryGetSpanningTupleStart(
        SequenceNumberType sequenceNumberBitIndex,
        SequenceNumberType sequenceNumberBitmapOffset,
        const SequenceNumberType& tupleDelimiterBitmap,
        const SequenceNumberType& seenAndUsedBitmap);

    /// Determines whether the buffer of the sequence number connects to buffer with tuple delimiter, which ends a spanning tuple that originates
    /// from the buffer of the sequence number, if it has a tuple delimiter, or in an earlier buffer.
    std::pair<SequenceNumberType, bool> tryGetSpanningTupleEnd(
        SequenceNumberType sequenceNumberBitIndex,
        SequenceNumberType sequenceNumberBitmapOffset,
        const SequenceNumberType& tupleDelimiterBitmap,
        const SequenceNumberType& seenAndUsedBitmap);

    /// Called by a thread that needs to search for the start of a spanning tuple in a lower bitmap than the one that its sequence number maps to.
    /// Skips over buffers that were seen, but did not contain a tuple delimiter, represented by a '0' in the tuple delimiter bitmap and
    /// a '1' in the seen and used bitmap. If the first 'non-0-1-buffer' contains a tuple delimiter, the function returns the sequence number
    /// representing the start of a spanning tuple, otherwise it returns 0 (the INVALID_SEQUENCE_NUMBER).
    std::pair<SequenceNumberType, bool> tryToFindLowerWrappingSpanningTuple(
        size_t sequenceNumberBitmapOffset, size_t currentBitmapIndex, const BitmapVectorSnapshot& bitmapSnapshot);

    /// Called by a thread that needs to search for the end of a spanning tuple in a higher bitmap than the one that its sequence number maps to.
    /// Skips over buffers that were seen, but did not contain a tuple delimiter, represented by a '1' in the tuple delimiter bitmap and a '0'
    /// seen and used bitmap. If the first 'non-0-1-buffer' contains a tuple delimiter, the function returns the sequence number
    /// representing the end of a spanning tuple, otherwise it returns 0 (the INVALID_SEQUENCE_NUMBER).
    std::pair<SequenceNumberType, bool> tryToFindHigherWrappingSpanningTuple(
        size_t sequenceNumberBitmapOffset, size_t currentBitmapIndex, const BitmapVectorSnapshot& bitmapSnapshot);

    /// Checks results in the absence of a tuple delimiter.
    /// If a buffer does not contain a tuple delimiter, it is only possible to find one spanning tuple.
    /// Both, the start and the end of the spanning tuple must be valid.
    StagedBufferResult checkResultsWithoutTupleDelimiter(
        SpanningTuple spanningTuple, SequenceNumberType sequenceNumber, SequenceNumberType numberOfBitmapsModuloSnapshot);

    /// Checks results in the presence of a tuple delimiter.
    /// If a buffer contains a tuple delimiter, it is possible to find two spanning tuples, one ending in the sequence number and one starting with it.
    StagedBufferResult checkResultsWithTupleDelimiter(
        SpanningTuple spanningTuple, SequenceNumberType sequenceNumber, SequenceNumberType numberOfBitmapsModuloSnapshot);
};
}