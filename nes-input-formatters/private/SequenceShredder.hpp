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
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES::InputFormatters
{

/*
     The SequenceShredder is a thread-safe data structure that assigns responsibilities to threads that
     concurrently process input formatter tasks (buffers with raw data) originating from the same source.

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
     If a thread sees a tuple delimiter (T1(14ms)), it knows that it terminates a tuple, but it does not necessarily know where it starts.
     If a thread sees no tuple delimiter in its buffer, it does neither know where the tuple that the buffer belongs to starts, nor where it ends.
     Thus, we need to synchronize between threads. At least one thread must see the start and the end of the spanning tuple.
     Exactly one thread must format the tuple and pass it to the next operator to avoid duplication.

     ---- The Solution
     Given that NebulaStream must perform the above synchronization for every single source and that a fundamental
     design goal of NebulaStream is to support as many sources as possible on a single node, we opt for a design that:
     I) has a small memory footprint, II) can handle arbitrary many sequence numbers, III) does not bottleneck the entire query due to synchronization
     I: Fundamentally, a buffer may either contain a tuple delimiter or not. A spanning tuple is a tuple that starts in a
        buffer with a tuple delimiter, spans over an arbitrary number of buffers without a tuple delimiter and ends in buffer
        with a tuple delimiter. Thus, it is sufficient to remember for each buffer, whether it has a tuple delimiter and
        whether we saw and used it already. We can achieve this using two bitmaps. One denotes whether a tuple buffer contains a
        delimiter with a '1', the other denotes whether we saw a buffer and made use of it with a '1'. If a
        buffer does not contain a tuple delimiter, we immediately set a '1' in this 'seenAndUsed' bitmap. If a buffer contains a tuple
        delimiter we set a '1' only when we complete the tuple that starts in the buffer.
             1000001 <--- (15ms) tupleDelimiter bitmap for the example above
             0111100 <--- (15ms) seenAndUsed bitmap for the example above (buffer 6 is still missing)
             1000001 <--- (16ms) tuple delimiters for example above
             1111110 <--- (16ms) T3 processes buffer 6, it sees that it can reach buffer 1 and 7, which both have tuple delimiters)
        A buffer can reach a tuple delimiter (both directions), if all buffers between the buffer and the buffer containing the tuple delimiter
        were seen and used already, but do not contain a tuple delimiter. That is, there is a sequence of 0s in the tuple
        delimiter bitmap, exactly contrasted by a sequence of 1s in the corresponding seen and used bitmap.
        This representation allows us to find all spanning tuples using just 2 bits per buffer. Two bitmaps of size 64 bit
        represent 64 buffers. The default size of buffers in NebulaStream is the page size (4096 bytes). A single pair of bitmaps
        (128 bit) can therefore represent 64 * 4096 bytes = 262144 bytes worth of buffer data, meaning the bitmaps are cache friendly.
        The SequenceShredder starts with 8 pairs of bitmaps. That is, 8 * 128 = 1024 bits. These 1024 bits represent 512 buffers,
        which means 512 * 4096KB = 2097152KB ~= 2GB. Representing 1 million sources (with 8 bitmaps each), takes 1.000.000 * 1024 bits ~= 500KB.
     II: To address the concern of arbitrarily increasing sequence numbers, the SequenceShredder implements a dynamically increasing ring buffer.
         Each slot in the ring buffer represents two bitmaps. If a thread requests to process a sequence number that is not
         in the range of the ring buffer, the thread puts its task back into the queue (rarely the case). The SequenceShredder keeps track of
         these events and increases the size of the ring buffer if an 'out of range' sequence number occurred often enough.
         The SequenceShredder can also handle sequence numbers that wrap back to the initial value of 1.
     III: The SequenceShredder needs to synchronize between concurrently executing threads. Every buffer, produced by the
          source that the SequenceShredder belongs to, must pass through the sequence shredder. The SequenceShredder
          may slow down an entire query, if synchronization takes too long. Additionally, the design should be robust, since
          it affects (almost) every single value processed by NebulaStream.
          We use 3 locks for synchronization, aiming to minimize the number of instructions in each protected section.
          The first lock is to safely read (avoiding torn-reads) the current tail and determine whether the sequence number
          is in range.
          The second lock is the first lock in 'processSequenceNumber()'. In its protected section, the thread first
          denotes in the bitmaps, whether it found a tuple delimiter in its buffer or not. Second, it determines
          if it is sufficient to just look at the bitmap that its sequence number maps to, or whether it may find a spanning tuple
          that spans to another bitmap (back or forward). It then either takes a snapshot(copy) of only its own bitmap, or of all bitmaps (copying
          all bitmaps is the uncommon case, but still fast). This snapshot has two major advantages. First, all bits set
          in the bitmap were set by threads that arrived in the lock-protected section earlier
          and that consequently did not see the bit just set by the thread.
             T1   T2        T1   T2
             X    |    or   |    X   <--- T1/T2 is first, sets its bit, and does not see the bit set by the other thread
             |    X         X    |   <--- T2/T1 then comes second, sets its bit, and sees the bit of the other thread
          Thus, if a thread sees that it just added the last bit which completes a spanning tuple, it knows that no other
          thread that came before saw this last bit (bits correspond to sequence numbers and sequence numbers are unique) and since all
          sequence numbers belonging to that spanning tuple were seen already, it knows that no later thread will find the same spanning tuple.
          Thus, the thread knows that it is the only thread that was able to find the spanning tuple and can therefore safely process it.
          The second major advantage is that the thread can search for spanning tuples using its snapshot, without the need for locking.
          When a thread finds a spanning tuple, it requires a third lock. It sets a bit in the seen and used bitmap to
          tell the other threads that the tuple delimiter that started the bitmap was now used. It then determines whether
          it just completed the tail bitmap and therefore has to move the tail. A thread completes a bitmap if it flips
          the last non-'1' bit in the seen and used bitmap to '1'. It means that no more spanning tuple can start in this bitmap.
          If the bitmap happens to be the tail bitmap, it means that no prior bitmap can still start a spanning tuple, which
          means that no more spanning tuples can end in the bitmap. It is safe to reset it and move the tail.
          If a thread detects that it completed the tail bitmap, it keeps the lock and increases the tail, until it finds
          a bitmap that is not yet complete (not all bits in the seen and used bitmap are '1's). If enough threads requested an increase
          of the ring buffer, and the thread can safely increase the capacity of the ring buffer (see incrementTail function),
          the thread doubles the size of the ring buffer.
*/

class SequenceShredder
{
public:
    using BitmapType = uint64_t;
    using SequenceNumberType = SequenceNumber::Underlying;
    static constexpr size_t SIZE_OF_BITMAP_IN_BITS = sizeof(BitmapType) * 8; /// 8 bits in one byte
    static constexpr size_t INITIAL_NUM_BITMAPS = 64;
    using BitmapVectorType = std::vector<BitmapType>;

    struct StagedBuffer
    {
        Memory::TupleBuffer buffer;
        size_t sizeOfBufferInBytes;
        uint32_t offsetOfFirstTupleDelimiter;
        uint32_t offsetOfLastTupleDelimiter;
    };

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

    /// The SequenceShredder's main job is to find spanning tuples (during concurrent processing of raw input buffers).
    /// A spanning tuple is a tuple that spans over at least two raw input buffers.
    /// Given an input buffer without a delimiter, the SequenceShredder may find one spanning tuple at most (the raw input buffer connects
    /// two raw input buffers with tuple delimiters).
    /// Given an input buffer with a tuple delimiter, the SequenceShredder may find up to two spanning tuples. A first, starting in a raw input
    /// buffer with a lower sequence number, ending in the raw input buffer. A second, starting in the raw input buffer, ending in a raw
    /// input buffer with a higher sequence number.
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

    /// Return
    [[nodiscard]] size_t getTail() const { return this->tail; }

    /// Check if the sequence number is in the allowed range of the ring buffer.
    /// Example: 4 bitmaps, tail is 4, then the allowed range for sequence numbers is 256-511: [256,319][320,383][384,447][448,511]
    [[nodiscard]] bool isInRange(SequenceNumberType sequenceNumber);

    /// Since EoF/EoS is not a symbol that allows the parser to tell that the final tuple just ended, we require a function
    /// that inserts an artificial tuple delimiter that completes the last tuple in the final buffer and flushes it.
    /// The artificial tuple is a buffer with a sequence number (SN) that is exactly one larger than the largest seen SN.
    /// We configure the buffer to 'contain' a tuple delimiter as its first and only content.
    [[nodiscard]] std::pair<SpanningTupleBuffers, SequenceNumberType> flushFinalPartialTuple();

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
    bool isLastTuple = false;

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
