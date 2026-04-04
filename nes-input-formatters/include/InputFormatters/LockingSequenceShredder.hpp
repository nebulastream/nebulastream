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

#include <cstddef>
#include <memory>
#include <mutex>
#include <ostream>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>


class ConcurrentSynchronizationTest;

namespace NES
{

class LockingSpanningTupleBuffer;

/// Mutex-based alternative to SequenceShredder. Has the same interface, but uses a central lock instead of lock-free
/// synchronization. Every public method acquires the mutex once before delegating to the LockingSpanningTupleBuffer,
/// ensuring that only one thread at a time traverses the buffer.
/// The underlying LockingSpanningTupleBuffer uses PlainState (non-atomic uint64_t) instead of AtomicState.
class LockingSequenceShredder
{
    static constexpr size_t INITIAL_SIZE_OF_SPANNING_TUPLE_BUFFER = 1024;

public:
    explicit LockingSequenceShredder(size_t sizeOfTupleDelimiterInBytes);
    ~LockingSequenceShredder();

    LockingSequenceShredder(const LockingSequenceShredder&) = delete;
    LockingSequenceShredder& operator=(const LockingSequenceShredder&) = delete;
    LockingSequenceShredder(LockingSequenceShredder&&) = delete;
    LockingSequenceShredder& operator=(LockingSequenceShredder&&) = delete;

    SequenceShredderResult findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer);
    SequenceShredderResult findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer);

    SpanningBuffers findTrailingSpanningTupleWithDelimiter(SequenceNumber sequenceNumber);
    SpanningBuffers findTrailingSpanningTupleWithDelimiter(SequenceNumber sequenceNumber, FieldIndex offsetOfLastTuple);

    friend std::ostream& operator<<(std::ostream& os, const LockingSequenceShredder& sequenceShredder);

private:
    std::unique_ptr<LockingSpanningTupleBuffer> spanningTupleBuffer;
    mutable std::mutex mutex;

    friend ConcurrentSynchronizationTest;
    SequenceShredderResult findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer, SequenceNumber sequenceNumber);
    SequenceShredderResult findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer, SequenceNumber sequenceNumber);
};

}

FMT_OSTREAM(NES::LockingSequenceShredder);
