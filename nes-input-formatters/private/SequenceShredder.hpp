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
#include <cstdint>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>


class ConcurrentSynchronizationTest;

namespace NES
{

/// Acronyms:
/// ST <-> Spanning Tuple
/// SN <-> Sequence Number
/// ABA <-> ABA Problem (en.wikipedia.org/wiki/ABA_problem)
/// abaItNo/ABAItNo <-> ABA Iteration Number


/// Forward referencing 'STBuffer' to hide implementation details
class STBuffer;

/// Contains an empty 'spanningBuffers' vector, if the SequenceShredder could not claim any spanning tuples for the calling thread
/// Otherwise, 'spanningBuffers' contains the buffers of the 1-2 spanning tuples and 'indexOfInputBuffer' indicates which of the buffers
/// matches the buffer that the calling thread provided to search for spanning tuples
/// May return 'false' for 'isInRange', if the calling thread provides a sequence number that the spanning tuple buffer can't process yet
class SpanningBuffers
{
public:
    SpanningBuffers() = default;

    explicit SpanningBuffers(std::vector<StagedBuffer> spanningBuffers) : spanningBuffers(std::move(spanningBuffers)) { }

    [[nodiscard]] size_t getSize() const { return this->spanningBuffers.size(); }

    [[nodiscard]] bool hasSpanningTuple() const { return spanningBuffers.size() > 1; }

    [[nodiscard]] const std::vector<StagedBuffer>& getSpanningBuffers() const { return spanningBuffers; }

private:
    std::vector<StagedBuffer> spanningBuffers;
};

struct SequenceShredderResult
{
    bool isInRange = false;
    SpanningBuffers spanningBuffers;
};

/// The SequenceShredder concurrently takes StagedBuffers and uses a (thread-safe) spanning tuple buffer (STBuffer) to determine whether
/// the provided buffer completes spanning tuples with buffers that (usually) other threads processed
/// (Planned) The SequenceShredder keeps track of sequence numbers that were not in range of the STBuffer
/// (Planned) Given enough out-of-range requests, the SequenceShredder doubles the size of the STBuffer
class SequenceShredder
{
    static constexpr size_t INITIAL_SIZE_OF_ST_BUFFER = 1024;

public:
    explicit SequenceShredder(size_t sizeOfTupleDelimiterInBytes);
    /// Destructor validates (final) state of spanning tuple buffer
    ~SequenceShredder();

    SequenceShredder(const SequenceShredder&) = delete;
    SequenceShredder& operator=(const SequenceShredder&) = delete;
    SequenceShredder(SequenceShredder&&) = default;
    SequenceShredder& operator=(SequenceShredder&&) = default;

    /// Uses the STBuffer to thread-safely determine whether the 'indexedRawBuffer' with the given 'sequenceNumber'
    /// completes spanning tuples and whether the calling thread is the first to claim the individual spanning tuples
    SequenceShredderResult findLeadingSTWithDelimiter(const StagedBuffer& indexedRawBuffer);
    SequenceShredderResult findSTWithoutDelimiter(const StagedBuffer& indexedRawBuffer);

    /// Assumes findLeadingSTWithDelimiter was already called and the StagedBuffer for 'sequenceNumber' already set
    /// Searches for a reachable buffer that delimits tuples in trailing direction (higher SequenceNumbers)
    SpanningBuffers findTrailingSTWithDelimiter(SequenceNumber sequenceNumber);
    /// Overload that allows to lazily set the offset of the last record starting in the StagedBuffer (and the atomic state)
    /// if the offset was not already known when 'findLeadingSTWithDelimiter' was called
    SpanningBuffers findTrailingSTWithDelimiter(SequenceNumber sequenceNumber, FieldIndex offsetOfLastTuple);

    friend std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder);

private:
    std::unique_ptr<STBuffer> spanningTupleBuffer;

    /// Enable 'ConcurrentSynchronizationTest' to used mocked buffer and provide 'sequenceNumber' as additional argument
    friend ConcurrentSynchronizationTest;
    SequenceShredderResult findLeadingSTWithDelimiter(const StagedBuffer& indexedRawBuffer, SequenceNumber sequenceNumber);
    SequenceShredderResult findSTWithoutDelimiter(const StagedBuffer& indexedRawBuffer, SequenceNumber sequenceNumber);
};

}

FMT_OSTREAM(NES::SequenceShredder);
