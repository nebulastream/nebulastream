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
#include <ostream>
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

/// Acronyms:
/// ST <-> Spanning Tuple
/// STuple/sTuple <-> Spanning Tuple
/// SN <-> Sequence Number
/// ABA <-> ABA Problem (https: //en.wikipedia.org/wiki/ABA_problem)
/// abaItNo/ABAItNo <-> ABA Iteration Number

// Todo:
// - strong types for SN, ABA, other indexes
// - adapt README.md document

// Todo (optional):
// - resizing
// - verbose logging

/// Todo: documentation
/// - sits on to

/// Contains an empty 'spanningBuffers' vector, if the SequenceShredder could not claim any spanning tuples for the calling thread
/// Otherwise, 'spanningBuffers' contains the buffers of the 1-2 spanning tuples and 'indexOfInputBuffer' indicates which of the buffers
/// matches the buffer that the calling thread provided to search for spanning tuples
/// May return 'false' for 'isInRange', if the calling thread provides a sequence number that the spanning tuple buffer can't process yet
struct SequenceShredderResult
{
    bool isInRange;
    size_t indexOfInputBuffer;
    std::vector<StagedBuffer> spanningBuffers;
};

/// Forward referencing 'STBuffer' to hide implementation details
class STBuffer;

// Todo: documentation <-- think about the level of detail, probably defer to 'STBuffer' for technical details'
// - stay high level and focus on resizing aspect / dealing with out of range tuples

/// The SequenceShredder concurrently takes StagedBuffers and uses a (thread-safe) spanning tuple buffer (STBuffer) to determine whether
/// the provided buffer completes spanning tuples with buffers that (usually) other threads processed
/// (Planned) The SequenceShredder keeps track of sequence numbers that were not in range of the STBuffer
/// (Planned) Given enough out-of-range requests, the SequenceShredder doubles the size of the STBuffer
class SequenceShredder
{
    static constexpr size_t INITIAL_SIZE_OF_ST_BUFFER = 1024;

public:
    explicit SequenceShredder();
    /// Destructor validates (final) state of spanning tuple buffer
    ~SequenceShredder();

    SequenceShredder(const SequenceShredder&) = delete;
    SequenceShredder& operator=(const SequenceShredder&) = delete;
    SequenceShredder(SequenceShredder&&) = default;
    SequenceShredder& operator=(SequenceShredder&&) = default;

    /// Thread-safely checks if the buffer represented by the sequence number completes spanning tuples.
    /// Uses the STBuffer to determine whether the 'indexedRawBuffer' with the given 'sequenceNumber' completes spanning tuples and
    /// whether the calling thread is the first to claim the individual spanning tuples
    // Todo: replace with only 'indexedRawBuffer' as input and make current functions private and ConcurrentSynchronizationTest friend
    SequenceShredderResult findSTsWithDelimiter(const StagedBuffer& indexedRawBuffer, SequenceNumberType sequenceNumber);
    SequenceShredderResult findSTsWithoutDelimiter(const StagedBuffer& indexedRawBuffer, SequenceNumberType sequenceNumber);

    friend std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder);

private:
    std::unique_ptr<STBuffer> spanningTupleBuffer;
};

}

FMT_OSTREAM(NES::InputFormatters::SequenceShredder);
