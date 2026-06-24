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
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <Util/Logger/Formatter.hpp>
#include <UncompiledRawTupleBuffer.hpp>


class ConcurrentSynchronizationTest;

namespace NES
{

/// Acronyms:
/// ST <-> Spanning Tuple
/// SN <-> Sequence Number
/// ABA <-> ABA Problem (en.wikipedia.org/wiki/ABA_problem)
/// abaItNo/UncompiledABAItNo <-> ABA Iteration Number


/// Forward referencing 'UncompiledSTBuffer' to hide implementation details
class UncompiledSTBuffer;

/// Contains an empty 'spanningBuffers' vector, if the UncompiledSequenceShredder could not claim any spanning tuples for the calling thread
/// Otherwise, 'spanningBuffers' contains the buffers of the 1-2 spanning tuples and 'indexOfInputBuffer' indicates which of the buffers
/// matches the buffer that the calling thread provided to search for spanning tuples
/// May return 'false' for 'isInRange', if the calling thread provides a sequence number that the spanning tuple buffer can't process yet
class UncompiledSpanningBuffers
{
public:
    UncompiledSpanningBuffers() = default;

    explicit UncompiledSpanningBuffers(std::vector<UncompiledStagedBuffer> spanningBuffers)
        : spanningBuffers(std::move(spanningBuffers)) { }

    [[nodiscard]] size_t getSize() const { return this->spanningBuffers.size(); }

    [[nodiscard]] bool hasSpanningTuple() const { return spanningBuffers.size() > 1; }

    [[nodiscard]] const std::vector<UncompiledStagedBuffer>& getSpanningBuffers() const { return spanningBuffers; }

private:
    std::vector<UncompiledStagedBuffer> spanningBuffers;
};

struct UncompiledSequenceShredderResult
{
    bool isInRange = false;
    UncompiledSpanningBuffers spanningBuffers;
};

/// The UncompiledSequenceShredder concurrently takes StagedBuffers and uses a (thread-safe) spanning tuple buffer (UncompiledSTBuffer) to determine whether
/// the provided buffer completes spanning tuples with buffers that (usually) other threads processed
/// (Planned) The UncompiledSequenceShredder keeps track of sequence numbers that were not in range of the UncompiledSTBuffer
/// (Planned) Given enough out-of-range requests, the UncompiledSequenceShredder doubles the size of the UncompiledSTBuffer
class UncompiledSequenceShredder
{
    static constexpr size_t INITIAL_SIZE_OF_ST_BUFFER = 1024;

public:
    explicit UncompiledSequenceShredder(size_t sizeOfTupleDelimiterInBytes);
    /// Destructor validates (final) state of spanning tuple buffer
    ~UncompiledSequenceShredder();

    UncompiledSequenceShredder(const UncompiledSequenceShredder&) = delete;
    UncompiledSequenceShredder& operator=(const UncompiledSequenceShredder&) = delete;
    UncompiledSequenceShredder(UncompiledSequenceShredder&&) = default;
    UncompiledSequenceShredder& operator=(UncompiledSequenceShredder&&) = default;

    /// Uses the UncompiledSTBuffer to thread-safely determine whether the 'indexedRawBuffer' with the given 'sequenceNumber'
    /// completes spanning tuples and whether the calling thread is the first to claim the individual spanning tuples
    UncompiledSequenceShredderResult findLeadingSTWithDelimiter(const UncompiledStagedBuffer& indexedRawBuffer);
    UncompiledSpanningBuffers findTrailingSTWithDelimiter(SequenceNumber sequenceNumber);
    UncompiledSequenceShredderResult findSTWithoutDelimiter(const UncompiledStagedBuffer& indexedRawBuffer);

    friend std::ostream& operator<<(std::ostream& os, const UncompiledSequenceShredder& sequenceShredder);

private:
    std::unique_ptr<UncompiledSTBuffer> spanningTupleBuffer;

    /// Enable 'ConcurrentSynchronizationTest' to used mocked buffer and provide 'sequenceNumber' as additional argument
    friend ConcurrentSynchronizationTest;
    UncompiledSequenceShredderResult
    findLeadingSTWithDelimiter(const UncompiledStagedBuffer& indexedRawBuffer, SequenceNumber sequenceNumber);
    UncompiledSequenceShredderResult findSTWithoutDelimiter(const UncompiledStagedBuffer& indexedRawBuffer, SequenceNumber sequenceNumber);
};

}

FMT_OSTREAM(NES::UncompiledSequenceShredder);
