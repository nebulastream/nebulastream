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
#include <ostream>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>


class ConcurrentSynchronizationTest;

namespace NES
{

/// Forward referencing 'FragmentSpanningTupleBuffer' to hide implementation details
class FragmentSpanningTupleBuffer;

/// Fragment-copy alternative to the (lock-free) SequenceShredder. Same interface, same lock-free
/// synchronization protocol, but the ring entries COPY the boundary bytes of each buffer (the head before the
/// first delimiter, the tail after the last delimiter) into owned strings instead of pinning the TupleBuffer
/// until the neighbouring buffers arrive. This removes the shredder's per-source frontier hold (~1 pinned
/// buffer per source awaiting its successor), which is what couples the required buffer-pool size to the
/// source fan-in (the N=512 many-source wedge; liveness law pool >= 2 x sources_per_cell).
/// Buffers stay pinned only for: interior no-delimiter buffers (whole content needed), the lazy-offset path,
/// and boundary fragments larger than MAX_FRAGMENT_BYTES.
/// Because entries complete independently of successor arrival, the ring can stay small: the sequence-number
/// spread per source is bounded by the source's in-flight buffer count (<= its pool), not by frontier holds.
class FragmentSequenceShredder
{
    static constexpr size_t INITIAL_SIZE_OF_SPANNING_TUPLE_BUFFER = 4096;
    /// Boundary fragments larger than this fall back to pinning the buffer (unbounded VARSIZED rows);
    /// generously above typical row sizes (~100 B), well below the 128 KiB default buffer size
    static constexpr size_t MAX_FRAGMENT_BYTES = 8192;

public:
    explicit FragmentSequenceShredder(size_t sizeOfTupleDelimiterInBytes);
    /// Destructor validates (final) state of spanning tuple buffer
    ~FragmentSequenceShredder();

    FragmentSequenceShredder(const FragmentSequenceShredder&) = delete;
    FragmentSequenceShredder& operator=(const FragmentSequenceShredder&) = delete;
    FragmentSequenceShredder(FragmentSequenceShredder&&) = default;
    FragmentSequenceShredder& operator=(FragmentSequenceShredder&&) = default;

    /// Uses the FragmentSpanningTupleBuffer to thread-safely determine whether the 'indexedRawBuffer' with the given 'sequenceNumber'
    /// completes spanning tuples and whether the calling thread is the first to claim the individual spanning tuples
    SequenceShredderResult findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer);
    SequenceShredderResult findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer);

    /// Assumes findLeadingSpanningTupleWithDelimiter was already called and the StagedBuffer for 'sequenceNumber' already set
    /// Searches for a reachable buffer that delimits tuples in trailing direction (higher SequenceNumbers)
    SpanningBuffers findTrailingSpanningTupleWithDelimiter(SequenceNumber sequenceNumber);
    /// Overload that allows to lazily set the offset of the last record starting in the StagedBuffer (and the atomic state)
    /// if the offset was not already known when 'findLeadingSpanningTupleWithDelimiter' was called
    SpanningBuffers findTrailingSpanningTupleWithDelimiter(SequenceNumber sequenceNumber, FieldIndex offsetOfLastTuple);

    friend std::ostream& operator<<(std::ostream& os, const FragmentSequenceShredder& sequenceShredder);

private:
    std::unique_ptr<FragmentSpanningTupleBuffer> spanningTupleBuffer;

    /// Enable 'ConcurrentSynchronizationTest' to used mocked buffer and provide 'sequenceNumber' as additional argument
    friend ConcurrentSynchronizationTest;
    SequenceShredderResult findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer, SequenceNumber sequenceNumber);
    SequenceShredderResult findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer, SequenceNumber sequenceNumber);
};

}

FMT_OSTREAM(NES::FragmentSequenceShredder);
