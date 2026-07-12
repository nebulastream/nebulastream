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
#include <optional>
#include <ostream>
#include <span>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <FragmentSpanningTupleBufferState.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>

namespace NES
{

/// Fragment-copy fork of the SpanningTupleBuffer (see SpanningTupleBuffer.hpp for the full protocol
/// documentation — the traversal, ABA and CAS-claim logic here is identical). The only difference is the
/// entry type: FragmentSpanningTupleBufferEntry copies the boundary bytes of a buffer instead of pinning
/// the buffer itself, so entries need the tuple-delimiter size and a fragment size cap at set time.
class FragmentSpanningTupleBuffer
{
    /// Result of trying to claim a buffer (with a specific SN) as the start of a spanning tuple.
    /// Multiple threads may try to claim a specific buffer. Only the thread that wins the race gets 'firstBuffer' with a value.
    struct ClaimedSpanningTuple
    {
        std::optional<StagedBuffer> firstBuffer;
        SequenceNumber snOfLastBuffer = INVALID<SequenceNumber>;
    };

public:
    struct WithoutDelimiterSearchResult
    {
        std::optional<StagedBuffer> leadingSpanningTupleStartBuffer = std::nullopt;
        SequenceNumber firstSequenceNumber = INVALID<SequenceNumber>;
        SequenceNumber lastSequenceNumber = INVALID<SequenceNumber>;
    };

    explicit FragmentSpanningTupleBuffer(size_t initialSize, size_t delimiterSize, size_t maxFragmentBytes);

    /// First, checks if the prior entry at the index of 'sequenceNumber' contains the expected prior ABA iteration number and is used up
    /// If not, returns as 'NOT_IN_RANGE'
    /// Otherwise, searches for valid spanning tuples and on finding a spanning tuple, tries to claim the first buffer of the spanning tuple
    /// for the calling thread, which claims the entire spanning tuple.
    [[nodiscard]] SequenceShredderResult
    tryFindLeadingSpanningTupleForBufferWithDelimiter(SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer);
    [[nodiscard]] SpanningBuffers tryFindTrailingSpanningTupleForBufferWithDelimiter(SequenceNumber sequenceNumber);
    [[nodiscard]] SpanningBuffers
    tryFindTrailingSpanningTupleForBufferWithDelimiter(SequenceNumber sequenceNumber, FieldIndex offsetOfLastTuple);

    /// Tries to find a reachable buffer with a delimiter in leading direction.
    /// If successful, starts a claiming trailing delimiter search with that reachable buffer as the start (search starts at 'sequenceNumber')
    [[nodiscard]] SequenceShredderResult
    tryFindSpanningTupleForBufferWithoutDelimiter(SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer);

    [[nodiscard]] bool validate() const;

    friend std::ostream& operator<<(std::ostream& os, const FragmentSpanningTupleBuffer& sequenceRingBuffer);

private:
    std::vector<FragmentSpanningTupleBufferEntry> buffer;
    size_t delimiterSize;
    size_t maxFragmentBytes;

    [[nodiscard]] std::pair<SpanningTupleBufferIdx, ABAItNo> getBufferIdxAndABAItNo(SequenceNumber sequenceNumber) const;

    /// Searches for two buffers with delimiters that are connected by the buffer with the provided 'sequenceNumber'
    /// Tries to claim the (first buffer of the) leading spanning tuple, if search is successful
    [[nodiscard]] WithoutDelimiterSearchResult searchAndTryClaimWithoutDelimiter(SequenceNumber sequenceNumber);

    /// Searches for a reachable buffer that can start a spanning tuple (in leading direction - smaller SNs)
    /// 'Reachable' means that there is a path from 'searchStartBufferIdx' to a buffer that traverses 0 or more buffers without delimiters
    /// and valid ABA iteration numbers and that the final buffer has a delimiter and a valid ABA iteration number
    /// Returns distance to 'reachable buffer', if it succeeds in finding one
    [[nodiscard]] std::optional<size_t> searchLeading(SpanningTupleBufferIdx searchStartBufferIdx, ABAItNo abaItNumber) const;
    /// Analog to 'searchLeading', but searches in trailing direction (larger SNs)
    [[nodiscard]] std::optional<size_t> searchTrailing(SpanningTupleBufferIdx searchStartBufferIdx, ABAItNo abaItNumber) const;

    /// Assumes spanningTupleEndSN as the end of a potential spanning tuple.
    /// Searches in leading direction (smaller SNs) for a buffer with a delimiter that can start the spanning tuple.
    /// Aborts as soon as it finds a non-connecting ABA iteration number (different abaItNo and not first/last element of ring buffer).
    /// On finding a valid starting buffer, threads compete to claim that buffer (and thereby all buffers of the SpanningTuple).
    /// Only one thread can succeed in claiming the first buffer (ClaimedSpanningTuple::firstBuffer != nullopt).
    [[nodiscard]] ClaimedSpanningTuple claimingLeadingDelimiterSearch(SequenceNumber stEndSN);
    /// Analog to 'claimingLeadingDelimiterSearch', but traverses in trailing direction (larger SNs)
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumber stStartSN, SequenceNumber searchStartSN);

    /// Calls claimingTrailingDelimiterSearch(spanningTupleStartSN, spanningTupleStartSN) <-- SpanningTuple start is also where search should start
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumber stStartSN);

    /// Claims all trailing buffers of an SpanningTuple (all buffers except the first, which the thread must have claimed already to claim the rest)
    /// Assumes size of 'spanningTupleBuffers' as the size of the SpanningTuple, assigning using an index, instead of emplacing to the back
    void claimSpanningTupleBuffers(SequenceNumber stStartSn, std::span<StagedBuffer> spanningTupleBuffers);
};
}

FMT_OSTREAM(NES::FragmentSpanningTupleBuffer);
