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

#include <cmath>
#include <cstddef>
#include <optional>
#include <ostream>
#include <span>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>
#include <STBufferState.hpp>
#include <SequenceShredder.hpp>

namespace NES
{

/// The STBuffer enables threads to concurrently resolve spanning tuples.
/// Spanning tuples (STs) are tuples that span over two or more buffers.
/// The buffers that form the ST may be processed by multiple threads concurrently.
/// The STBuffer makes sure that exactly one thread processes an ST (avoiding missing an ST or producing duplicate STs)
///
/// The central abstraction that allows the STBuffer to thread-safely determine STs is the AtomicState attached to each of its entries.
/// The atomic state can thread-safely denote whether an STBufferEntry has a delimiter, whether its buffers are used already, and
/// very importantly, which iteration the entry belongs to.
/// The STBuffer derives the iteration by dividing the sequence number (SN) of an incoming buffer by the size of the STBuffer and adding 1
/// e.g., for a buffer size of 1024 entries and an SN of 2025: 2025 / 1024 + 1 = 2 (see 'getBufferIdxAndABAItNo' of STBuffer)
/// Two SNs are connected, if they  have a distance of 1 and either share the same iteration, or are the first/last entries of the buffer,
/// with the first entry being in the iteration that immediately follows the iteration of the last entry
/// Two buffers that both have delimiters and that are connected or separated by a connected series of buffers without delimiters form an ST
/// The STBuffer takes a buffer and then iterates over its entries, using their atomic states to determine valid STs
/// The STBuffer guarantees that at least one thread determines a valid ST, because an entry can only be replaced with a new entry, if both
/// of its uses (part of atomic state) have been claimed. The uses are only claimed, if a thread found an ST that uses the respective
/// entries.
/// The STBuffer guarantees that only one thread can claim an ST, because (multiple) threads that found the same ST can only claim the ST
/// by using a CAS-loop to atomically set the 'claimedSpanningTupleBit' of the STBufferEntry that represents the first buffer of the ST to 1.
/// The CAS-loop requires that the 'claimedSpanningTupleBit' is '0' and a matching abaItNo.
/// Thus, only the thread that wins the race to flip the 'claimedSpanningTupleBit' to 1 can claim the ST.
/// After a thread claimed an ST, it knows it is the only thread that claimed the ST and can safely claim the uses of all other buffers that
/// are part of the ST.
class STBuffer
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
        std::optional<StagedBuffer> leadingSTStartBuffer = std::nullopt;
        SequenceNumber firstSequenceNumber = INVALID<SequenceNumber>;
        SequenceNumber lastSequenceNumber = INVALID<SequenceNumber>;
    };

    explicit STBuffer(size_t initialSize, TupleBuffer dummyBuffer);

    /// First, checks if the prior entry at the index of 'sequenceNumber' contains the expected prior ABA iteration number and is used up
    /// If not, returns as 'NOT_IN_RANGE'
    /// Otherwise, searches for valid spanning tuples and on finding a spanning tuple, tries to claim the first buffer of the spanning tuple
    /// for the calling thread, which claims the entire spanning tuple.
    [[nodiscard]] SequenceShredderResult
    tryFindLeadingSTForBufferWithDelimiter(SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer);
    [[nodiscard]] SpanningBuffers tryFindTrailingSTForBufferWithDelimiter(SequenceNumber sequenceNumber);
    [[nodiscard]] SpanningBuffers tryFindTrailingSTForBufferWithDelimiter(SequenceNumber sequenceNumber, FieldIndex offsetOfLastTuple);

    /// Tries to find a reachable buffer with a delimiter in leading direction.
    /// If successful, starts a claiming trailing delimiter search with that reachable buffer as the start (search starts at 'sequenceNumber')
    [[nodiscard]] SequenceShredderResult
    tryFindSTForBufferWithoutDelimiter(SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer);

    [[nodiscard]] bool validate() const;

    friend std::ostream& operator<<(std::ostream& os, const STBuffer& sequenceRingBuffer);

private:
    std::vector<STBufferEntry> buffer;

    [[nodiscard]] std::pair<STBufferIdx, ABAItNo> getBufferIdxAndABAItNo(SequenceNumber sequenceNumber) const;

    /// Searches for two buffers with delimiters that are connected by the buffer with the provided 'sequenceNumber'
    /// Tries to claim the (first buffer of the) leading spanning tuple, if search is successful
    [[nodiscard]] WithoutDelimiterSearchResult searchAndTryClaimWithoutDelimiter(SequenceNumber sequenceNumber);

    /// Searches for spanning tuples both in leading and trailing direction of the 'sequenceNumber'
    /// Tries to claim the (first buffers of the) spanning tuples (and thereby the STs) if it finds valid STs.
    [[nodiscard]] WithoutDelimiterSearchResult searchAndTryClaimLeadingAndTrailingST(SequenceNumber sequenceNumber);

    /// Searches for a reachable buffer that can start a spanning tuple (in leading direction - smaller SNs)
    /// 'Reachable' means that there is a path from 'searchStartBufferIdx' to a buffer that traverses 0 or more buffers without delimiters
    /// and valid ABA iteration numbers and that the final buffer has a delimiter and a valid ABA iteration number
    /// Returns distance to 'reachable buffer', if it succeeds in finding one
    [[nodiscard]] std::optional<size_t> searchLeading(STBufferIdx searchStartBufferIdx, ABAItNo abaItNumber) const;
    /// Analog to 'searchLeading', but searches in trailing direction (larger SNs)
    [[nodiscard]] std::optional<size_t> searchTrailing(STBufferIdx searchStartBufferIdx, ABAItNo abaItNumber) const;

    /// Assumes spanningTupleEndSN as the end of a potential spanning tuple.
    /// Searches in leading direction (smaller SNs) for a buffer with a delimiter that can start the spanning tuple.
    /// Aborts as soon as it finds a non-connecting ABA iteration number (different abaItNo and not first/last element of ring buffer).
    /// On finding a valid starting buffer, threads compete to claim that buffer (and thereby all buffers of the ST).
    /// Only one thread can succeed in claiming the first buffer (ClaimedSpanningTuple::firstBuffer != nullopt).
    [[nodiscard]] ClaimedSpanningTuple claimingLeadingDelimiterSearch(SequenceNumber stEndSN);
    /// Analog to 'claimingLeadingDelimiterSearch', but traverses in trailing direction (larger SNs)
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumber stStartSN, SequenceNumber searchStartSN);

    /// Calls claimingTrailingDelimiterSearch(spanningTupleStartSN, spanningTupleStartSN) <-- ST start is also where search should start
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumber stStartSN);

    /// Claims all trailing buffers of an ST (all buffers except the first, which the thread must have claimed already to claim the rest)
    /// Assumes size of 'spanningTupleBuffers' as the size of the ST, assigning using an index, instead of emplacing to the back
    void claimSTBuffers(SequenceNumber stStartSn, std::span<StagedBuffer> spanningTupleBuffers);
};
}

FMT_OSTREAM(NES::STBuffer);
