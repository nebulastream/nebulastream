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
#include <optional>
#include <ostream>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>
#include <Util/Ranges.hpp>
#include <STBufferState.hpp>
#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

/// The STBuffer enables the SequenceShredder to:
/// 1.
class STBuffer
{
    /// Result of trying to claim a buffer (with a specific SN) as the start of a spanning tuple.
    /// Multiple threads may try to claim a specific buffer. Only the thread that wins the race gets 'firstBuffer' with a value.
    struct ClaimedSpanningTuple
    {
        std::optional<StagedBuffer> firstBuffer;
        SequenceNumberType snOfLastBuffer{};
    };

public:
    struct ClaimingSearchResult
    {
        enum class Type : uint8_t
        {
            NONE,
            LEADING_ST_ONLY,
            TRAILING_ST_ONLY,
            LEADING_AND_TRAILING_ST,
            NOT_IN_RANGE,
        };

        Type type = Type::NONE;
        std::optional<StagedBuffer> leadingSTupleStartBuffer = std::nullopt;
        std::optional<StagedBuffer> trailingSTupleStartBuffer = std::nullopt;
        SequenceNumberType sTupleStartSN{};
        SequenceNumberType sTupleEndSN{};
    };

    explicit STBuffer(size_t initialSize);

    /// First, checks if the prior entry at the index of 'sequenceNumber' contains the expected prior ABA iteration number and is used up
    /// If not, returns as 'NOT_IN_RANGE'
    /// Otherwise, searches for valid spanning tuples and on finding a spanning tuple, tries to claim the first buffer of the spanning tuple
    /// for the calling thread, which claims the entire spanning tuple.
    [[nodiscard]] ClaimingSearchResult tryFindSTsForBufferWithDelimiter(size_t sequenceNumber, const StagedBuffer& indexedRawBuffer);
    [[nodiscard]] ClaimingSearchResult tryFindSTsForBufferWithoutDelimiter(size_t sequenceNumber, const StagedBuffer& indexedRawBuffer);

    /// Claims all trailing buffers of a STuple (all buffers except the first, which the thread must have claimed already to claim the rest)
    /// Assumes size of 'spanningTupleBuffers' as the size of the ST, assigning using an index, instead of emplacing to the back
    void claimSTupleBuffers(size_t sTupleStartSN, std::span<StagedBuffer> spanningTupleBuffers);

    [[nodiscard]] bool validate() const;

    friend std::ostream& operator<<(std::ostream& os, const STBuffer& sequenceRingBuffer);

private:
    std::vector<STBufferEntry> buffer;

    [[nodiscard]] std::pair<SequenceNumberType, uint32_t> getBufferIdxAndABAItNo(SequenceNumberType sequenceNumber) const;

    /// Searches for two buffers with delimiters that are connected by the buffer with the provided 'sequenceNumber'
    /// Tries to claim the (first buffer of the) leading spanning tuple, if search is successful
    [[nodiscard]] ClaimingSearchResult searchAndTryClaimLeadingSTuple(SequenceNumberType sequenceNumber);

    /// Searches for spanning tuples both in leading and trailing direction of the 'sequenceNumber'
    /// Tries to claim the (first buffers of the) spanning tuples (and thereby the STs) if it finds valid STs.
    [[nodiscard]] ClaimingSearchResult searchAndTryClaimLeadingAndTrailingSTuple(SequenceNumberType sequenceNumber);

    /// Searches for a reachable buffer that can start a spanning tuple (in leading direction - smaller SNs)
    /// 'Reachable' means there is a path from 'searchStartBufferIdx' to the buffer that traverses 0 or more buffers without delimiters and
    /// valid ABA iteration numbers and that the target buffer has a delimiter and a valid ABA iteration number
    /// Returns distance to 'reachable buffer', if it succeeds in finding one
    [[nodiscard]] std::optional<size_t> searchLeading(size_t searchStartBufferIdx, size_t abaItNumber) const;
    /// Analog to 'searchLeading', but searches in trailing direction (larger SNs)
    [[nodiscard]] std::optional<size_t> searchTrailing(size_t searchStartBufferIdx, size_t abaItNumber) const;

    /// Assumes spanningTupleEndSN as the end of a potential spanning tuple.
    /// Searches in leading direction (smaller SNs) for a buffer with a delimiter that can start the spanning tuple.
    /// Aborts as soon as it finds a non-connecting ABA iteration number (different and not first/last element of ring buffer).
    /// On finding a valid starting buffer, threads compete to claim that buffer (and thereby all buffers of the ST).
    /// Only one thread can succeed in claiming the first buffer (ClaimedSpanningTuple::firstBuffer != nullopt).
    [[nodiscard]] ClaimedSpanningTuple claimingLeadingDelimiterSearch(SequenceNumberType spanningTupleEndSN);


    /// Assumes spanningTupleStartSN as the start of a potential spanning tuple.
    /// Searches in trailing direction (larger SNs) for a buffer with a delimiter that terminates the spanning tuple.
    /// Aborts as soon as it finds a non-connecting ABA iteration number (different and not first/last element of ring buffer).
    /// On finding a valid terminating buffer, threads compete to claim the first buffer of the ST(spanningTupleStartSN) and thereby the ST.
    /// Only one thread can succeed in claiming the first buffer (ClaimedSpanningTuple::firstBuffer != nullopt).
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumberType sTupleStartSN, SequenceNumberType searchStartSN);

    /// Calls claimingTrailingDelimiterSearch(spanningTupleStartSN, spanningTupleStartSN) <-- sTuple start is also where search should start
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumberType sTupleStartSN);
};
}

FMT_OSTREAM(NES::InputFormatters::STBuffer);