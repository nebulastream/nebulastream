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
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>
#include <PlainSpanningTupleBufferState.hpp>

namespace NES
{

/// Same logic as SpanningTupleBuffer but uses PlainSpanningTupleBufferEntry (non-atomic state).
/// All access must be externally synchronized with a mutex.
class LockingSpanningTupleBuffer
{
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

    explicit LockingSpanningTupleBuffer(size_t initialSize, TupleBuffer dummyBuffer);

    [[nodiscard]] SequenceShredderResult
    tryFindLeadingSpanningTupleForBufferWithDelimiter(SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer);
    [[nodiscard]] SpanningBuffers tryFindTrailingSpanningTupleForBufferWithDelimiter(SequenceNumber sequenceNumber);
    [[nodiscard]] SpanningBuffers
    tryFindTrailingSpanningTupleForBufferWithDelimiter(SequenceNumber sequenceNumber, FieldIndex offsetOfLastTuple);

    [[nodiscard]] SequenceShredderResult
    tryFindSpanningTupleForBufferWithoutDelimiter(SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer);

    [[nodiscard]] bool validate() const;

    friend std::ostream& operator<<(std::ostream& os, const LockingSpanningTupleBuffer& spanningTupleBuffer);

private:
    std::vector<PlainSpanningTupleBufferEntry> buffer;

    [[nodiscard]] std::pair<SpanningTupleBufferIdx, ABAItNo> getBufferIdxAndABAItNo(SequenceNumber sequenceNumber) const;

    [[nodiscard]] WithoutDelimiterSearchResult searchAndTryClaimWithoutDelimiter(SequenceNumber sequenceNumber);

    [[nodiscard]] std::optional<size_t> searchLeading(SpanningTupleBufferIdx searchStartBufferIdx, ABAItNo abaItNumber) const;
    [[nodiscard]] std::optional<size_t> searchTrailing(SpanningTupleBufferIdx searchStartBufferIdx, ABAItNo abaItNumber) const;

    [[nodiscard]] ClaimedSpanningTuple claimingLeadingDelimiterSearch(SequenceNumber stEndSN);
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumber stStartSN, SequenceNumber searchStartSN);
    [[nodiscard]] ClaimedSpanningTuple claimingTrailingDelimiterSearch(SequenceNumber stStartSN);

    void claimSpanningTupleBuffers(SequenceNumber stStartSn, std::span<StagedBuffer> spanningTupleBuffers);
};
}

FMT_OSTREAM(NES::LockingSpanningTupleBuffer);
