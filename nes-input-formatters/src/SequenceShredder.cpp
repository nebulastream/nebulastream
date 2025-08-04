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

#include <SequenceShredder.hpp>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <RawTupleBuffer.hpp>
#include <STBuffer.hpp>

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

SequenceShredder::SequenceShredder() : ringBuffer(std::make_unique<STBuffer>(SIZE_OF_RING_BUFFER)) {}
SequenceShredder::~SequenceShredder()
{
    try
    {
        if (ringBuffer->validate())
        {
            NES_INFO("Successfully validated SequenceShredder");
        }
        else
        {
            NES_ERROR("Failed to validate SequenceShredder");
        }
    }
    catch (...)
    {
        NES_ERROR("Unexpected exception during validation of SequenceShredder.");
    }
};

SequenceShredderResult SequenceShredder::findSTsWithDelimiter(StagedBuffer indexedRawBuffer, const SequenceNumberType sequenceNumber)
{
    switch (auto searchAndClaimResult = ringBuffer->tryFindSTsForBufferWithDelimiter(sequenceNumber, indexedRawBuffer);
            searchAndClaimResult.type)
    {
        case STBuffer::ClaimingSearchResult::Type::NOT_IN_RANGE: {
            return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
        }
        case STBuffer::ClaimingSearchResult::Type::NONE: {
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
        }
        case STBuffer::ClaimingSearchResult::Type::LEADING_ST_ONLY: {
            const auto sizeOfSpanningTuple = sequenceNumber - searchAndClaimResult.sTupleStartSN + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTupleStartBuffer.value());
            ringBuffer->claimSTupleBuffers(searchAndClaimResult.sTupleStartSN, spanningTupleBuffers);
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = sizeOfSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        case STBuffer::ClaimingSearchResult::Type::TRAILING_ST_ONLY: {
            const auto sizeOfSpanningTuple = searchAndClaimResult.sTupleEndSN - sequenceNumber + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.trailingSTupleStartBuffer.value());
            ringBuffer->claimSTupleBuffers(sequenceNumber, spanningTupleBuffers);
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        case STBuffer::ClaimingSearchResult::Type::LEADING_AND_TRAILING_ST: {
            const auto sizeOfFirstST = sequenceNumber - searchAndClaimResult.sTupleStartSN + 1;
            const auto sizeOfBothSTs = searchAndClaimResult.sTupleEndSN - searchAndClaimResult.sTupleStartSN + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfBothSTs);
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTupleStartBuffer.value());
            /// Claim the remaining buffers of both STs (separately, to not double claim the trailing buffer of the entry at 'rbIdxOfSN')
            ringBuffer->claimSTupleBuffers(searchAndClaimResult.sTupleStartSN, std::span{spanningTupleBuffers}.subspan(0, sizeOfFirstST));
            ringBuffer->claimSTupleBuffers(sequenceNumber, std::span{spanningTupleBuffers}.subspan(sizeOfFirstST - 1));
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = sizeOfFirstST - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
    }
    std::unreachable();
}

SequenceShredderResult SequenceShredder::findSTsWithoutDelimiter(StagedBuffer indexedRawBuffer, const SequenceNumberType sequenceNumber)
{
    switch (auto searchAndClaimResult = ringBuffer->tryFindSTsForBufferWithoutDelimiter(sequenceNumber, indexedRawBuffer);
            searchAndClaimResult.type)
    {
        case STBuffer::ClaimingSearchResult::Type::NOT_IN_RANGE: {
            return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
        }
        case STBuffer::ClaimingSearchResult::Type::NONE: {
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
        }
        case STBuffer::ClaimingSearchResult::Type::LEADING_ST_ONLY: {
            const auto sizeOfSpanningTuple = searchAndClaimResult.sTupleEndSN - searchAndClaimResult.sTupleStartSN + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingSTupleStartBuffer.value());
            ringBuffer->claimSTupleBuffers(searchAndClaimResult.sTupleStartSN, spanningTupleBuffers);
            const auto currentBufferIdx = sequenceNumber - searchAndClaimResult.sTupleEndSN;
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = currentBufferIdx, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        case STBuffer::ClaimingSearchResult::Type::TRAILING_ST_ONLY:
        case STBuffer::ClaimingSearchResult::Type::LEADING_AND_TRAILING_ST:
            INVARIANT(false, "A buffer without a delimiter can only find one spanning tuple in leading direction.");
    }
    std::unreachable();
}


std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder)
{
    return os << fmt::format("SequenceShredder({})", *sequenceShredder.ringBuffer);
}
}
