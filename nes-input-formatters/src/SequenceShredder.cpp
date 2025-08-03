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
#include <SequenceRingBuffer.hpp>

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

SequenceShredder::~SequenceShredder()
{
    try
    {
        if (ringBuffer.validate())
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
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / SIZE_OF_RING_BUFFER) + 1;
    const auto rbIdxOfSN = sequenceNumber % SIZE_OF_RING_BUFFER;

    if (not ringBuffer.trySetNewBufferWithDelimiter(rbIdxOfSN, abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
    }

    switch (auto searchAndClaimResult = ringBuffer.searchAndClaimBuffers(rbIdxOfSN, abaItNumber, sequenceNumber);
            searchAndClaimResult.state)
    {
        case SequenceRingBuffer::RangeSearchState::NONE: {
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
        }
        case SequenceRingBuffer::RangeSearchState::LEADING_ST: {
            const auto sizeOfSpanningTuple = sequenceNumber - searchAndClaimResult.leadingStartSN + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingStartBuffer.value());
            ringBuffer.claimSTupleBuffers(searchAndClaimResult.leadingStartSN, spanningTupleBuffers);
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = sizeOfSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        case SequenceRingBuffer::RangeSearchState::TRAILING_ST: {
            const auto sizeOfSpanningTuple = searchAndClaimResult.trailingStartSN - sequenceNumber + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.trailingStartBuffer.value());
            ringBuffer.claimSTupleBuffers(sequenceNumber, spanningTupleBuffers);
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        case SequenceRingBuffer::RangeSearchState::LEADING_AND_TRAILING_ST: {
            const auto sizeOfFirstST = sequenceNumber - searchAndClaimResult.leadingStartSN + 1;
            const auto sizeOfBothSTs = searchAndClaimResult.trailingStartSN - searchAndClaimResult.leadingStartSN + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfBothSTs);
            spanningTupleBuffers[0] = std::move(searchAndClaimResult.leadingStartBuffer.value());
            /// Claim the remaining buffers of both STs (separately, to not double claim the trailing buffer of the entry at 'rbIdxOfSN')
            ringBuffer.claimSTupleBuffers(searchAndClaimResult.leadingStartSN, std::span{spanningTupleBuffers}.subspan(0, sizeOfFirstST));
            ringBuffer.claimSTupleBuffers(sequenceNumber, std::span{spanningTupleBuffers}.subspan(sizeOfFirstST - 1));
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = sizeOfFirstST - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
    }
    std::unreachable();
}

SequenceShredderResult SequenceShredder::findSTsWithoutDelimiter(StagedBuffer indexedRawBuffer, SequenceNumberType sequenceNumber)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / SIZE_OF_RING_BUFFER) + 1;
    const auto rbIdxOfSN = sequenceNumber % SIZE_OF_RING_BUFFER;

    if (not ringBuffer.trySetNewBufferWithOutDelimiter(rbIdxOfSN, abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
    }

    switch (const auto searchResult = ringBuffer.searchWithoutClaimingBuffers(rbIdxOfSN, abaItNumber, sequenceNumber); searchResult.state)
    {
        case SequenceRingBuffer::NonClaimingRangeSearchState::NONE: {
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
        }
        case SequenceRingBuffer::NonClaimingRangeSearchState::LEADING_AND_TRAILING_ST: {
            const auto firstDelimiterIdx = searchResult.leadingStartSN % SIZE_OF_RING_BUFFER;
            const auto abaItNumberOfFirstDelimiter = abaItNumber - static_cast<size_t>(firstDelimiterIdx > rbIdxOfSN);
            if (auto optStagedBuffer = ringBuffer.tryClaimSpanningTuple(firstDelimiterIdx, abaItNumberOfFirstDelimiter))
            {
                const auto sizeOfSpanningTuple = searchResult.trailingStartSN - searchResult.leadingStartSN + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
                spanningTupleBuffers[0] = std::move(optStagedBuffer.value());
                ringBuffer.claimSTupleBuffers(searchResult.leadingStartSN, spanningTupleBuffers);
                const auto currentBufferIdx = sequenceNumber - searchResult.leadingStartSN;
                return SequenceShredderResult{
                    .isInRange = true, .indexOfInputBuffer = currentBufferIdx, .spanningBuffers = std::move(spanningTupleBuffers)};
            }
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
        }
    }
    std::unreachable();
}


std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder)
{
    return os << fmt::format("SequenceShredder({})", sequenceShredder.ringBuffer);
}
}
