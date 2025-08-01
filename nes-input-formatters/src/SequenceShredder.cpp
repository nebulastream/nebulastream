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
#include <fmt/format.h>
#include <fmt/ranges.h>
#include "RawTupleBuffer.hpp"
#include "SequenceRingBuffer.hpp"
#include "Util/Ranges.hpp"

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

SequenceShredder::~SequenceShredder()
{
    try
    {
        ringBuffer.validate();
    }
    catch (...)
    {
        NES_ERROR("Validation failed unexpectedly.");
    }
};
// Todo:
// - strong types for SN, ABA, other indexes
// - impl(.cpp) for SequenceRingBuffer (<-- rename to SpanningRingBuffer?)
// - clean up validation
// - implement logging

SequenceShredderResult SequenceShredder::processBufferWithDelimiter(StagedBuffer indexedRawBuffer, const SequenceNumberType sequenceNumber)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / SIZE_OF_RING_BUFFER) + 1;
    const auto snRBIdx = sequenceNumber % SIZE_OF_RING_BUFFER;

    if (not ringBuffer.rangeCheck<true>(snRBIdx, abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
    }

    switch (const auto searchAndClaimResult = ringBuffer.searchAndClaimBuffers(snRBIdx, abaItNumber, sequenceNumber);
            searchAndClaimResult.state)
    {
        case SequenceRingBuffer<SIZE_OF_RING_BUFFER>::RangeSearchState::NONE: {
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
        }
        case SequenceRingBuffer<SIZE_OF_RING_BUFFER>::RangeSearchState::LEADING_ST: {
            const auto sizeOfSpanningTuple = sequenceNumber - searchAndClaimResult.leadingStartSN + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = searchAndClaimResult.leadingStartBuffer.value();
            const auto firstIndex = searchAndClaimResult.leadingStartSN % SIZE_OF_RING_BUFFER;
            ringBuffer.setCompletedFlags(firstIndex, snRBIdx, spanningTupleBuffers, 1);
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = sizeOfSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        case SequenceRingBuffer<SIZE_OF_RING_BUFFER>::RangeSearchState::TRAILING_ST: {
            const auto sizeOfSpanningTuple = searchAndClaimResult.trailingStartSN - sequenceNumber + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = searchAndClaimResult.trailingStartBuffer.value();
            const auto lastIndex = searchAndClaimResult.trailingStartSN % SIZE_OF_RING_BUFFER;
            ringBuffer.setCompletedFlags(snRBIdx, lastIndex, spanningTupleBuffers, 1);
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        case SequenceRingBuffer<SIZE_OF_RING_BUFFER>::RangeSearchState::LEADING_AND_TRAILING_ST: {
            const auto sizeOfFirstSpanningTuple = sequenceNumber - searchAndClaimResult.leadingStartSN + 1;
            const auto sizeOfBothSpanningTuples = searchAndClaimResult.trailingStartSN - searchAndClaimResult.leadingStartSN + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfBothSpanningTuples);
            spanningTupleBuffers[0] = searchAndClaimResult.leadingStartBuffer.value();
            const auto firstIndex = searchAndClaimResult.leadingStartSN % SIZE_OF_RING_BUFFER;
            ringBuffer.setCompletedFlags(firstIndex, snRBIdx, spanningTupleBuffers, 1);
            const auto lastIndex = searchAndClaimResult.trailingStartSN % SIZE_OF_RING_BUFFER;
            ringBuffer.setCompletedFlags(snRBIdx, lastIndex, spanningTupleBuffers, sizeOfFirstSpanningTuple);
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = sizeOfFirstSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
    }
    std::unreachable();
}

SequenceShredderResult SequenceShredder::processBufferWithoutDelimiter(StagedBuffer indexedRawBuffer, SequenceNumberType sequenceNumber)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / SIZE_OF_RING_BUFFER) + 1;
    const auto snRBIdx = sequenceNumber % SIZE_OF_RING_BUFFER;

    if (not ringBuffer.rangeCheck<false>(snRBIdx, abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
    }

    switch (const auto searchResult = ringBuffer.searchWithoutClaimingBuffers(snRBIdx, abaItNumber, sequenceNumber); searchResult.state)
    {
        case SequenceRingBuffer<SIZE_OF_RING_BUFFER>::NonClaimingRangeSearchState::LEADING_AND_TRAILING_ST: {
            const auto firstDelimiterIdx = searchResult.leadingStartSN % SIZE_OF_RING_BUFFER;
            const auto abaItNumberOfFirstDelimiter = abaItNumber - static_cast<size_t>(firstDelimiterIdx > snRBIdx);
            if (const auto optStagedBuffer = ringBuffer.tryClaimSpanningTuple(firstDelimiterIdx, abaItNumberOfFirstDelimiter))
            {
                const auto sizeOfSpanningTuple = searchResult.trailingStartSN - searchResult.leadingStartSN + 1;
                std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
                spanningTupleBuffers[0] = optStagedBuffer.value();
                ringBuffer.setCompletedFlags(
                    firstDelimiterIdx, searchResult.trailingStartSN % SIZE_OF_RING_BUFFER, spanningTupleBuffers, 1);
                const auto currentBufferIdx = sequenceNumber - searchResult.leadingStartSN;
                return SequenceShredderResult{
                    .isInRange = true, .indexOfInputBuffer = currentBufferIdx, .spanningBuffers = std::move(spanningTupleBuffers)};
            }
        }
        case SequenceRingBuffer<SIZE_OF_RING_BUFFER>::NonClaimingRangeSearchState::NONE: {
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
        }
    }
    std::unreachable();
}


std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder)
{
    // Todo: implement logging
    (void)sequenceShredder;
    return os;
}
}
