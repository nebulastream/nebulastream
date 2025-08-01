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

#include <bit>
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
#include <variant>
#include <vector>

#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include "RawTupleBuffer.hpp"
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
// - clean up validation
// - implement
// - enum return

// Todo: instead of template and requires, simply create different functions that are not templated
SequenceShredderResult SequenceShredder::processBufferWithDelimiter(StagedBuffer indexedRawBuffer, const SequenceNumberType sequenceNumber)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / SIZE_OF_RING_BUFFER) + 1;
    const auto snRBIdx = sequenceNumber % SIZE_OF_RING_BUFFER;

    if (not ringBuffer.rangeCheck<true>(snRBIdx, abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
    }

    // Todo: since the two searches differ from the 'no delimiter' searches anyway
    // -> perform a combined 'claiming' search that returns an enum, which we use below to determine what to do
    const auto [firstBuffer, firstBufferSN] = ringBuffer.leadingDelimiterSearch(snRBIdx, abaItNumber, sequenceNumber);
    const auto [secondBuffer, secondBufferSN]
        = ringBuffer.trailingDelimiterSearch(snRBIdx, abaItNumber, sequenceNumber, snRBIdx, abaItNumber);

    /// Neither a leading, nor a trailing spanning tuple found, return empty vector
    if (not(firstBuffer.has_value()) and not(secondBuffer.has_value()))
    {
        return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
    }
    if (firstBuffer.has_value() and not(secondBuffer.has_value()))
    {
        const auto sizeOfSpanningTuple = sequenceNumber - firstBufferSN + 1;
        std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
        spanningTupleBuffers[0] = firstBuffer.value();
        const auto firstIndex = firstBufferSN % SIZE_OF_RING_BUFFER;
        ringBuffer.setCompletedFlags(firstIndex, snRBIdx, spanningTupleBuffers, 1);
        return SequenceShredderResult{
            .isInRange = true, .indexOfInputBuffer = sizeOfSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
    }
    if (not(firstBuffer.has_value()) and secondBuffer.has_value())
    {
        const auto sizeOfSpanningTuple = secondBufferSN - sequenceNumber + 1;
        std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
        spanningTupleBuffers[0] = secondBuffer.value();
        const auto lastIndex = secondBufferSN % SIZE_OF_RING_BUFFER;
        ringBuffer.setCompletedFlags(snRBIdx, lastIndex, spanningTupleBuffers, 1);
        return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = std::move(spanningTupleBuffers)};
    }
    if (firstBuffer.has_value() and secondBuffer.has_value())
    {
        const auto sizeOfFirstSpanningTuple = sequenceNumber - firstBufferSN + 1;
        const auto sizeOfBothSpanningTuples = secondBufferSN - firstBufferSN + 1;
        std::vector<StagedBuffer> spanningTupleBuffers(sizeOfBothSpanningTuples);
        spanningTupleBuffers[0] = firstBuffer.value();
        const auto firstIndex = firstBufferSN % SIZE_OF_RING_BUFFER;
        ringBuffer.setCompletedFlags(firstIndex, snRBIdx, spanningTupleBuffers, 1);
        const auto lastIndex = secondBufferSN % SIZE_OF_RING_BUFFER;
        ringBuffer.setCompletedFlags(snRBIdx, lastIndex, spanningTupleBuffers, sizeOfFirstSpanningTuple);
        return SequenceShredderResult{
            .isInRange = true, .indexOfInputBuffer = sizeOfFirstSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
    }
    std::unreachable();
}

SequenceShredderResult SequenceShredder::processBufferWithoutDelimiter(StagedBuffer indexedRawBuffer, SequenceNumberType sequenceNumber)
{
    const auto abaItNumber = static_cast<uint32_t>(sequenceNumber / SIZE_OF_RING_BUFFER) + 1;
    const auto snRBIdx = sequenceNumber % SIZE_OF_RING_BUFFER;

    // No Tuple Delimiter
    if (not ringBuffer.rangeCheck<false>(snRBIdx, abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
    }

    const auto firstDelimiter = ringBuffer.nonClaimingLeadingDelimiterSearch(snRBIdx, abaItNumber, sequenceNumber);
    if (not firstDelimiter.has_value())
    {
        return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
    }

    if (const auto lastDelimiter = ringBuffer.nonClaimingTrailingDelimiterSearch(snRBIdx, abaItNumber, sequenceNumber);
        lastDelimiter.has_value())
    {
        const auto firstDelimiterIdx = firstDelimiter.value() % SIZE_OF_RING_BUFFER;
        const auto abaItNumberOfFirstDelimiter = abaItNumber - static_cast<size_t>(firstDelimiterIdx > snRBIdx);
        if (const auto optStagedBuffer = ringBuffer.tryClaimSpanningTuple(firstDelimiterIdx, abaItNumberOfFirstDelimiter))
        {
            const auto sizeOfSpanningTuple = lastDelimiter.value() - firstDelimiter.value() + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = optStagedBuffer.value();
            /// Successfully claimed the first tuple, now set the rest
            ringBuffer.setCompletedFlags(firstDelimiterIdx, lastDelimiter.value() % SIZE_OF_RING_BUFFER, spanningTupleBuffers, 1);
            const auto currentBufferIdx = sequenceNumber - firstDelimiter.value();
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = currentBufferIdx, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
    }
    return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
}


std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder)
{
    // Todo: implement logging
    (void)sequenceShredder;
    return os;
}
}
