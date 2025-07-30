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
#include "Util/Ranges.hpp"

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

SequenceShredder::~SequenceShredder()
{
    try
    {
        for (const auto& [idx, metaData] : ringBuffer | NES::views::enumerate)
        {
            const auto state = metaData.getState();
            INVARIANT(state.hasUsedLeadingBuffer() == true, "Buffer at index {} does still claim to own leading buffer", idx);
            INVARIANT(metaData.isLeadingBufferRefNull() == true, "Buffer at index {} still owns a leading buffer reference", idx);

            const auto& nextEntryRef = ringBuffer.at((idx + 1) % ringBuffer.size());
            const auto nextEntryRefState = nextEntryRef.getState();
            const auto adjustment = static_cast<size_t>(static_cast<size_t>(idx + 1) == ringBuffer.size());
            if (state.getABAItNo() == nextEntryRefState.getABAItNo() - adjustment)
            {
                INVARIANT(state.hasUsedTrailingBuffer() == true, "Buffer at index {} does still claim to own leading buffer", idx);
                INVARIANT(metaData.isTrailingBufferRefNull() == true, "Buffer at index {} still owns a trailing buffer reference", idx);
            }
        }
    }
    catch (...)
    {
        NES_ERROR("Validation failed unexpectedly.");
    }
};

uint32_t getABAItNumber(const size_t sequenceNumber, const size_t rbSize)
{
    return static_cast<uint32_t>(sequenceNumber / rbSize) + 1;
}

// Todo(When replacing sequence shredder impl): convert 'hasNoTD' to function on 'SSMetaData' object
// -> 'RingBuffer' should be its own class and expose the functions below and the lambda functions
// -> make sure to hide implementation details to the outside, the interface should potentially be a single function
//    -> 'resolveSpanningTuple()' that either returns an empty spanning tuple (fail) or a spanning tuple with at least 2 buffers
//      -> gets rid of 'isInRange()', evaluating lazily, might mean we already indexed an buffer
//          -> if it becomes performance critical, implement 'stashed' queue that threads (that otherwise did not do any work?) complete
template <bool IsLeading>
bool hasNoTD(const size_t snRBIdx, const size_t abaItNumber, const size_t distance, const std::vector<SSMetaData>& tds)
{
    const auto adjustedSN = (IsLeading) ? snRBIdx - distance : snRBIdx + distance;
    const auto tdsIdx = adjustedSN % tds.size();
    const auto bitmapState = tds[tdsIdx].getState();
    if (IsLeading)
    {
        const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < distance);
        return bitmapState.hasNoTupleDelimiter() and bitmapState.getABAItNo() == adjustedAbaItNumber;
    }
    const auto adjustedAbaItNumber = abaItNumber + static_cast<size_t>((snRBIdx + distance) >= tds.size());
    return bitmapState.hasNoTupleDelimiter() and bitmapState.getABAItNo() == adjustedAbaItNumber;
}

template <bool IsLeading>
bool hasTD(const size_t snRBIdx, const size_t abaItNumber, const size_t distance, const std::vector<SSMetaData>& tds)
{
    const auto adjustedSN = (IsLeading) ? snRBIdx - distance : snRBIdx + distance;
    const auto tdsIdx = adjustedSN % tds.size();
    const auto bitmapState = tds[tdsIdx].getState();
    if (IsLeading)
    {
        const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < distance);
        return bitmapState.hasTupleDelimiter() and bitmapState.getABAItNo() == adjustedAbaItNumber;
    }
    const auto adjustedAbaItNumber = abaItNumber + static_cast<size_t>((snRBIdx + distance) >= tds.size());
    return bitmapState.hasTupleDelimiter() and bitmapState.getABAItNo() == adjustedAbaItNumber;
}

void setCompletedFlags(
    const size_t firstDelimiter,
    const size_t lastDelimiter,
    std::vector<SSMetaData>& ringBuffer,
    std::vector<StagedBuffer>& spanningTupleBuffers,
    size_t spanningTupleIdx)
{
    size_t nextDelimiter = (firstDelimiter + 1) % ringBuffer.size();
    while (nextDelimiter != lastDelimiter)
    {
        ringBuffer[nextDelimiter].claimNoDelimiterBuffer(spanningTupleBuffers, spanningTupleIdx);
        ++spanningTupleIdx;
        nextDelimiter = (nextDelimiter + 1) % ringBuffer.size();
    }
    ringBuffer[lastDelimiter].claimLeadingBuffer(spanningTupleBuffers, spanningTupleIdx);
}

// Todo: could also just return TupleBuffer (and get SN from buffer), but initial dummy buffer is problem
// -> could honestly think about creating struct that mimics buffer (and control block) and reinterpet_casting it to TupleBuffer
std::optional<uint32_t> nonClaimingLeadingDelimiterSearch(
    const std::vector<SSMetaData>& ringBuffer,
    const size_t snRBIdx,
    const size_t abaItNumber,
    const SequenceShredder::SequenceNumberType currentSequenceNumber)
{
    size_t leadingDistance = 1;
    while (hasNoTD<true>(snRBIdx, abaItNumber, leadingDistance, ringBuffer))
    {
        ++leadingDistance;
    }
    return hasTD<true>(snRBIdx, abaItNumber, leadingDistance, ringBuffer) ? std::optional{currentSequenceNumber - leadingDistance}
                                                                          : std::nullopt;
}
std::optional<uint32_t> nonClaimingTrailingDelimiterSearch(
    const std::vector<SSMetaData>& ringBuffer,
    const size_t snRBIdx,
    const size_t abaItNumber,
    const SequenceShredder::SequenceNumberType currentSequenceNumber)
{
    size_t trailingDistance = 1;
    while (hasNoTD<false>(snRBIdx, abaItNumber, trailingDistance, ringBuffer))
    {
        ++trailingDistance;
    }
    return hasTD<false>(snRBIdx, abaItNumber, trailingDistance, ringBuffer) ? std::optional{currentSequenceNumber + trailingDistance}
                                                                            : std::nullopt;
};
std::pair<std::optional<StagedBuffer>, SequenceShredder::SequenceNumberType> leadingDelimiterSearch(
    std::vector<SSMetaData>& ringBuffer,
    const size_t snRBIdx,
    const size_t abaItNumber,
    const SequenceShredder::SequenceNumberType currentSequenceNumber)
{
    size_t leadingDistance = 1;
    while (hasNoTD<true>(snRBIdx, abaItNumber, leadingDistance, ringBuffer))
    {
        ++leadingDistance;
    }
    if (hasTD<true>(snRBIdx, abaItNumber, leadingDistance, ringBuffer))
    {
        const auto spanningTupleStartSN = currentSequenceNumber - leadingDistance;
        const auto spanningTupleStartIdx = spanningTupleStartSN % ringBuffer.size();
        // Todo: adjust aba number while iterating
        const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < leadingDistance);
        return std::make_pair(ringBuffer[spanningTupleStartIdx].tryClaimSpanningTuple(adjustedAbaItNumber), spanningTupleStartSN);
    }
    return std::make_pair(std::nullopt, 0);
};

std::pair<std::optional<StagedBuffer>, SequenceShredder::SequenceNumberType> trailingDelimiterSearch(
    std::vector<SSMetaData>& ringBuffer,
    const size_t snRBIdx,
    const size_t abaItNumber,
    const SequenceShredder::SequenceNumberType currentSequenceNumber,
    const size_t spanningTupleStartIdx,
    const size_t abaItNumberSpanningTupleStart)
{
    size_t trailingDistance = 1;

    while (hasNoTD<false>(snRBIdx, abaItNumber, trailingDistance, ringBuffer))
    {
        ++trailingDistance;
    }
    if (hasTD<false>(snRBIdx, abaItNumber, trailingDistance, ringBuffer))
    {
        const auto leadingSequenceNumber = currentSequenceNumber + trailingDistance;

        /// Todo Problem:
        /// - we adjust the ABAItNumber based on the end of the spanning tuple, but we want to claim the start of the spanning tuple
        /// - the 'spanningTupleStartIdx'
        return std::make_pair(
            ringBuffer[spanningTupleStartIdx].tryClaimSpanningTuple(abaItNumberSpanningTupleStart), leadingSequenceNumber);
    }
    return std::make_pair(std::nullopt, 0);
    // return hasTD<false>(snRBIdx, abaItNumber, trailingDistance, ringBuffer) ? std::optional{currentSequenceNumber + trailingDistance}
    //                                                                         : std::nullopt;
};

// Todo: either also use 'requires' approach here, instead of if constexpr
//      -> or combine (but combining is probably not a good idea <-- rather use helper functions to abstract away similar behavior
template <bool HasTupleDelimiter>
SequenceShredderResult SequenceShredder::processSequenceNumber(StagedBuffer indexedRawBuffer, const SequenceNumberType sequenceNumber)
{
    const auto abaItNumber = getABAItNumber(sequenceNumber, ringBuffer.size());
    const auto snRBIdx = sequenceNumber % ringBuffer.size();

    // Todo: streamline both cases better
    if constexpr (HasTupleDelimiter)
    {
        if (not ringBuffer[snRBIdx].isInRange<true>(abaItNumber, indexedRawBuffer))
        {
            return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
        }

        const auto [firstBuffer, firstBufferSN] = leadingDelimiterSearch(ringBuffer, snRBIdx, abaItNumber, sequenceNumber);
        const auto [secondBuffer, secondBufferSN]
            = trailingDelimiterSearch(ringBuffer, snRBIdx, abaItNumber, sequenceNumber, snRBIdx, abaItNumber);
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
            const auto firstIndex = firstBufferSN % ringBuffer.size();
            setCompletedFlags(firstIndex, snRBIdx, ringBuffer, spanningTupleBuffers, 1);
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = sizeOfSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        if (not(firstBuffer.has_value()) and secondBuffer.has_value())
        {
            const auto sizeOfSpanningTuple = secondBufferSN - sequenceNumber + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = secondBuffer.value();
            const auto lastIndex = secondBufferSN % ringBuffer.size();
            setCompletedFlags(snRBIdx, lastIndex, ringBuffer, spanningTupleBuffers, 1);
            return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
        if (firstBuffer.has_value() and secondBuffer.has_value())
        {
            const auto sizeOfFirstSpanningTuple = sequenceNumber - firstBufferSN + 1;
            const auto sizeOfBothSpanningTuples = secondBufferSN - firstBufferSN + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfBothSpanningTuples);
            spanningTupleBuffers[0] = firstBuffer.value();
            const auto firstIndex = firstBufferSN % ringBuffer.size();
            setCompletedFlags(firstIndex, snRBIdx, ringBuffer, spanningTupleBuffers, 1);
            const auto lastIndex = secondBufferSN % ringBuffer.size();
            setCompletedFlags(snRBIdx, lastIndex, ringBuffer, spanningTupleBuffers, sizeOfFirstSpanningTuple);
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = sizeOfFirstSpanningTuple - 1, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
    }

    // No Tuple Delimiter
    if (not ringBuffer[snRBIdx].isInRange<false>(abaItNumber, indexedRawBuffer))
    {
        return SequenceShredderResult{.isInRange = false, .indexOfInputBuffer = 0, .spanningBuffers = {}};
    }
    // TODO: must not try to claim during first search
    // -> we want to claim the tuple delimiter buffer that we find during the leading search
    // -> Two options:
    //      1. leading search -> successful -> trailing search -> successful -> claim leading
    //      2. trailing search -> successful -> try-claim leading search
    const auto firstDelimiter = nonClaimingLeadingDelimiterSearch(ringBuffer, snRBIdx, abaItNumber, sequenceNumber);
    if (not firstDelimiter.has_value())
    {
        return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
    }

    if (const auto lastDelimiter = nonClaimingTrailingDelimiterSearch(ringBuffer, snRBIdx, abaItNumber, sequenceNumber);
        lastDelimiter.has_value())
    {
        const auto firstDelimiterIdx = firstDelimiter.value() % ringBuffer.size();
        const auto abaItNumberOfFirstDelimiter = abaItNumber - static_cast<size_t>(firstDelimiterIdx > snRBIdx);
        if (const auto optStagedBuffer = ringBuffer[firstDelimiterIdx].tryClaimSpanningTuple(abaItNumberOfFirstDelimiter))
        {
            const auto sizeOfSpanningTuple = lastDelimiter.value() - firstDelimiter.value() + 1;
            std::vector<StagedBuffer> spanningTupleBuffers(sizeOfSpanningTuple);
            spanningTupleBuffers[0] = optStagedBuffer.value();
            /// Successfully claimed the first tuple, now set the rest
            setCompletedFlags(firstDelimiterIdx, lastDelimiter.value() % ringBuffer.size(), ringBuffer, spanningTupleBuffers, 1);
            const auto currentBufferIdx = sequenceNumber - firstDelimiter.value();
            return SequenceShredderResult{
                .isInRange = true, .indexOfInputBuffer = currentBufferIdx, .spanningBuffers = std::move(spanningTupleBuffers)};
        }
    }
    return SequenceShredderResult{.isInRange = true, .indexOfInputBuffer = 0, .spanningBuffers = {indexedRawBuffer}};
}
/// Instantiate processSequenceNumber for both 'true' and 'false' so that the linker knows which templates to generate.
template SequenceShredderResult SequenceShredder::processSequenceNumber<true>(StagedBuffer, SequenceNumberType);
template SequenceShredderResult SequenceShredder::processSequenceNumber<false>(StagedBuffer, SequenceNumberType);


std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder)
{
    // Todo: implement logging
    (void)sequenceShredder;
    return os;
}
}
