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
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <RawTupleBuffer.hpp>
#include <SpanningTupleBuffer.hpp>

namespace NES
{

SequenceShredder::SequenceShredder(const size_t sizeOfTupleDelimiterInBytes)
{
    auto dummyBuffer = BufferManager::create(1, 1)->getBufferBlocking();
    dummyBuffer.setNumberOfTuples(sizeOfTupleDelimiterInBytes);
    this->spanningTupleBuffer = std::make_unique<SpanningTupleBuffer>(INITIAL_SIZE_OF_SPANNING_TUPLE_BUFFER, std::move(dummyBuffer));
}

SequenceShredder::~SequenceShredder()
{
    CPPTRACE_TRY
    {
        if (spanningTupleBuffer->validate())
        {
            NES_INFO("Successfully validated SequenceShredder");
        }
        else
        {
            NES_ERROR("Failed to validate SequenceShredder");
        }
    }
    CPPTRACE_CATCH(...)
    {
        NES_ERROR("Unexpected exception during validation of SequenceShredder.");
    }
}

SequenceShredderResult SequenceShredder::findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer)
{
    return findLeadingSpanningTupleWithDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SpanningBuffers SequenceShredder::findTrailingSpanningTupleWithDelimiter(const SequenceNumber sequenceNumber)
{
    return spanningTupleBuffer->tryFindTrailingSpanningTupleForBufferWithDelimiter(sequenceNumber);
}

SpanningBuffers
SequenceShredder::findTrailingSpanningTupleWithDelimiter(const SequenceNumber sequenceNumber, const FieldIndex offsetOfLastTuple)
{
    return spanningTupleBuffer->tryFindTrailingSpanningTupleForBufferWithDelimiter(sequenceNumber, offsetOfLastTuple);
}

SequenceShredderResult SequenceShredder::findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer)
{
    return findSpanningTupleWithoutDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SequenceShredderResult
SequenceShredder::findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    /// (Planned) Atomically count the number of out of range attempts
    /// (Planned) Thread that increases atomic counter to threshold blocks access to the current SpanningTupleBUffer, allocates new SpanningTupleBuffer
    /// (Planned) with double the size, copies over the current state, swaps out the pointer to the SpanningTupleBuffer, and then enables other
    /// (Planned) threads to access the new SpanningTupleBuffer
    return spanningTupleBuffer->tryFindLeadingSpanningTupleForBufferWithDelimiter(sequenceNumber, indexedRawBuffer);
}

SequenceShredderResult
SequenceShredder::findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    if (const auto stSearchResult = spanningTupleBuffer->tryFindSpanningTupleForBufferWithoutDelimiter(sequenceNumber, indexedRawBuffer);
        stSearchResult.isInRange) [[likely]]
    {
        return stSearchResult;
    }
    else
    {
        NES_WARNING("Sequence number: {} was out of range of SpanningTupleBuffer", sequenceNumber);
        return stSearchResult;
    }
}

std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder)
{
    return os << fmt::format("SequenceShredder({})", *sequenceShredder.spanningTupleBuffer);
}
}
