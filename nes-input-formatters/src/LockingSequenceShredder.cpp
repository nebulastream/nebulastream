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

#include <LockingSequenceShredder.hpp>

#include <cstddef>
#include <memory>
#include <mutex>
#include <ostream>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LockingSpanningTupleBuffer.hpp>
#include <RawTupleBuffer.hpp>
#include <from_current.hpp>

namespace NES
{

LockingSequenceShredder::LockingSequenceShredder(const size_t sizeOfTupleDelimiterInBytes)
{
    auto dummyBuffer = BufferManager::create(1, 1)->getBufferBlocking();
    dummyBuffer.setNumberOfTuples(sizeOfTupleDelimiterInBytes);
    this->spanningTupleBuffer = std::make_unique<LockingSpanningTupleBuffer>(INITIAL_SIZE_OF_SPANNING_TUPLE_BUFFER, std::move(dummyBuffer));
}

LockingSequenceShredder::~LockingSequenceShredder()
{
    CPPTRACE_TRY
    {
        if (spanningTupleBuffer->validate())
        {
            NES_INFO("Successfully validated LockingSequenceShredder");
        }
        else
        {
            NES_ERROR("Failed to validate LockingSequenceShredder");
        }
    }
    CPPTRACE_CATCH(...)
    {
        NES_ERROR("Unexpected exception during validation of LockingSequenceShredder.");
    }
}

SequenceShredderResult LockingSequenceShredder::findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer)
{
    return findLeadingSpanningTupleWithDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SequenceShredderResult
LockingSequenceShredder::findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    std::lock_guard<std::mutex> lock(mutex);
    return spanningTupleBuffer->tryFindLeadingSpanningTupleForBufferWithDelimiter(sequenceNumber, indexedRawBuffer);
}

SpanningBuffers LockingSequenceShredder::findTrailingSpanningTupleWithDelimiter(const SequenceNumber sequenceNumber)
{
    std::lock_guard<std::mutex> lock(mutex);
    return spanningTupleBuffer->tryFindTrailingSpanningTupleForBufferWithDelimiter(sequenceNumber);
}

SpanningBuffers
LockingSequenceShredder::findTrailingSpanningTupleWithDelimiter(const SequenceNumber sequenceNumber, const FieldIndex offsetOfLastTuple)
{
    std::lock_guard<std::mutex> lock(mutex);
    return spanningTupleBuffer->tryFindTrailingSpanningTupleForBufferWithDelimiter(sequenceNumber, offsetOfLastTuple);
}

SequenceShredderResult LockingSequenceShredder::findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer)
{
    return findSpanningTupleWithoutDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SequenceShredderResult
LockingSequenceShredder::findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    std::lock_guard<std::mutex> lock(mutex);
    if (const auto stSearchResult = spanningTupleBuffer->tryFindSpanningTupleForBufferWithoutDelimiter(sequenceNumber, indexedRawBuffer);
        stSearchResult.isInRange) [[likely]]
    {
        return stSearchResult;
    }
    else
    {
        NES_WARNING("Sequence number: {} was out of range of LockingSpanningTupleBuffer", sequenceNumber);
        return stSearchResult;
    }
}

std::ostream& operator<<(std::ostream& os, const LockingSequenceShredder& sequenceShredder)
{
    return os << fmt::format("LockingSequenceShredder({})", *sequenceShredder.spanningTupleBuffer);
}

}
