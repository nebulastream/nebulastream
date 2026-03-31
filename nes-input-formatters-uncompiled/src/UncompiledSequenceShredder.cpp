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

#include <UncompiledSequenceShredder.hpp>

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
#include <UncompiledRawTupleBuffer.hpp>
#include <UncompiledSTBuffer.hpp>
#include <from_current.hpp>

namespace NES
{

UncompiledSequenceShredder::UncompiledSequenceShredder(const size_t sizeOfTupleDelimiterInBytes)
{
    auto dummyBuffer = BufferManager::create(1, 1)->getBufferBlocking();
    dummyBuffer.setNumberOfTuples(sizeOfTupleDelimiterInBytes);
    this->spanningTupleBuffer = std::make_unique<UncompiledSTBuffer>(INITIAL_SIZE_OF_ST_BUFFER, std::move(dummyBuffer));
}

UncompiledSequenceShredder::~UncompiledSequenceShredder()
{
    CPPTRACE_TRY
    {
        if (spanningTupleBuffer->validate())
        {
            NES_INFO("Successfully validated UncompiledSequenceShredder");
        }
        else
        {
            NES_ERROR("Failed to validate UncompiledSequenceShredder");
        }
    }
    CPPTRACE_CATCH(...)
    {
        NES_ERROR("Unexpected exception during validation of UncompiledSequenceShredder.");
    }
}

UncompiledSequenceShredderResult UncompiledSequenceShredder::findLeadingSTWithDelimiter(const UncompiledStagedBuffer& indexedRawBuffer)
{
    return findLeadingSTWithDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

UncompiledSpanningBuffers UncompiledSequenceShredder::findTrailingSTWithDelimiter(const SequenceNumber sequenceNumber)
{
    return spanningTupleBuffer->tryFindTrailingSTForBufferWithDelimiter(sequenceNumber);
}

UncompiledSequenceShredderResult UncompiledSequenceShredder::findSTWithoutDelimiter(const UncompiledStagedBuffer& indexedRawBuffer)
{
    return findSTWithoutDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

UncompiledSequenceShredderResult
UncompiledSequenceShredder::findLeadingSTWithDelimiter(const UncompiledStagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    /// (Planned) Atomically count the number of out of range attempts
    /// (Planned) Thread that increases atomic counter to threshold blocks access to the current STBUffer, allocates new UncompiledSTBuffer
    /// (Planned) with double the size, copies over the current state, swaps out the pointer to the UncompiledSTBuffer, and then enables other
    /// (Planned) threads to access the new UncompiledSTBuffer
    return spanningTupleBuffer->tryFindLeadingSTForBufferWithDelimiter(sequenceNumber, indexedRawBuffer);
}

UncompiledSequenceShredderResult UncompiledSequenceShredder::findSTWithoutDelimiter(const UncompiledStagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    if (const auto stSearchResult = spanningTupleBuffer->tryFindSTForBufferWithoutDelimiter(sequenceNumber, indexedRawBuffer);
        stSearchResult.isInRange) [[likely]]
    {
        return stSearchResult;
    }
    else
    {
        NES_WARNING("Sequence number: {} was out of range of UncompiledSTBuffer", sequenceNumber);
        return stSearchResult;
    }
}

std::ostream& operator<<(std::ostream& os, const UncompiledSequenceShredder& sequenceShredder)
{
    return os << fmt::format("UncompiledSequenceShredder({})", *sequenceShredder.spanningTupleBuffer);
}
}
