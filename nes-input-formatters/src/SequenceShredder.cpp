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
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <RawTupleBuffer.hpp>
#include <STBuffer.hpp>
#include <from_current.hpp>

namespace NES
{

SequenceShredder::SequenceShredder() : spanningTupleBuffer(std::make_unique<STBuffer>(INITIAL_SIZE_OF_ST_BUFFER))
{
}

SequenceShredder::~SequenceShredder(){CPPTRACE_TRY{if (spanningTupleBuffer->validate()){NES_INFO("Successfully validated SequenceShredder");
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
;

SequenceShredderResult SequenceShredder::findSTsWithDelimiter(const StagedBuffer& indexedRawBuffer)
{
    return findSTsWithDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SequenceShredderResult SequenceShredder::findSTsWithoutDelimiter(const StagedBuffer& indexedRawBuffer)
{
    return findSTsWithoutDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SequenceShredderResult SequenceShredder::findSTsWithDelimiter(const StagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    if (const auto stSearchResult = spanningTupleBuffer->tryFindSTsForBufferWithDelimiter(sequenceNumber, indexedRawBuffer);
        stSearchResult.isInRange) [[likely]]
    {
        return stSearchResult;
    }
    else
    {
        /// (Planned) Atomically count the number of out of range attempts
        /// (Planned) Thread that increases atomic counter to threshold blocks access to the current STBUffer, allocates new STBuffer
        /// (Planned) with double the size, copies over the current state, swaps out the pointer to the STBuffer, and then enables other
        /// (Planned) threads to access the new STBuffer
        NES_WARNING("Sequence number: {} was out of range of STBuffer", sequenceNumber);
        return stSearchResult;
    }
}

SequenceShredderResult SequenceShredder::findSTsWithoutDelimiter(const StagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    if (const auto stSearchResult = spanningTupleBuffer->tryFindSTsForBufferWithoutDelimiter(sequenceNumber, indexedRawBuffer);
        stSearchResult.isInRange) [[likely]]
    {
        return stSearchResult;
    }
    else
    {
        NES_WARNING("Sequence number: {} was out of range of STBuffer", sequenceNumber);
        return stSearchResult;
    }
}

std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder)
{
    return os << fmt::format("SequenceShredder({})", *sequenceShredder.spanningTupleBuffer);
}
}
