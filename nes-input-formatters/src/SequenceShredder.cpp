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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
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
#include <from_current.hpp>

namespace
{
/// TMP DIAGNOSTIC: accumulate wall time spent in the SequenceShredder's per-buffer spanning-tuple
/// resolution (find*), across all threads, printed at process exit. Measures the shredder's share of
/// the compiled formatter pipeline. REVERT before merge.
std::atomic<std::uint64_t> g_shredNs{0};
std::atomic<std::uint64_t> g_shredCalls{0};

const struct ShredDiagPrinter
{
    ~ShredDiagPrinter()
    {
        std::fprintf(
            stderr,
            "DIAG_SHREDDER shred_cpu=%.4fs calls=%llu\n",
            g_shredNs.load() / 1e9,
            static_cast<unsigned long long>(g_shredCalls.load()));
    }
} g_shredDiagPrinter;

struct ShredScopeTimer
{
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();

    ~ShredScopeTimer()
    {
        g_shredNs.fetch_add(
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count()),
            std::memory_order_relaxed);
        g_shredCalls.fetch_add(1, std::memory_order_relaxed);
    }
};
}

#define SHRED_TIMER() const ShredScopeTimer shredScopeTimer /// NOLINT

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
    SHRED_TIMER();
    return findLeadingSpanningTupleWithDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SpanningBuffers SequenceShredder::findTrailingSpanningTupleWithDelimiter(const SequenceNumber sequenceNumber)
{
    SHRED_TIMER();
    return spanningTupleBuffer->tryFindTrailingSpanningTupleForBufferWithDelimiter(sequenceNumber);
}

SpanningBuffers
SequenceShredder::findTrailingSpanningTupleWithDelimiter(const SequenceNumber sequenceNumber, const FieldIndex offsetOfLastTuple)
{
    SHRED_TIMER();
    return spanningTupleBuffer->tryFindTrailingSpanningTupleForBufferWithDelimiter(sequenceNumber, offsetOfLastTuple);
}

SequenceShredderResult SequenceShredder::findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer)
{
    SHRED_TIMER();
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
