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

#include <FragmentSequenceShredder.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <ostream>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <FragmentSpanningTupleBuffer.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>
#include <from_current.hpp>

namespace
{
/// TMP DIAGNOSTIC: accumulate wall time spent in the FragmentSequenceShredder's per-buffer spanning-tuple
/// resolution (find*), across all threads, printed at process exit. Mirrors DIAG_SHREDDER in
/// SequenceShredder.cpp so the LOCK_FREE vs LOCK_FREE_FRAGMENTS CPU share is directly comparable. REVERT before merge.
std::atomic<std::uint64_t> g_fragShredNs{0};
std::atomic<std::uint64_t> g_fragShredCalls{0};

const struct FragShredDiagPrinter
{
    ~FragShredDiagPrinter()
    {
        std::fprintf(
            stderr,
            "DIAG_FRAGSHREDDER shred_cpu=%.4fs calls=%llu\n",
            g_fragShredNs.load() / 1e9,
            static_cast<unsigned long long>(g_fragShredCalls.load()));
    }
} g_fragShredDiagPrinter;

struct FragShredScopeTimer
{
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();

    ~FragShredScopeTimer()
    {
        g_fragShredNs.fetch_add(
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count()),
            std::memory_order_relaxed);
        g_fragShredCalls.fetch_add(1, std::memory_order_relaxed);
    }
};
}

#define FRAG_SHRED_TIMER() const FragShredScopeTimer fragShredScopeTimer /// NOLINT

namespace NES
{

FragmentSequenceShredder::FragmentSequenceShredder(const size_t sizeOfTupleDelimiterInBytes)
{
    /// No dummy TupleBuffer needed: the first ring entry uses an empty owned fragment as the stream-start dummy
    this->spanningTupleBuffer = std::make_unique<FragmentSpanningTupleBuffer>(
        INITIAL_SIZE_OF_SPANNING_TUPLE_BUFFER, sizeOfTupleDelimiterInBytes, MAX_FRAGMENT_BYTES);
}

FragmentSequenceShredder::~FragmentSequenceShredder()
{
    CPPTRACE_TRY
    {
        if (spanningTupleBuffer->validate())
        {
            NES_INFO("Successfully validated FragmentSequenceShredder");
        }
        else
        {
            NES_ERROR("Failed to validate FragmentSequenceShredder");
        }
    }
    CPPTRACE_CATCH(...)
    {
        NES_ERROR("Unexpected exception during validation of FragmentSequenceShredder.");
    }
}

SequenceShredderResult FragmentSequenceShredder::findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer)
{
    FRAG_SHRED_TIMER();
    return findLeadingSpanningTupleWithDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SpanningBuffers FragmentSequenceShredder::findTrailingSpanningTupleWithDelimiter(const SequenceNumber sequenceNumber)
{
    FRAG_SHRED_TIMER();
    return spanningTupleBuffer->tryFindTrailingSpanningTupleForBufferWithDelimiter(sequenceNumber);
}

SpanningBuffers
FragmentSequenceShredder::findTrailingSpanningTupleWithDelimiter(const SequenceNumber sequenceNumber, const FieldIndex offsetOfLastTuple)
{
    FRAG_SHRED_TIMER();
    return spanningTupleBuffer->tryFindTrailingSpanningTupleForBufferWithDelimiter(sequenceNumber, offsetOfLastTuple);
}

SequenceShredderResult FragmentSequenceShredder::findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer)
{
    FRAG_SHRED_TIMER();
    return findSpanningTupleWithoutDelimiter(indexedRawBuffer, indexedRawBuffer.getRawTupleBuffer().getSequenceNumber());
}

SequenceShredderResult
FragmentSequenceShredder::findLeadingSpanningTupleWithDelimiter(const StagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    return spanningTupleBuffer->tryFindLeadingSpanningTupleForBufferWithDelimiter(sequenceNumber, indexedRawBuffer);
}

SequenceShredderResult
FragmentSequenceShredder::findSpanningTupleWithoutDelimiter(const StagedBuffer& indexedRawBuffer, const SequenceNumber sequenceNumber)
{
    if (const auto stSearchResult = spanningTupleBuffer->tryFindSpanningTupleForBufferWithoutDelimiter(sequenceNumber, indexedRawBuffer);
        stSearchResult.isInRange) [[likely]]
    {
        return stSearchResult;
    }
    else
    {
        NES_WARNING("Sequence number: {} was out of range of FragmentSpanningTupleBuffer", sequenceNumber);
        return stSearchResult;
    }
}

std::ostream& operator<<(std::ostream& os, const FragmentSequenceShredder& sequenceShredder)
{
    return os << fmt::format("FragmentSequenceShredder({})", *sequenceShredder.spanningTupleBuffer);
}
}
