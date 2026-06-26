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

#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <variant>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// system_clock (wall-clock) microseconds. Used by the source runners to stamp read-start and by the
/// sink to stamp arrival, so latency = arrival - read-start is a true end-to-end value in us (ms was
/// too coarse: latencies came out 0/1 ms). Same clock domain on both ends is essential.
inline uint64_t ingestNowMicros()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

/// Opt-in per-source ingest-copy meter. When NES_INGEST_COPY_STATS names a file, the source runner
/// brackets the read/recv + kernel copy into the pool buffer (fillBuffer / fillTupleBuffer) -- exactly
/// the cost zero-copy aims to remove -- and appends the per-source copy rate on stream end. When unset
/// it is effectively free (one predictable branch per buffer, no extra clock read, no IO). Release
/// builds strip logging, so this file channel is how the copy cost is surfaced.
/// CSV columns: source_id,bytes,copy_us,copy_gbps,us_per_buffer.
class IngestCopyMeter
{
    const char* const path = std::getenv("NES_INGEST_COPY_STATS"); ///NOLINT(concurrency-mt-unsafe)
    uint64_t micros = 0;
    uint64_t bytes = 0;
    uint64_t bufs = 0;

public:
    [[nodiscard]] bool enabled() const { return path != nullptr; }

    void add(const uint64_t copyMicros, const uint64_t numBytes)
    {
        micros += copyMicros;
        bytes += numBytes;
        ++bufs;
    }

    void writeOnClose(const OriginId sourceId) const
    {
        if (path != nullptr && bufs > 0 && micros > 0)
        {
            std::ofstream(path, std::ios::app) << fmt::format(
                "{},{},{},{:.3f},{:.2f}\n",
                sourceId,
                bytes,
                micros,
                static_cast<double>(bytes) / static_cast<double>(micros) / 1000.0,
                static_cast<double>(micros) / static_cast<double>(bufs));
        }
    }
};

inline void addBufferMetadata(const OriginId originId, TupleBuffer& buffer, const uint64_t sequenceNumber)
{
    buffer.setOriginId(originId);
    /// creationTimestamp = ingestion/event time used by windowing (currentTs). Keep it in ms and on
    /// system_clock (wall-clock, 1970 epoch) to match the sink side.
    /// NB: sourceCreationTimestamp is intentionally NOT set here. The async runner stamps it at
    /// READ-START in MICROSECONDS (before fillBuffer), so the sink measures ingestion-inclusive e2e
    /// latency/throughput and the runner can isolate the per-buffer copy cost. See AsyncSourceRunner.
    buffer.setCreationTimestampInMS(Timestamp(static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count())));
    buffer.setSequenceNumber(SequenceNumber{sequenceNumber});
    buffer.setChunkNumber(ChunkNumber{1});
    buffer.setLastChunk(true);

    NES_TRACE(
        "Setting buffer metadata with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
        buffer.getOriginId(),
        buffer.getSequenceNumber(),
        buffer.getChunkNumber(),
        buffer.isLastChunk());
}
}
