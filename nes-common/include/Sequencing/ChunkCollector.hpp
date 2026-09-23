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
#include <atomic>
#include <cassert>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// The purpose of the ChunkCollector is to keep track of SequenceNumber which have been split into multiple chunks.
/// The ChunkCollector is able to collect chunks in a multithreaded scenario and return if all chunks for a specific sequence number
/// have been seen.
class ChunkCollector
{
public:
    /// Collect a chunk and return a sequenceNumber and the associated watermark if all chunks have been collected.
    std::optional<std::pair<SequenceNumber, Timestamp>> collect(SequenceData data, Timestamp watermark);

private:
    template <typename T, typename MergeFunction, T InitialValue>
    class Chunk
    {
        std::atomic<ChunkNumber::Underlying> counter;
#ifndef NO_ASSERT
        std::atomic<bool> seenLastChunk = false;
#endif
        std::atomic<T> value = {InitialValue};

    public:
        std::optional<T> update(const SequenceData& sequence, T newWatermark)
        {
            const auto chunk = sequence.chunkNumber - ChunkNumber::INITIAL;
            auto current = value.load();
            while (MergeFunction{}(newWatermark, current) && !value.compare_exchange_weak(current, newWatermark))
            {
            }

            /// Updating if the last chunk has been seen. We do not need to lock the value, as we only update the value once.
            if (sequence.lastChunk)
            {
                INVARIANT(
                    not std::atomic_exchange(&seenLastChunk, true),
                    "Last chunk has already been seen for this sequence {}. We require that the last chunk is only seen once.",
                    sequence.sequenceNumber);
            }

            /// If the chunk is the last chunk, we update the counter with the current chunk number, otherwise we decrease the counter
            /// This way, we can release the chunk number once all chunks have been collected ---> counter == 0
            const auto updatedCounter = sequence.lastChunk ? counter.fetch_add(chunk) + chunk : counter.fetch_sub(1) - 1;

            if (updatedCounter == 0)
            {
                return {value.load()};
            }

            return {};
        }
    };

    folly::Synchronized<std::map<
        SequenceNumber::Underlying,
        Chunk<Timestamp::Underlying, std::greater<>, std::numeric_limits<Timestamp::Underlying>::min()>>>
        chunks;
};

inline std::optional<std::pair<SequenceNumber, Timestamp>>
ChunkCollector::collect(SequenceData data, Timestamp watermark)
{
    PRECONDITION(data.sequenceNumber != SequenceNumber::INVALID, "SequenceNumber is invalid");
    PRECONDITION(data.chunkNumber != ChunkNumber::INVALID, "ChunkNumber is invalid");

    auto wlocked = chunks.wlock();

    auto& chunk = (*wlocked)[data.sequenceNumber];

    if (auto finalWatermark = chunk.update(data, watermark.getRawValue()))
    {
        wlocked->erase(data.sequenceNumber);
        return {{SequenceNumber(data.sequenceNumber), Timestamp(*finalWatermark)}};
    }
    return {};
}
}
