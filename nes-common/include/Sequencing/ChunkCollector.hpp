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
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <list>
#include <optional>
#include <utility>
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
/// @tparam NodeSize Internally the ChunkCollector uses a linked list of arrays, which are called nodes. Usually a larger size of nodes will
/// yield better performance, but will use more memory.
/// @tparam Alignment From benchmarking, changing the alignment of the atomic counters does not appear to increase performance. However, this
/// could change in the future and an alignment of `std::hardware_destructive_interference_size` could improve performance
template <size_t NodeSize = 1024, size_t Alignment = 8>
class ChunkCollector
{
    static_assert(NodeSize > 0, "NodeSize must be greater than 0");

public:
    /// Collect a chunk and return a sequenceNumber and the associated watermark if all chunks have been collected.
    std::optional<std::pair<SequenceNumber, Timestamp>> collect(SequenceData data, Timestamp watermark);

    /// Frees the nodes whose sequence numbers are all at most `lastCompleted`, the only way nodes are freed.
    /// Every sequence number up to `lastCompleted` must be complete.
    void releaseUpTo(SequenceNumber lastCompleted);

    [[nodiscard]] size_t getNumberOfNodes() const { return nodes.rlock()->size(); }

private:
    template <typename T, typename MergeFunction, T InitialValue>
    class Chunk
    {
        std::atomic<ChunkNumber::Underlying> counter;
#ifndef NO_ASSERT
        std::atomic<bool> seenLastChunk = false;
        static constexpr uint64_t NO_RANGE_OFFSET = std::numeric_limits<uint64_t>::max();
        /// The range offset of the first collected chunk; every chunk of a sequence number must cover the same range.
        std::atomic<uint64_t> sequenceRangeOffset = NO_RANGE_OFFSET;
#endif
        std::atomic<T> value = {InitialValue};

    public:
        std::optional<T> update(const SequenceData& sequence, T newWatermark)
        {
#ifndef NO_ASSERT
            auto firstOffset = NO_RANGE_OFFSET;
            INVARIANT(
                sequenceRangeOffset.compare_exchange_strong(firstOffset, sequence.sequenceRangeOffset)
                    || firstOffset == sequence.sequenceRangeOffset,
                "Chunks of sequence {} cover different ranges: offset {} and {}",
                sequence.sequenceNumber,
                firstOffset,
                sequence.sequenceRangeOffset);
#endif
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

    template <typename T>
    struct Padded
    {
        alignas(Alignment) T v;
    };

    struct Node
    {
        explicit Node(size_t start) : start(start) { }

        size_t start;
        std::array<Padded<Chunk<Timestamp::Underlying, std::greater<>, std::numeric_limits<Timestamp::Underlying>::min()>>, NodeSize>
            data{};
    };

    folly::Synchronized<std::list<Node>> nodes;
    /// Number of leading nodes that `releaseUpTo` has freed.
    std::atomic<size_t> releasedNodes{0};
};

/// We store the chunk number counter for every non-completed sequence number. We want to be able to release completed chunk numbers so
/// they do not grow infinitely.
/// We use a linked list with each node holding N chunk number counters. Once all sequence numbers in one such node have been completed,
/// we can remove the node from the linked list without invalidating (moving) other counters.

/// We have to lock the linked list to locate the relevant node; if no such node exists, we append a new one. Within the node, we locate
/// the chunk counter for the sequence number. We decrease the counter for every non-last chunk. Once we receive the last chunk, we
/// increase the chunk counter by the current chunk number (which will be the maximum chunk number for this sequence). If the result of
/// updating the chunk number is 0, we know that the SequenceNumber is complete.

/// A buffer spanning multiple sequence numbers is tracked by its last sequence number.
template <size_t NodeSize, size_t Alignment>
std::optional<std::pair<SequenceNumber, Timestamp>> ChunkCollector<NodeSize, Alignment>::collect(SequenceData data, Timestamp watermark)
{
    PRECONDITION(data.sequenceNumber != SequenceNumber::INVALID, "SequenceNumber is invalid");
    PRECONDITION(data.chunkNumber != ChunkNumber::INVALID, "ChunkNumber is invalid");
    auto sequence = data.sequenceNumber - SequenceNumber::INITIAL;
    PRECONDITION(
        sequence / NodeSize >= releasedNodes.load(std::memory_order::relaxed), "Chunk of the completed sequence {}", data.sequenceNumber);

    auto& node = [&]() -> Node&
    {
        auto locked = nodes.ulock();
        auto it = std::ranges::find_if(*locked, [&](const auto& n) { return n.start <= sequence && sequence < n.start + NodeSize; });
        if (it == locked->end())
        {
            auto wlocked = locked.moveFromUpgradeToWrite();
            wlocked->emplace_back((sequence / NodeSize) * NodeSize);
            return wlocked->back();
        }
        return const_cast<Node&>(*it);
    }();

    auto& chunk = node.data[sequence % NodeSize].v;

    if (auto finalWatermark = chunk.update(data, watermark.getRawValue()))
    {
        return {{SequenceNumber(data.sequenceNumber), Timestamp(*finalWatermark)}};
    }
    return {};
}

/// `collect` uses a node after releasing the lock. Erasing it is safe, as no chunk of a released node arrives anymore.
template <size_t NodeSize, size_t Alignment>
void ChunkCollector<NodeSize, Alignment>::releaseUpTo(const SequenceNumber lastCompleted)
{
    const auto completedNodes = (lastCompleted.getRawValue() + 1 - SequenceNumber::INITIAL) / NodeSize;
    if (completedNodes <= releasedNodes.load(std::memory_order::relaxed))
    {
        return;
    }
    const auto locked = nodes.wlock();
    releasedNodes.store(std::max(releasedNodes.load(std::memory_order::relaxed), completedNodes), std::memory_order::relaxed);
    std::erase_if(*locked, [&](const Node& node) { return node.start < completedNodes * NodeSize; });
}
}
