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

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace NES
{

/// Where a buffer stands in its stream: per origin, sequence by sequence, and within a sequence chunk 1..k, the chunk
/// marked last being k (`EmitOperatorHandler::setChunkNumber`).
struct StreamPosition
{
    uint64_t origin;
    uint64_t sequence;
    uint64_t chunk;
    bool lastChunk;
};

/// Restores the stream order of buffers that arrive out of order, origin by origin. Origins are independent: a buffer is
/// held back only by missing buffers of its own origin, and different origins are interleaved as they arrive.
///
/// Every origin starts at sequence 1, chunk 1. A position that is already behind its origin (sent twice, or from a
/// producer that does not number its buffers) is released at once rather than held forever.
template <typename Item>
class StreamReorderer
{
public:
    static constexpr uint64_t FIRST_SEQUENCE = 1;
    static constexpr uint64_t FIRST_CHUNK = 1;

    /// The items that are next in their origin now, `item` itself and any it unblocked, in stream order.
    std::vector<Item> push(const StreamPosition& position, Item item)
    {
        std::vector<Item> released;
        auto& origin = origins[position.origin];
        if (std::tie(position.sequence, position.chunk) < std::tie(origin.nextSequence, origin.nextChunk))
        {
            released.push_back(std::move(item));
            return released;
        }
        origin.waiting.emplace(std::pair{position.sequence, position.chunk}, Waiting{position.lastChunk, std::move(item)});
        ++held;
        for (auto next = origin.waiting.find({origin.nextSequence, origin.nextChunk}); next != origin.waiting.end();
             next = origin.waiting.find({origin.nextSequence, origin.nextChunk}))
        {
            if (next->second.lastChunk)
            {
                ++origin.nextSequence;
                origin.nextChunk = FIRST_CHUNK;
            }
            else
            {
                ++origin.nextChunk;
            }
            released.push_back(std::move(next->second.item));
            origin.waiting.erase(next);
            --held;
        }
        return released;
    }

    /// Everything still held, in stream order per origin, origins by id. Only at the end of the stream, when no
    /// missing buffer can arrive any more.
    std::vector<Item> drain()
    {
        std::vector<Item> released;
        for (auto& [id, origin] : origins)
        {
            for (auto& [position, waiting] : origin.waiting)
            {
                released.push_back(std::move(waiting.item));
            }
            origin.waiting.clear();
        }
        held = 0;
        return released;
    }

    /// Buffers waiting for an earlier one of their origin.
    [[nodiscard]] size_t size() const { return held; }

private:
    struct Waiting
    {
        bool lastChunk;
        Item item;
    };

    struct Origin
    {
        uint64_t nextSequence = FIRST_SEQUENCE;
        uint64_t nextChunk = FIRST_CHUNK;
        std::map<std::pair<uint64_t, uint64_t>, Waiting> waiting;
    };

    std::map<uint64_t, Origin> origins;
    size_t held = 0;
};

/// The rows of an async stage that wait to start, in stream order, each with the ordering keys of its calls.
///
/// A row may start once none of its keys is held by a running row and no row before it that is still waiting has one
/// of its keys. So rows of one key start one at a time and in stream order, rows of different keys run side by side, and
/// rows without keys start in stream order whenever a slot is free. Waiting rows hold nothing but their place here.
template <typename Row>
class KeyedRowQueue
{
public:
    void push(Row row, std::vector<std::string> keys) { waiting.push_back({std::move(row), std::move(keys)}); }

    /// Up to `limit` rows that may start, in stream order, at most one per key. Their keys count as running until
    /// `finish` releases them.
    std::vector<std::pair<Row, std::vector<std::string>>> take(const size_t limit)
    {
        std::vector<std::pair<Row, std::vector<std::string>>> taken;
        std::unordered_set<std::string> blocked;
        for (auto entry = waiting.begin(); entry != waiting.end() && taken.size() < limit;)
        {
            if (startable(entry->keys, blocked))
            {
                for (const auto& key : entry->keys)
                {
                    ++running[key];
                    blocked.insert(key);
                }
                taken.emplace_back(std::move(entry->row), std::move(entry->keys));
                entry = waiting.erase(entry);
                continue;
            }
            blocked.insert(entry->keys.begin(), entry->keys.end());
            ++entry;
        }
        return taken;
    }

    /// A row that `take` handed out has finished.
    void finish(const std::vector<std::string>& keys)
    {
        for (const auto& key : keys)
        {
            if (const auto entry = running.find(key); entry != running.end() && --entry->second == 0)
            {
                running.erase(entry);
            }
        }
    }

    /// Rows that `take` would hand out now, without a limit.
    [[nodiscard]] size_t startableRows() const
    {
        size_t count = 0;
        std::unordered_set<std::string> blocked;
        for (const auto& entry : waiting)
        {
            if (startable(entry.keys, blocked))
            {
                ++count;
            }
            blocked.insert(entry.keys.begin(), entry.keys.end());
        }
        return count;
    }

    [[nodiscard]] size_t size() const { return waiting.size(); }

    [[nodiscard]] size_t runningKeys() const { return running.size(); }

    void clear()
    {
        waiting.clear();
        running.clear();
    }

private:
    struct Entry
    {
        Row row;
        std::vector<std::string> keys;
    };

    [[nodiscard]] bool startable(const std::vector<std::string>& keys, const std::unordered_set<std::string>& blocked) const
    {
        for (const auto& key : keys)
        {
            if (running.contains(key) || blocked.contains(key))
            {
                return false;
            }
        }
        return true;
    }

    std::list<Entry> waiting;
    std::unordered_map<std::string, size_t> running;
};

}
