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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
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
///
/// Kept incrementally: per key the waiting rows in stream order, and the set of rows that may start, so pushing, taking
/// and finishing cost O(log n) however many rows wait. A deep queue of rows that wait for their key is what lets a keyed
/// stage over input sorted by key keep every key busy.
template <typename Row>
class KeyedRowQueue
{
public:
    void push(Row row, std::vector<std::string> keys)
    {
        const auto position = nextPosition++;
        bool ready = true;
        for (const auto& key : keys)
        {
            auto& ofKey = waitingByKey[key];
            ready = ready && ofKey.empty() && !running.contains(key);
            ofKey.push_back(position);
        }
        waiting.emplace(position, Entry{std::move(row), std::move(keys)});
        if (ready)
        {
            startable.insert(position);
        }
    }

    /// Up to `limit` rows that may start, in stream order, at most one per key. Their keys count as running until
    /// `finish` releases them.
    std::vector<std::pair<Row, std::vector<std::string>>> take(const size_t limit)
    {
        std::vector<std::pair<Row, std::vector<std::string>>> taken;
        while (!startable.empty() && taken.size() < limit)
        {
            const auto position = *startable.begin();
            startable.erase(startable.begin());
            auto entry = waiting.extract(position);
            for (const auto& key : entry.mapped().keys)
            {
                ++running[key];
                const auto ofKey = waitingByKey.find(key);
                ofKey->second.pop_front();
                if (ofKey->second.empty())
                {
                    waitingByKey.erase(ofKey);
                }
            }
            taken.emplace_back(std::move(entry.mapped().row), std::move(entry.mapped().keys));
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
                if (const auto ofKey = waitingByKey.find(key); ofKey != waitingByKey.end())
                {
                    startIfReady(ofKey->second.front());
                }
            }
        }
    }

    /// Rows that `take` would hand out now, without a limit.
    [[nodiscard]] size_t startableRows() const { return startable.size(); }

    [[nodiscard]] size_t size() const { return waiting.size(); }

    [[nodiscard]] size_t runningKeys() const { return running.size(); }

    void clear()
    {
        waiting.clear();
        startable.clear();
        waitingByKey.clear();
        running.clear();
    }

private:
    struct Entry
    {
        Row row;
        std::vector<std::string> keys;
    };

    /// The row at `position` heads every one of its keys' waiting rows; it may start if none of them runs.
    void startIfReady(const uint64_t position)
    {
        const auto& keys = waiting.at(position).keys;
        if (std::ranges::all_of(
                keys, [&](const std::string& key) { return !running.contains(key) && waitingByKey.at(key).front() == position; }))
        {
            startable.insert(position);
        }
    }

    uint64_t nextPosition = 0;
    /// By stream position.
    std::map<uint64_t, Entry> waiting;
    std::set<uint64_t> startable;
    std::unordered_map<std::string, std::deque<uint64_t>> waitingByKey;
    std::unordered_map<std::string, size_t> running;
};

/// How often and how soon a failed async call is tried again.
struct RetryPolicy
{
    /// Further attempts after a failure that is not transient, the n-th after `baseBackoff` * 2^n.
    size_t maxRetries = 0;
    /// Further attempts after transient failures (`AsyncTransientFailure`), and the time from the call's first attempt
    /// within which they have to be made.
    size_t maxTransientRetries = 8;
    std::chrono::milliseconds transientTimeLimit{std::chrono::seconds{120}};
    std::chrono::milliseconds baseBackoff{100};
    /// Caps the backoff after transient failures; the one after other failures is capped at `baseBackoff` * 2^10.
    std::chrono::milliseconds maxBackoff{std::chrono::seconds{30}};
};

/// The retries of one call (one row, or one batch) under a `RetryPolicy`. Transient failures and other failures count
/// separately, so a service that is briefly overloaded does not use up the retries meant for real failures.
///
/// After the k-th transient failure the call waits d/2 plus a random part of d/2, with d = min(maxBackoff,
/// baseBackoff * 2^k): growing, but spread out so that calls that failed together do not come back together. A
/// service's own estimate (`Retry-After`) is the least it waits. The call gives up when the next attempt would come
/// later than `transientTimeLimit` after its first one.
class RetryBudget
{
public:
    RetryBudget(const RetryPolicy& policy, const std::chrono::steady_clock::time_point start) : policy(policy), start(start) { }

    /// How long to wait before the next attempt, or nothing if the call has to fail. `random` in [0, 1) picks the jitter.
    std::optional<std::chrono::milliseconds> next(
        const bool transient,
        const std::optional<std::chrono::milliseconds> retryAfter,
        const std::chrono::steady_clock::time_point now,
        const double random)
    {
        if (!transient)
        {
            if (retries >= policy.maxRetries)
            {
                return std::nullopt;
            }
            return doubled(retries++);
        }
        if (transientRetries >= policy.maxTransientRetries)
        {
            return std::nullopt;
        }
        const auto span = std::min(policy.maxBackoff, doubled(transientRetries));
        auto wait = (span / 2)
            + std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::duration<double, std::milli>(static_cast<double>(span.count()) / 2.0 * std::clamp(random, 0.0, 1.0)));
        if (retryAfter)
        {
            wait = std::max(wait, *retryAfter);
        }
        if (now + wait - start > policy.transientTimeLimit)
        {
            return std::nullopt;
        }
        ++transientRetries;
        return wait;
    }

    [[nodiscard]] size_t transientRetriesMade() const { return transientRetries; }

private:
    /// `baseBackoff` * 2^n, with n capped at 10.
    [[nodiscard]] std::chrono::milliseconds doubled(const size_t times) const
    {
        constexpr size_t maxDoublings = 10;
        return policy.baseBackoff * (int64_t{1} << std::min(times, maxDoublings));
    }

    RetryPolicy policy;
    std::chrono::steady_clock::time_point start;
    size_t retries = 0;
    size_t transientRetries = 0;
};

}
