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
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/StatisticListener.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <folly/Synchronized.h>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>

namespace NES
{

/// Aggregates the query engine event stream into per-query counters that the status RPCs report.
///
/// Sharded by the emitting worker thread: each worker only ever touches its own shard on the
/// per-task hot path, so there is no global lock and a query's counters are never written by more
/// than one thread (no cross-thread cacheline contention). getCounters — called only by the status
/// RPC — sums a query's counters across all shards.
class MetricsListener final : public StatisticListener
{
public:
    MetricsListener();
    void onEvent(Event event) override;
    void onEvent(SystemEvent event) override;

    /// Counters for one query, summed across shards; zeros if the query never produced events.
    [[nodiscard]] QueryCounters getCounters(const QueryId& queryId) const;

private:
    struct AtomicCounters
    {
        std::atomic<uint64_t> processedTuples{0};
        std::atomic<uint64_t> processedTasks{0};
        std::atomic<uint64_t> expiredTasks{0};
    };

    /// Typical x86-64/arm64 cacheline; shards are aligned to it so neighbours' locks don't false-share.
    static constexpr std::size_t cacheLineBytes = 64;

    /// One shard per worker thread (picked by WorkerThreadId % shards.size()). Cacheline-aligned so
    /// adjacent shards' locks don't false-share. Entries are created on first event of a query and
    /// kept until worker shutdown; QueryLog is equally unbounded today — if it ever gains retention,
    /// these shards must be evicted in lockstep. std::unordered_map is node-based, so references
    /// handed out by countersFor stay valid after the lock is released.
    struct alignas(cacheLineBytes) Shard
    {
        folly::Synchronized<std::unordered_map<QueryId, AtomicCounters>> counters;
    };

    /// Sized to the hardware concurrency at construction, so worker ids (0..N-1) usually map 1:1.
    std::vector<Shard> shards;

    [[nodiscard]] Shard& shardFor(WorkerThreadId threadId);
    [[nodiscard]] static AtomicCounters& countersFor(Shard& shard, const QueryId& queryId);
};

}
