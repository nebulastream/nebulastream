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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <optional>
#include <semaphore>
#include <stop_token>
#include <thread>
#include <utility>
#include <vector>
#include <immintrin.h>
#include <folly/MPMCQueue.h>
#include <folly/concurrency/UnboundedQueue.h>

namespace NES
{

/// CCX-aware sibling of TaskQueue (see TaskQueue.hpp; enabled via the ccx_aware_task_queues
/// worker option): the bounded admission queue is SHARDED per CCX (one queue per L3 domain)
/// while the internal queue stays global. Emitting threads (io/source threads) enqueue into the
/// shard of their own CCX; workers dequeue internal-first, then their OWN shard, then steal from
/// the other shards in ring order (idle-only stealing). This keeps a source's recycled buffer
/// ring inside one L3: without sharding, any worker on any CCX may scan any raw buffer, leaving
/// its cache lines Shared in a remote L3, and the source's copy loop pays a cross-fabric RFO per
/// line on recycle (measured -43% supply at 23 workers vs 5, 2026-07-09).
///
/// Concurrency design: ONE global counting semaphore covers internal + all shards
/// (INVARIANT: internal.size() + sum(shards[s].size()) >= tasksAvailable). A permit therefore
/// guarantees a task exists SOMEWHERE, not where: a full scan can miss (a concurrent dequeue
/// took "our" task while we passed its shard), but permit conservation -- every dequeue redeems
/// exactly one permit -- bounds consecutive misses by the number of in-flight dequeuers, so
/// rescanning terminates. The blocking path caps rescans (ValveRounds) and then releases its
/// permit and re-enters the timed acquire loop (lossless; restores stop-token responsiveness).
/// The non-blocking path (termination drain) rescans unboundedly: returning nullopt while
/// provably holding evidence of a task could strand that task at shutdown.
template <typename TaskType>
class ShardedTaskQueue
{
    folly::UMPMCQueue<TaskType, true> internal;
    /// deque (not vector): folly::MPMCQueue is neither copyable nor movable.
    std::deque<folly::MPMCQueue<TaskType>> shards;

    /// INVARIANT: internal.size() + sum(shards[s].size()) >= tasksAvailable
    std::counting_semaphore<> tasksAvailable{0};

    /// To provide cancellation, we only block for StopTokenCheckInterval (as TaskQueue).
    static constexpr std::chrono::milliseconds StopTokenCheckInterval{100};
    /// Failed full scans in the blocking path before the permit is returned (safety valve).
    static constexpr size_t ValveRounds{256};

    /// STEAL DELAY (env NES_CCX_STEAL_DELAY_US, default 100, 0 = steal immediately): the global
    /// semaphore wakes an ARBITRARY parked worker, so without a delay a remote-CCX worker races
    /// the (busy) owner-CCX workers to the shard and wins constantly -- measured 32%/52% stolen
    /// at W11/W23 on q2, which reintroduces the cross-CCX scanning the sharding exists to
    /// prevent. With the delay, a woken worker whose own shard is empty RELEASES its permit and
    /// sleeps off the wake pool (permits then pool up for the owner CCX's workers, whose
    /// try_acquire fast path never parks); it steals only if the foreign backlog persists longer
    /// than the delay (i.e. the owner CCX genuinely cannot keep up).
    const std::chrono::microseconds stealDelay{
        []
        {
            const char* env = std::getenv("NES_CCX_STEAL_DELAY_US");
            return std::chrono::microseconds{env != nullptr ? std::strtoul(env, nullptr, 10) : 100UL};
        }()};

    /// Diagnostics (env NES_TASKQUEUE_STATS): per-shard enqueue/owner-take/steal counters +
    /// global scan health. `stolen[s]` counts dequeues from shard s by a worker whose own shard
    /// differs -- the validation signal for "idle-only stealing" (expect ~0 when all workers
    /// share the io thread's CCX, a thin tail otherwise). valveTrips is expected to stay 0.
    const bool statsEnabled{std::getenv("NES_TASKQUEUE_STATS") != nullptr};

    struct alignas(64) ShardStats
    {
        std::atomic<uint64_t> enqueued{0};
        std::atomic<uint64_t> takenByOwner{0};
        std::atomic<uint64_t> stolen{0};
    };

    std::deque<ShardStats> shardStats;
    std::atomic<uint64_t> statInternalTaken{0};
    std::atomic<uint64_t> statFast{0};
    std::atomic<uint64_t> statSlow{0};
    std::atomic<uint64_t> statRescanRounds{0};
    std::atomic<uint64_t> statValveTrips{0};

    /// One scan in locality order: internal, own shard, then (if allowed) the other shards in
    /// ring order. Caller must hold one permit; a miss means a concurrent dequeuer raced us,
    /// folly's `read` failed spuriously, or the only tasks live in remote shards while stealing
    /// is not (yet) allowed.
    std::optional<TaskType> tryScanOnce(const size_t ownShard, const bool allowSteal)
    {
        TaskType task;
        if (internal.try_dequeue(task))
        {
            if (statsEnabled) [[unlikely]]
            {
                statInternalTaken.fetch_add(1, std::memory_order_relaxed);
            }
            return task;
        }
        const size_t shardsToScan = allowSteal ? shards.size() : 1;
        for (size_t k = 0; k < shardsToScan; ++k)
        {
            /// Own shard first, then ring order. On a single NUMA node all remote CCXs are
            /// equidistant, so no distance-sorted order is needed.
            const size_t shard = (ownShard + k) % shards.size();
            if (shards[shard].read(task))
            {
                if (statsEnabled) [[unlikely]]
                {
                    (shard == ownShard ? shardStats[shard].takenByOwner : shardStats[shard].stolen).fetch_add(1, std::memory_order_relaxed);
                }
                return task;
            }
        }
        return std::nullopt;
    }

    /// Redeem a held permit into a task. Blocking flavor: bounded rescans, then give the permit
    /// back (another dequeuer will redeem it) and signal the caller to re-acquire. A NARROW
    /// (allowSteal=false) miss is expected whenever the task lives in a remote shard, so it uses
    /// far fewer rounds before returning the permit.
    std::optional<TaskType> redeemPermitBlocking(const size_t ownShard, const bool allowSteal)
    {
        const size_t rounds = allowSteal ? ValveRounds : 8;
        for (size_t round = 0; round < rounds; ++round)
        {
            if (auto task = tryScanOnce(ownShard, allowSteal))
            {
                return task;
            }
            if (statsEnabled) [[unlikely]]
            {
                statRescanRounds.fetch_add(1, std::memory_order_relaxed);
            }
            _mm_pause();
        }
        if (statsEnabled && allowSteal) [[unlikely]]
        {
            statValveTrips.fetch_add(1, std::memory_order_relaxed);
        }
        tasksAvailable.release();
        return std::nullopt;
    }

public:
    ShardedTaskQueue(const size_t numShards, const size_t admissionTaskQueueSizePerShard)
    {
        /// Each shard gets the FULL configured admission capacity (not size/numShards): a
        /// single-source query then has exactly the single-queue backpressure depth; total
        /// capacity grows with the number of CCXs actually hosting sources.
        for (size_t s = 0; s < std::max<size_t>(1, numShards); ++s)
        {
            shards.emplace_back(admissionTaskQueueSizePerShard);
            shardStats.emplace_back();
        }
    }

    ~ShardedTaskQueue()
    {
        if (statsEnabled)
        {
            const auto fast = statFast.load(std::memory_order_relaxed);
            const auto slow = statSlow.load(std::memory_order_relaxed);
            std::fprintf(
                stderr,
                "[NES_TASKQUEUE_STATS] sharded: shards=%zu acquires=%llu fast=%llu slow=%llu internalTaken=%llu "
                "rescanRounds=%llu valveTrips=%llu\n",
                shards.size(),
                static_cast<unsigned long long>(fast + slow),
                static_cast<unsigned long long>(fast),
                static_cast<unsigned long long>(slow),
                static_cast<unsigned long long>(statInternalTaken.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(statRescanRounds.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(statValveTrips.load(std::memory_order_relaxed)));
            for (size_t s = 0; s < shards.size(); ++s)
            {
                std::fprintf(
                    stderr,
                    "[NES_TASKQUEUE_STATS] shard[%zu]: enqueued=%llu takenByOwner=%llu stolen=%llu\n",
                    s,
                    static_cast<unsigned long long>(shardStats[s].enqueued.load(std::memory_order_relaxed)),
                    static_cast<unsigned long long>(shardStats[s].takenByOwner.load(std::memory_order_relaxed)),
                    static_cast<unsigned long long>(shardStats[s].stolen.load(std::memory_order_relaxed)));
            }
        }
    }

    /// Bounded blocking write into one shard (the emitter's own CCX). Semantics as
    /// TaskQueue::addAdmissionTaskBlocking: backpressures on a full shard, cancellable.
    template <typename T = TaskType>
    bool addAdmissionTaskBlocking(const std::stop_token& stoken, const size_t shard, T&& task)
    {
        auto& queue = shards[std::min(shard, shards.size() - 1)];
        while (!stoken.stop_requested())
        {
            /// NOLINTNEXTLINE(bugprone-use-after-move) no move happens if the write does not succeed. If a move happens, we return.
            if (queue.tryWriteUntil(std::chrono::steady_clock::now() + StopTokenCheckInterval, std::forward<T>(task)))
            {
                tasksAvailable.release();
                if (statsEnabled) [[unlikely]]
                {
                    shardStats[std::min(shard, shards.size() - 1)].enqueued.fetch_add(1, std::memory_order_relaxed);
                }
                return true;
            }
        }
        return false;
    }

    /// Write a Task to the global internal task queue (unbounded, always succeeds).
    template <typename T = TaskType>
    void addInternalTaskNonBlocking(T&& task)
    {
        internal.enqueue(std::forward<T>(task));
        tasksAvailable.release();
    }

    /// Blocking read: internal queue first, then own shard; remote shards only after the
    /// backlog outlives stealDelay (see above). Cancellable via the stop token; prioritizes
    /// reading over cancellation like TaskQueue.
    std::optional<TaskType> getNextTaskBlocking(const std::stop_token& stoken, const size_t ownShard)
    {
        /// Set when a held permit could not be redeemed from internal/own shard -- i.e. the
        /// backlog (if any) is foreign. Per-call state: every call dances the delay once before
        /// its (single) steal, which caps a remote worker's steal rate at ~1/stealDelay tasks/s
        /// -- built-in reluctance that keeps stealing a thin overflow tail.
        std::optional<std::chrono::steady_clock::time_point> foreignBacklogSince;
        while (true)
        {
            if (tasksAvailable.try_acquire())
            {
                if (statsEnabled) [[unlikely]]
                {
                    statFast.fetch_add(1, std::memory_order_relaxed);
                }
            }
            else
            {
                foreignBacklogSince.reset(); /// nothing pending at all -> a fresh task is not a persisting backlog
                while (!tasksAvailable.try_acquire_for(StopTokenCheckInterval))
                {
                    if (stoken.stop_requested())
                    {
                        return std::nullopt;
                    }
                }
                if (statsEnabled) [[unlikely]]
                {
                    statSlow.fetch_add(1, std::memory_order_relaxed);
                }
            }
            const auto now = std::chrono::steady_clock::now();
            const bool allowSteal = stealDelay.count() == 0 || (foreignBacklogSince && now - *foreignBacklogSince >= stealDelay);
            if (auto task = redeemPermitBlocking(ownShard, allowSteal))
            {
                return task;
            }
            /// Permit went back (narrow miss or wide valve trip). Narrow miss: remember when the
            /// foreign backlog was first seen and sleep OFF the semaphore's wake pool, so the
            /// returned permit pools up for the owner CCX's workers instead of re-waking us.
            if (stoken.stop_requested())
            {
                return std::nullopt;
            }
            if (!allowSteal)
            {
                if (!foreignBacklogSince)
                {
                    foreignBacklogSince = now;
                }
                std::this_thread::sleep_for(stealDelay);
            }
        }
    }

    /// Non-blocking read (termination drain): rescans unboundedly once a permit is held -- the
    /// permit proves a task exists, and the drain loop stops at the first nullopt, so bailing
    /// early could strand a task at shutdown. Progress is guaranteed by permit conservation.
    std::optional<TaskType> getNextTaskNonBlocking(const size_t ownShard)
    {
        if (!tasksAvailable.try_acquire())
        {
            return std::nullopt;
        }
        while (true)
        {
            if (auto task = tryScanOnce(ownShard, /*allowSteal=*/true))
            {
                return task;
            }
            _mm_pause();
        }
    }
};
}
