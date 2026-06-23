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
#include <optional>
#include <semaphore>
#include <stop_token>
#include <utility>
#include <folly/MPMCQueue.h>
#include <folly/concurrency/UnboundedQueue.h>

namespace NES
{

/// The TaskQueue is a central component within the QueryEngine. External components like sources or users of the system can add new tasks
/// to an admission queue which is bounded and will backpressure sources if necessary. Internally, WorkerThreads communicate via a shared
/// internal queue, which is unbounded to deal with occasionally bursty loads like a large join. Access to the internal task queue is always
/// non-blocking. The TaskQueue exposes a blocking `getNextTaskBlocking` method which reads from either queue without spinning and is
/// supposed to be used by the worker threads.
template <typename TaskType>
class TaskQueue
{
    folly::UMPMCQueue<TaskType, true> internal;
    folly::MPMCQueue<TaskType> admission;

    /// INVARIANT: internal.size() + admission.size() >= tasksAvailable
    std::counting_semaphore<> tasksAvailable{0};

    /// To provide cancellation, we only block for StopTokenCheckInterval.
    /// This parameter could be tuned to allow for more timely cancellation
    static constexpr std::chrono::milliseconds StopTokenCheckInterval{100};

    /// TMP DIAGNOSTIC (env NES_TASKQUEUE_STATS): split worker acquires into fast-path (queue non-empty -> immediate
    /// CAS, no spin) vs slow-path (queue empty -> wait, which is where libstdc++'s semaphore __sched_yield lives).
    /// statOutstanding mirrors the semaphore count; avgDepthOnFastAcquire = mean queued-task count seen at an
    /// immediate acquire: ~1 => producer<->worker lockstep (producer-paced), >>1 => producer runs ahead
    /// (worker-bound). Dumped to stderr in the dtor. Revert before merge.
    const bool statsEnabled{std::getenv("NES_TASKQUEUE_STATS") != nullptr};
    std::atomic<uint64_t> statFast{0};
    std::atomic<uint64_t> statSlow{0};
    std::atomic<int64_t> statOutstanding{0};
    std::atomic<uint64_t> statDepthSum{0};

    TaskType readElementAssumingItExists()
    {
        TaskType task;
        /// The semaphore guarantees that there is at least one element in either one of the queues.
        if (internal.try_dequeue(task))
        {
            return task;
        }

        /// However, the MPMC `read` can spuriously fail under high contention, the alternative `readIfNotEmpty` does not but is
        /// significantly slower.
        while (!admission.read(task)) [[unlikely]]
        {
        }

        return task;
    }

public:
    explicit TaskQueue(size_t admissionTaskQueueSize) : admission(admissionTaskQueueSize) { }

    ~TaskQueue()
    {
        if (statsEnabled)
        {
            const auto fast = statFast.load(std::memory_order_relaxed);
            const auto slow = statSlow.load(std::memory_order_relaxed);
            const auto total = fast + slow;
            const auto depthSum = statDepthSum.load(std::memory_order_relaxed);
            std::fprintf(
                stderr,
                "[NES_TASKQUEUE_STATS] acquires=%llu fast=%llu (%.2f%%) slow=%llu (%.2f%%) avgDepthOnFastAcquire=%.2f\n",
                static_cast<unsigned long long>(total),
                static_cast<unsigned long long>(fast),
                total ? 100.0 * static_cast<double>(fast) / static_cast<double>(total) : 0.0,
                static_cast<unsigned long long>(slow),
                total ? 100.0 * static_cast<double>(slow) / static_cast<double>(total) : 0.0,
                fast ? static_cast<double>(depthSum) / static_cast<double>(fast) : 0.0);
        }
    }

    /// By design the admission queue is bounded, which could lead to writes being blocked.
    /// The stop token allows cancellation. In case the writing was canceled, this method returns false.
    template <typename T = TaskType>
    bool addAdmissionTaskBlocking(const std::stop_token& stoken, T&& task)
    {
        while (!stoken.stop_requested())
        {
            /// The order of operation upholds the invariant
            /// NOLINTNEXTLINE(bugprone-use-after-move) no move happens if the write does not succeed. If a move happens, we return.
            if (admission.tryWriteUntil(std::chrono::steady_clock::now() + StopTokenCheckInterval, std::forward<T>(task)))
            {
                /// tasksAvailable is only increased if write to admission queue was successful.
                tasksAvailable.release();
                if (statsEnabled) [[unlikely]]
                {
                    statOutstanding.fetch_add(1, std::memory_order_relaxed);
                }
                return true;
            }
        }
        return false;
    }

    /// Write a Task to the internal task queue. The internal task queue is unbounded thus this operation will always succeed
    template <typename T = TaskType>
    void addInternalTaskNonBlocking(T&& task)
    {
        /// The order of operation upholds the invariant. internal is unbounded which makes this write always succeed (unless oom)
        internal.enqueue(std::forward<T>(task));
        tasksAvailable.release();
        if (statsEnabled) [[unlikely]]
        {
            statOutstanding.fetch_add(1, std::memory_order_relaxed);
        }
    }

    /// Blocking read to retrieve the next task from the internal queue, or the admission queue if the internal task queue is empty.
    /// This operation can be canceled using a stop token. In case of a cancellation, this method returns an empty optional.
    /// The method prioritizes reading over cancellation. This implies, if a read is non-blocking, it succeeds regardless of the state of
    /// the stop token.
    std::optional<TaskType> getNextTaskBlocking(const std::stop_token& stoken)
    {
        if (statsEnabled) [[unlikely]]
        {
            /// try_acquire() never spins/yields: success => count was > 0 (queue non-empty) = fast path.
            if (tasksAvailable.try_acquire())
            {
                statFast.fetch_add(1, std::memory_order_relaxed);
                const auto depthBefore = statOutstanding.fetch_sub(1, std::memory_order_relaxed);
                statDepthSum.fetch_add(depthBefore > 0 ? static_cast<uint64_t>(depthBefore) : 0, std::memory_order_relaxed);
                return readElementAssumingItExists();
            }
            /// count was 0: we have to wait -- this is the spin/__sched_yield/futex path.
            statSlow.fetch_add(1, std::memory_order_relaxed);
            while (!tasksAvailable.try_acquire_for(StopTokenCheckInterval))
            {
                if (stoken.stop_requested())
                {
                    return std::nullopt;
                }
            }
            statOutstanding.fetch_sub(1, std::memory_order_relaxed);
            return readElementAssumingItExists();
        }

        while (!tasksAvailable.try_acquire_for(StopTokenCheckInterval))
        {
            if (stoken.stop_requested())
            {
                return std::nullopt;
            }
        }

        return readElementAssumingItExists();
    }

    /// Non-Blocking version of `getNextTaskBlocking` if the queue is empty, this method returns an empty optional.
    std::optional<TaskType> getNextTaskNonBlocking()
    {
        if (!tasksAvailable.try_acquire())
        {
            return std::nullopt;
        }

        if (statsEnabled) [[unlikely]]
        {
            statOutstanding.fetch_sub(1, std::memory_order_relaxed);
        }

        return readElementAssumingItExists();
    }
};
}
