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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <semaphore>
#include <stop_token>
#include <utility>
#include <vector>
#include <SchedulingStrategy.hpp>
#include <folly/MPMCQueue.h>
#include <folly/concurrency/UnboundedQueue.h>

namespace NES
{

/// MultiQueueTaskQueue provides per-worker-thread task queues with configurable assignment policies.
/// Each worker thread has its own unbounded internal queue and semaphore.
/// The shared bounded admission queue remains global for backpressure from sources.
template <typename TaskType>
class MultiQueueTaskQueue
{
    std::vector<std::unique_ptr<folly::UMPMCQueue<TaskType, true>>> threadQueues;
    std::vector<std::unique_ptr<std::counting_semaphore<>>> threadSemaphores;
    folly::MPMCQueue<TaskType> admission;

    SchedulingStrategy strategy;
    bool workStealingEnabled;
    std::atomic<size_t> roundRobinCounter{0};
    size_t numQueues;

    /// Lock-free approximate queue sizes for PER_THREAD_SMALLEST_QUEUE.
    /// Maintained via atomic increments/decrements on enqueue/dequeue instead of calling .size() under a lock.
    std::vector<std::atomic<int64_t>> approxQueueSizes;

    static constexpr std::chrono::milliseconds StopTokenCheckInterval{100};

    size_t selectTargetQueue()
    {
        if (strategy == SchedulingStrategy::PER_THREAD_ROUND_ROBIN)
        {
            return roundRobinCounter.fetch_add(1, std::memory_order_relaxed) % numQueues;
        }

        /// PER_THREAD_SMALLEST_QUEUE: find the queue with the fewest pending tasks using lock-free approximate sizes
        size_t bestIndex = 0;
        auto bestSize = std::numeric_limits<int64_t>::max();
        for (size_t i = 0; i < numQueues; ++i)
        {
            const auto sz = approxQueueSizes[i].load(std::memory_order_relaxed);
            if (sz < bestSize)
            {
                bestSize = sz;
                bestIndex = i;
            }
        }
        return bestIndex;
    }

    TaskType readElementFromThreadOrAdmission(size_t threadIndex)
    {
        TaskType task;
        /// Priority 1: own queue
        if (threadQueues[threadIndex]->try_dequeue(task))
        {
            approxQueueSizes[threadIndex].fetch_sub(1, std::memory_order_relaxed);
            return task;
        }

        /// Priority 2: steal from other threads' queues (if enabled)
        if (workStealingEnabled)
        {
            for (size_t i = 1; i < numQueues; ++i)
            {
                const auto victim = (threadIndex + i) % numQueues;
                if (threadQueues[victim]->try_dequeue(task))
                {
                    approxQueueSizes[victim].fetch_sub(1, std::memory_order_relaxed);
                    return task;
                }
            }
        }

        /// Priority 3: admission queue (semaphore guarantees at least one element exists somewhere)
        while (!admission.read(task)) [[unlikely]]
        {
        }
        return task;
    }

public:
    MultiQueueTaskQueue(size_t numThreads, size_t admissionQueueSize, SchedulingStrategy strategy, bool workStealing = false)
        : admission(admissionQueueSize), strategy(strategy), workStealingEnabled(workStealing), numQueues(numThreads),
          approxQueueSizes(numThreads)
    {
        threadQueues.reserve(numThreads);
        threadSemaphores.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i)
        {
            threadQueues.push_back(std::make_unique<folly::UMPMCQueue<TaskType, true>>());
            threadSemaphores.push_back(std::make_unique<std::counting_semaphore<>>(0));
            approxQueueSizes[i].store(0, std::memory_order_relaxed);
        }
    }

    /// External producers (sources, control messages). Writes to the shared admission queue with backpressure.
    /// Signals the target thread's semaphore so it wakes up to read from admission.
    template <typename T = TaskType>
    bool addAdmissionTaskBlocking(const std::stop_token& stoken, T&& task)
    {
        while (!stoken.stop_requested())
        {
            if (admission.tryWriteUntil(std::chrono::steady_clock::now() + StopTokenCheckInterval, std::forward<T>(task)))
            {
                const auto target = selectTargetQueue();
                threadSemaphores[target]->release();
                return true;
            }
        }
        return false;
    }

    /// Internal producers (worker threads). Dispatches to a per-thread queue based on the assignment policy.
    template <typename T = TaskType>
    void addInternalTaskNonBlocking(T&& task)
    {
        const auto target = selectTargetQueue();
        threadQueues[target]->enqueue(std::forward<T>(task));
        approxQueueSizes[target].fetch_add(1, std::memory_order_relaxed);
        threadSemaphores[target]->release();
    }

    /// Same as addInternalTaskNonBlocking but explicitly named for use from non-worker threads
    /// (e.g., DelayedTaskSubmitter) where the "internal" naming would be confusing.
    template <typename T = TaskType>
    void addTaskByPolicy(T&& task)
    {
        addInternalTaskNonBlocking(std::forward<T>(task));
    }

    /// Blocking read for a specific worker thread. Reads from its own per-thread queue first,
    /// then falls back to the shared admission queue. Cancellable via stop token.
    std::optional<TaskType> getNextTaskBlocking(size_t threadIndex, const std::stop_token& stoken)
    {
        while (!threadSemaphores[threadIndex]->try_acquire_for(StopTokenCheckInterval))
        {
            if (stoken.stop_requested())
            {
                return std::nullopt;
            }
        }

        return readElementFromThreadOrAdmission(threadIndex);
    }

    /// Non-blocking read from a specific thread's queue (used during termination).
    std::optional<TaskType> getNextTaskNonBlocking(size_t threadIndex)
    {
        if (!threadSemaphores[threadIndex]->try_acquire())
        {
            return std::nullopt;
        }

        return readElementFromThreadOrAdmission(threadIndex);
    }

    /// Non-blocking drain from any queue (used by terminator thread to pick up stragglers).
    std::optional<TaskType> getNextTaskFromAnyNonBlocking()
    {
        for (size_t i = 0; i < numQueues; ++i)
        {
            if (threadSemaphores[i]->try_acquire())
            {
                return readElementFromThreadOrAdmission(i);
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] size_t getQueueSize(size_t threadIndex) const { return threadQueues[threadIndex]->size(); }

    [[nodiscard]] size_t getNumQueues() const { return numQueues; }
};

}
