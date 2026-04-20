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
#include <coroutine>
#include <cstddef>
#include <optional>
#include <semaphore>
#include <stop_token>
#include <utility>

#include <absl/functional/any_invocable.h>
#include <coro/concepts/awaitable.hpp>
#include <coro/task.hpp>
#include <folly/MPMCQueue.h>
#include <folly/concurrency/UnboundedQueue.h>

#include "ittnotify.h"

namespace NES
{
static __itt_domain* taskQueueDomain = __itt_domain_create("engine.taskqueue");
static __itt_string_handle* blockingRead = __itt_string_handle_create("Blocking Read");
static __itt_string_handle* blockingWrite = __itt_string_handle_create("Blocking Write");
static __itt_counter queue_depth = __itt_counter_create("Depth", "engine.taskqueue");
static std::atomic<size_t> queue_depth_value{0};
#define decrease_count() \
    do \
    { \
        auto depth = queue_depth_value.fetch_sub(1, std::memory_order_relaxed) - 1; \
        __itt_counter_set_value(queue_depth, &depth); \
    } while (false)
#define increase_count() \
    do \
    { \
        auto depth = queue_depth_value.fetch_add(1, std::memory_order_relaxed) + 1; \
        __itt_counter_set_value(queue_depth, &depth); \
    } while (false)

using WakerCallback = absl::AnyInvocable<bool()>;

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
    folly::UMPMCQueue<WakerCallback, false> waker;

    /// INVARIANT: internal.size() + admission.size() >= tasksAvailable
    std::counting_semaphore<> tasksAvailable{0};

    /// To provide cancellation, we only block for StopTokenCheckInterval.
    /// This parameter could be tuned to allow for more timely cancellation
    static constexpr std::chrono::milliseconds StopTokenCheckInterval{1};

    TaskType readElementAssumingItExists()
    {
        decrease_count();
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

        WakerCallback wakerCallback;
        while (waker.try_dequeue(wakerCallback))
        {
            if (wakerCallback())
            {
                break;
            }
        }

        return task;
    }

public:
    struct SuspendUntilSignaled
    {
        TaskQueue& queue;

        bool await_ready() noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h) noexcept
        {
            queue.waker.enqueue(
                [h]()
                {
                    h.resume();
                    return true;
                });
        }

        void await_resume() noexcept { }
    };

    static_assert(coro::concepts::awaitable<SuspendUntilSignaled>);

    explicit TaskQueue(size_t admissionTaskQueueSize) : admission(admissionTaskQueueSize) { }

    /// NonBlocking Async Interface
    template <typename T = TaskType>
    coro::task<> addAdmissionTaskAsync(T&& task)
    {
        while (true)
        {
            if (admission.write(std::forward<T>(task)))
            {
                increase_count();
                tasksAvailable.release();
                co_return;
            }
            co_await SuspendUntilSignaled{*this};
        }
    }

    /// By design the admission queue is bounded, which could lead to writes being blocked.
    /// The stop token allows cancellation. In case the writing was canceled, this method returns false.
    template <typename T = TaskType>
    bool addAdmissionTaskBlocking(const std::stop_token& stoken, T&& task)
    {
        /// fast path
        if (stoken.stop_requested())
        {
            return false;
        }

        if (admission.write(std::forward<T>(task)))
        {
            increase_count();
            tasksAvailable.release();
            return true; }

        /// blocking write
        __itt_task_begin(taskQueueDomain, __itt_null, __itt_null, blockingWrite);
        SCOPE_EXIT
        {
            __itt_task_end(taskQueueDomain);
        };
        while (!stoken.stop_requested())
        {
            /// The order of operation upholds the invariant
            /// NOLINTNEXTLINE(bugprone-use-after-move) no move happens if the write does not succeed. If a move happens, we return.
            if (admission.tryWriteUntil(std::chrono::steady_clock::now() + StopTokenCheckInterval, std::forward<T>(task)))
            {
                /// tasksAvailable is only increased if write to admission queue was successful.
                increase_count();
                tasksAvailable.release();
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
        increase_count();
        tasksAvailable.release();
    }

    /// Blocking read to retrieve the next task from the internal queue, or the admission queue if the internal task queue is empty.
    /// This operation can be canceled using a stop token. In case of a cancellation, this method returns an empty optional.
    /// The method prioritizes reading over cancellation. This implies, if a read is non-blocking, it succeeds regardless of the state of
    /// the stop token.
    std::optional<TaskType> getNextTaskBlocking(const std::stop_token& stoken)
    {
        __itt_task_begin(taskQueueDomain, __itt_null, __itt_null, blockingRead);
        SCOPE_EXIT
        {
            __itt_task_end(taskQueueDomain);
        };
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

        return readElementAssumingItExists();
    }
};
}
