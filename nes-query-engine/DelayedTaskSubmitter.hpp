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
#include <condition_variable>
#include <mutex>
#include <queue>
#include <stop_token>
#include <utility>
#include <vector>
#include <absl/functional/any_invocable.h>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <Task.hpp>
#include <Thread.hpp>

namespace NES
{

/// The DelayedTaskSubmitter enables the query engine to defer submission of Tasks to a future point in time.
/// This is mostly used to implement retry/repeat logic without spamming the taskqueue.
template <typename CT = std::chrono::steady_clock>
class DelayedTaskSubmitter
{
public:
    using SubmitFn = absl::AnyInvocable<void(Task) const noexcept>;
    using ClockType = CT;

private:
    struct ScheduledTask
    {
        Task task;
        typename ClockType::time_point deadline;
    };

    /// Comparator for priority queue - earlier deadlines have higher priority
    struct TaskComparator
    {
        bool operator()(const ScheduledTask& left, const ScheduledTask& right) const { return left.deadline > right.deadline; }
    };

    using TaskQueue = std::priority_queue<ScheduledTask, std::vector<ScheduledTask>, TaskComparator>;

    struct State
    {
        TaskQueue taskQueue;
        bool acceptingTasks = true;
    };

    SubmitFn submitFn;

    std::condition_variable_any cv;
    folly::Synchronized<State, std::mutex> stateMtx;

    /// The DelayedTaskSubmitter is implemented as its own dedicated thread. Most of the time is spent blocking on an empty task queue
    /// or waiting until the deadline has passed to submit the next task.
    std::once_flag shutdownOnce;
    Thread workerThread;

    void workerLoop(const std::stop_token& stop);
    static void failTaskDuringShutdown(Task& task);

public:
    explicit DelayedTaskSubmitter(SubmitFn submitFn);

    /// Template function to accept any std::chrono::duration type
    /// The task will be submitted via the submit function after a delay.
    /// If the DelayedTaskSubmitter is shutdown the task will trigger onFailure and onComplete.
    template <typename Rep, typename Period>
    void submitTaskIn(Task task, std::chrono::duration<Rep, Period> delay)
    {
        auto deadline = ClockType::now() + delay;

        auto state = stateMtx.lock();
        if (!state->acceptingTasks)
        {
            state.unlock();
            failTaskDuringShutdown(task);
            return;
        }

        const bool isEarliest = state->taskQueue.empty() || deadline < state->taskQueue.top().deadline;
        state->taskQueue.emplace(ScheduledTask{std::move(task), deadline});

        /// Wake up the worker thread if this task has an earlier deadline or if the taskqueue was empty.
        /// This is necessary, as we need to set the cv_wait_until to ensure no task being missed.
        if (isEarliest)
        {
            state.unlock();
            cv.notify_one();
        }
    }

    /// Stops the submitter thread and fails all outstanding tasks. Concurrent calls block until the single shutdown operation has
    /// completed. Tasks submitted after shutdown are failed synchronously by submitTaskIn().
    void shutdown();

    /// Non-copyable and non-movable
    DelayedTaskSubmitter(const DelayedTaskSubmitter&) = delete;
    DelayedTaskSubmitter& operator=(const DelayedTaskSubmitter&) = delete;
    DelayedTaskSubmitter(DelayedTaskSubmitter&&) = delete;
    DelayedTaskSubmitter& operator=(DelayedTaskSubmitter&&) = delete;

    /// The destructor will shutdown the DelayedTaskSubmitter failing any pending tasks.
    ~DelayedTaskSubmitter();
};

template <typename CT>
DelayedTaskSubmitter<CT>::DelayedTaskSubmitter(SubmitFn submitFn)
    : submitFn(std::move(submitFn)), workerThread("task-delayer", &DelayedTaskSubmitter::workerLoop, this)
{
}

template <typename CT>
void DelayedTaskSubmitter<CT>::workerLoop(const std::stop_token& stop)
{
    auto state = stateMtx.lock();
    while (!stop.stop_requested())
    {
        if (state->taskQueue.empty())
        {
            /// Wait for tasks to be added or shutdown signal
            cv.wait(state.as_lock(), stop, [&state] { return !state->taskQueue.empty(); });
            continue;
        }

        auto now = ClockType::now();
        const auto& nextTask = state->taskQueue.top();

        if (nextTask.deadline <= now)
        {
            /// Task deadline has arrived - execute it

            /// Priority queue's API design has a limitation where top only returns a const reference, however we have to move the task out of the queue.
            /// Since we have exclusive access to the queue, and we are removing the task from the queue anyways and the underlying memory has to be mutable, stripping away the constness is ok (I think).
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
            auto task = std::move(const_cast<ScheduledTask&>(nextTask).task);
            state->taskQueue.pop();

            /// Release lock while calling submit function to avoid blocking other operations
            state.unlock();
            submitFn(std::move(task));
            state = stateMtx.lock();
        }
        else
        {
            auto nextDeadline = nextTask.deadline;
            /// Wait until the next task deadline or until notified of new more urgent task
            cv.wait_until(
                state.as_lock(),
                stop,
                nextDeadline,
                [&state, nextDeadline] { return !state->taskQueue.empty() && state->taskQueue.top().deadline < nextDeadline; });
        }
    }
}

template <typename CT>
void DelayedTaskSubmitter<CT>::failTaskDuringShutdown(Task& task)
{
    failTask(task, SkippingDelayedTaskDuringShutdown());
    completeTask(task);
}

template <typename CT>
void DelayedTaskSubmitter<CT>::shutdown()
{
    std::call_once(
        shutdownOnce,
        [this]
        {
            {
                auto state = stateMtx.lock();
                state->acceptingTasks = false;
            }

            /// Joining guarantees that no task submission into the engine task queue is still in flight when shutdown returns.
            workerThread = {};

            /// Run callbacks without holding the state mutex. Callbacks can re-enter submitTaskIn(), which will synchronously reject the
            /// task because acceptingTasks is false.
            while (true)
            {
                auto state = stateMtx.lock();
                if (state->taskQueue.empty())
                {
                    break;
                }

                /// Priority queue's API only returns a const reference. We have exclusive access and immediately remove the element.
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
                auto task = std::move(const_cast<ScheduledTask&>(state->taskQueue.top()).task);
                state->taskQueue.pop();
                state.unlock();
                failTaskDuringShutdown(task);
            }
        });
}

template <typename CT>
DelayedTaskSubmitter<CT>::~DelayedTaskSubmitter()
{
    shutdown();
}
}
