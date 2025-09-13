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
#include <thread>
#include <utility>
#include <vector>
#include <absl/functional/any_invocable.h>
#include <ErrorHandling.hpp>
#include <Task.hpp>

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

    SubmitFn submitFn;

    std::mutex mutex;
    std::condition_variable_any cv;
    std::priority_queue<ScheduledTask, std::vector<ScheduledTask>, TaskComparator> taskQueue;

    std::jthread workerThread;

    void workerLoop(const std::stop_token& stop);

public:
    explicit DelayedTaskSubmitter(SubmitFn submitFn);

    /// Template function to accept any std::chrono::duration type
    template <typename Rep, typename Period>
    void submitTaskIn(Task task, std::chrono::duration<Rep, Period> delay)
    {
        auto deadline = ClockType::now() + delay;

        const std::lock_guard lock(mutex);
        bool wasEmpty = taskQueue.empty();
        bool isEarliest = wasEmpty || deadline < taskQueue.top().deadline;

        taskQueue.emplace(ScheduledTask{std::move(task), deadline});

        /// Wake up the worker thread if this task has an earlier deadline
        if (isEarliest)
        {
            cv.notify_one();
        }
    }

    /// Non-copyable and non-movable
    DelayedTaskSubmitter(const DelayedTaskSubmitter&) = delete;
    DelayedTaskSubmitter& operator=(const DelayedTaskSubmitter&) = delete;
    DelayedTaskSubmitter(DelayedTaskSubmitter&&) = delete;
    DelayedTaskSubmitter& operator=(DelayedTaskSubmitter&&) = delete;

    ~DelayedTaskSubmitter();
};

template <typename CT>
DelayedTaskSubmitter<CT>::DelayedTaskSubmitter(SubmitFn submitFn)
    : submitFn(std::move(submitFn))
    , workerThread([](const std::stop_token& stopToken, DelayedTaskSubmitter* self) { self->workerLoop(stopToken); }, this)
{
    static_assert(std::is_member_function_pointer_v<decltype(&DelayedTaskSubmitter::workerLoop)>);
}

template <typename CT>
void DelayedTaskSubmitter<CT>::workerLoop(const std::stop_token& stop)
{
    std::unique_lock lock(mutex);

    while (!stop.stop_requested())
    {
        if (taskQueue.empty())
        {
            /// Wait for tasks to be added or shutdown signal
            cv.wait(lock, stop, [this] { return !taskQueue.empty(); });
            continue;
        }

        auto now = ClockType::now();
        const auto& nextTask = taskQueue.top();

        if (nextTask.deadline <= now)
        {
            /// Task deadline has arrived - execute it
            auto task = std::move(const_cast<ScheduledTask&>(nextTask).task);
            taskQueue.pop();

            /// Release lock while calling submit function to avoid blocking other operations
            lock.unlock();
            submitFn(std::move(task));
            lock.lock();
        }
        else
        {
            /// Wait until the next task deadline or until notified of new more urgent task
            cv.wait_until(
                lock,
                stop,
                nextTask.deadline,
                [this, &nextTask] { return !taskQueue.empty() && taskQueue.top().deadline < nextTask.deadline; });
        }
    }
}

template <typename CT>
DelayedTaskSubmitter<CT>::~DelayedTaskSubmitter()
{
    /// Signal shutdown and wake up worker threads.
    /// Throw away all pending tasks, as the engine is about to shutdown and trying to actually execute these tasks is unlikely to
    /// succeed, in addition to potentially creating infinite cycles.
    workerThread.request_stop();
    const std::lock_guard lock(mutex);
    while (!taskQueue.empty())
    {
        auto task = std::move(const_cast<ScheduledTask&>(taskQueue.top()).task);
        taskQueue.pop();
        failTask(task, SkippingDelayedTaskDuringShutdown());
    }
}
}
