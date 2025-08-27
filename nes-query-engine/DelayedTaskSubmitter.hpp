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
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <absl/functional/any_invocable.h>
#include <NESThread.hpp>
#include <Task.hpp>

namespace NES
{
class DelayedTaskSubmitter
{
public:
    using SubmitFn = absl::AnyInvocable<void(Task) const noexcept>;

private:
    struct ScheduledTask
    {
        Task task;
        std::chrono::steady_clock::time_point deadline;
    };

    /// Comparator for priority queue - earlier deadlines have higher priority
    struct TaskComparator
    {
        bool operator()(const ScheduledTask& a, const ScheduledTask& b) const { return a.deadline > b.deadline; }
    };

    SubmitFn submitFn;

    std::mutex mutex;
    std::condition_variable_any cv;
    std::priority_queue<ScheduledTask, std::vector<ScheduledTask>, TaskComparator> taskQueue;

    Thread workerThread;

    void workerLoop(const std::stop_token& stop);

public:
    explicit DelayedTaskSubmitter(SubmitFn submitFn);

    /// Template function to accept any std::chrono::duration type
    template <typename Rep, typename Period>
    void submitTaskIn(Task task, std::chrono::duration<Rep, Period> delay)
    {
        auto deadline = std::chrono::steady_clock::now() + delay;

        std::lock_guard lock(mutex);
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
}
