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

#include <DelayedTaskSubmitter.hpp>
#include <RunningQueryPlan.hpp>

namespace NES
{

DelayedTaskSubmitter::DelayedTaskSubmitter(SubmitFn submitFn)
    : submitFn(std::move(submitFn)), workerThread("delay-task-submitter", &DelayedTaskSubmitter::workerLoop, this)
{
    static_assert(std::is_member_function_pointer_v<decltype(&DelayedTaskSubmitter::workerLoop)>);
}

void DelayedTaskSubmitter::workerLoop(const std::stop_token& stop)
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

        auto now = std::chrono::steady_clock::now();
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
            /// Wait until the next task deadline or until notified of new task
            cv.wait_until(lock, stop, nextTask.deadline, [this] { return !taskQueue.empty(); });
        }
    }
}

DelayedTaskSubmitter::~DelayedTaskSubmitter()
{
    /// Signal shutdown and wake up worker threads.
    /// Throw away all pending tasks, as the engine is about to shutdown and trying to actually execute these tasks is unlikely to
    /// succeed, in addition to potentially creating infinite cycles.
    std::lock_guard lock(mutex);
    while (!taskQueue.empty())
    {
        auto task = std::move(const_cast<ScheduledTask&>(taskQueue.top()).task);
        taskQueue.pop();
        failTask(task, SkippingDelayedTaskDuringShutdown());
    }
}

}
