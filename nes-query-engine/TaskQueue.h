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
#include <optional>
#include <semaphore>
#include <folly/MPMCQueue.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <ErrorHandling.hpp>

namespace NES
{

template <typename TaskType>
class TaskQueue
{
    folly::UnboundedQueue<TaskType, false, false, true> internal;
    folly::MPMCQueue<TaskType> admission;

    std::counting_semaphore<> baton{0};


    static constexpr std::chrono::milliseconds StopTokenCheckInterval{100};
    static constexpr size_t AdmissionQueueReadBatchSize{100};

    void batchTransferAdmissionToInternal()
    {
        for (size_t i = 0; i < AdmissionQueueReadBatchSize; i++)
        {
            TaskType admissionTask;
            if (!admission.read(admissionTask))
            {
                break;
            }
            internal.enqueue(std::move(admissionTask));
        }
    }

public:
    explicit TaskQueue(size_t admissionTaskQueueSize) : admission(admissionTaskQueueSize) { }

    template <typename T = TaskType>
    void admissionTask(const std::stop_token& stoken, T&& task)
    {
        while (!stoken.stop_requested())
        {
            if (admission.tryWriteUntil(std::chrono::steady_clock::now() + StopTokenCheckInterval, std::forward<T>(task)))
            {
                baton.release();
                return;
            }
        }
    }

    template <typename T = TaskType>
    void internalTask(T&& task)
    {
        internal.enqueue(std::forward<T>(task));
        baton.release();
    }

    std::optional<TaskType> nextTask(const std::stop_token& stoken)
    {
        while (!baton.try_acquire_for(StopTokenCheckInterval))
        {
            if (stoken.stop_requested())
            {
                return std::nullopt;
            }
        }

        TaskType task;
        if (internal.try_dequeue(task))
        {
            return task;
        }

        if (admission.read(task))
        {
            return task;
        }

        INVARIANT(false, "the semaphore should have prevented a scenario where both queues are empty");
        std::unreachable();
    }

    std::optional<TaskType> tryNextTask()
    {
        if (!baton.try_acquire())
        {
            return std::nullopt;
        }

        TaskType task;
        if (internal.try_dequeue(task))
        {
            return task;
        }

        if (admission.read(task))
        {
            return task;
        }

        INVARIANT(false, "the semaphore should have prevented a scenario where both queues are empty");
        std::unreachable();
    }
};
}
