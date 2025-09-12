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

#include <TaskQueue.hpp>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <new>
#include <optional>
#include <random>
#include <stop_token>
#include <thread>
#include <unordered_set>
#include <vector>
#include <gtest/gtest.h>

namespace NES
{

class TaskQueueTest : public ::testing::Test
{
protected:
    using Task = std::pair<int, int>; /// {thread_id, sequence_number}
    TaskQueue<Task> queue{100};

    template <size_t NumberOfConsumers>
    struct ConsumedTasks
    {
        struct alignas(std::hardware_constructive_interference_size) LocalCounter
        {
            std::unordered_set<Task> tasks;

            void add(Task&& task) { ASSERT_TRUE(tasks.emplace(std::move(task)).second) << "Duplicate task found"; }
        };

        void verifyUnique()
        {
            std::unordered_set<Task> merged;
            for (auto& counter : localCounters)
            {
                size_t oldSize = merged.size();
                merged.insert(counter.tasks.begin(), counter.tasks.end());
                ASSERT_EQ(oldSize + counter.tasks.size(), merged.size());
            }
        }

        size_t size()
        {
            size_t size = 0;
            for (auto& counter : localCounters)
            {
                size += counter.tasks.size();
            }
            return size;
        }

        std::array<LocalCounter, NumberOfConsumers> localCounters{};
    };
};

TEST_F(TaskQueueTest, HighContentionStressTest)
{
    constexpr int numProducers = 8;
    constexpr int numConsumers = 8;
    constexpr int tasksPerProducer = 10000;
    constexpr int totalTasks = numProducers * tasksPerProducer;
    constexpr int totalThreads = numProducers + numConsumers;

    /// Barrier to synchronize all threads to start at the same time
    std::barrier syncBarrier{totalThreads};

    /// Track all consumed tasks to verify no duplicates/losses
    ConsumedTasks<numConsumers> consumedTasks;

    /// Producers (mix internal and admission tasks)
    std::vector<std::jthread> producers;
    producers.reserve(numProducers);
    for (int producerId = 0; producerId < numProducers; ++producerId)
    {
        producers.emplace_back(
            [&, producerId]()
            {
                const std::stop_source stopSource;
                std::mt19937 rng(producerId);
                std::uniform_int_distribution dist(0, 2);

                /// Wait for all threads to be ready before starting
                syncBarrier.arrive_and_wait();

                for (int i = 0; i < tasksPerProducer; ++i)
                {
                    Task task{producerId, i};

                    /// Randomly choose between internal and admission
                    if (dist(rng) == 0)
                    {
                        queue.internalTask(task);
                    }
                    else
                    {
                        queue.admissionTask(stopSource.get_token(), task);
                    }
                }
            });
    }

    /// Consumers (mix nextTask and tryNextTask)
    std::vector<std::jthread> consumers;
    consumers.reserve(numConsumers);
    for (int consumerId = 0; consumerId < numConsumers; ++consumerId)
    {
        consumers.emplace_back(
            [&, consumerId](const std::stop_token& stoken)
            {
                const std::stop_source stopSource;
                std::mt19937 rng(consumerId + 100);
                std::uniform_int_distribution dist(0, 1);

                /// Wait for all threads to be ready before starting
                syncBarrier.arrive_and_wait();
                while (!stoken.stop_requested())
                {
                    std::optional<Task> task;

                    /// Randomly choose consumption method
                    if (dist(rng) == 0)
                    {
                        task = queue.nextTask(stoken);
                    }
                    else
                    {
                        task = queue.tryNextTask();
                        if (!task)
                        {
                            std::this_thread::yield();
                            continue;
                        }
                    }

                    if (task)
                    {
                        consumedTasks.localCounters.at(consumerId).add(std::move(*task));
                    }
                }
            });
    }

    /// All threads will start simultaneously when the barrier is reached
    /// Wait for all threads to complete their work
    producers.clear();
    consumers.clear();

    /// Drain remaining tasks
    while (auto task = queue.tryNextTask())
    {
        consumedTasks.localCounters.back().add(std::move(*task));
    }

    /// Verify all tasks were consumed
    EXPECT_EQ(consumedTasks.size(), totalTasks);
    consumedTasks.verifyUnique();
}

TEST_F(TaskQueueTest, SpuriousFailureResilience)
{
    constexpr int numProducers = 4;
    constexpr int numConsumers = 12;
    constexpr std::chrono::milliseconds testDuration{1000};

    std::atomic tasksAdded{0};
    ConsumedTasks<numConsumers> consumedTasks;

    /// Barrier to synchronize all threads to start at the same time
    std::barrier syncBarrier{numConsumers + numProducers};

    /// Producers - rapid fire tasks
    std::vector<std::jthread> producers;
    producers.reserve(numProducers);
    for (int i = 0; i < numProducers; ++i)
    {
        producers.emplace_back(
            [&, i](const std::stop_token& stoken)
            {
                /// Wait for all threads to be ready before starting
                syncBarrier.arrive_and_wait();

                int count = 0;
                while (queue.admissionTask(stoken, Task{i, count++}))
                {
                }
                tasksAdded.fetch_add(count - 1, std::memory_order::relaxed);
            });
    }

    /// Consumers - try to trigger spurious failures
    std::vector<std::jthread> consumers;
    consumers.reserve(numConsumers);
    for (int i = 0; i < numConsumers; ++i)
    {
        consumers.emplace_back(
            [&, i](const std::stop_token& stoken)
            {
                /// Wait for all threads to be ready before starting
                syncBarrier.arrive_and_wait();

                int count = 0;
                while (!stoken.stop_requested())
                {
                    if (auto task = queue.nextTask(stoken))
                    {
                        if (task->first < numProducers && task->second % 2 == 0)
                        {
                            queue.internalTask(Task{i + numProducers, count++});
                        }
                        consumedTasks.localCounters.at(i).add(std::move(*task));
                    }
                }
                tasksAdded.fetch_add(count, std::memory_order::relaxed);
            });
    }

    /// Run for fixed duration
    std::this_thread::sleep_for(testDuration);

    /// Wait for all threads to complete their work
    producers.clear();
    consumers.clear();

    /// Drain remaining tasks
    while (auto task = queue.tryNextTask())
    {
        consumedTasks.localCounters.back().add(std::move(*task));
    }

    /// Some tasks might still be in flight, but retrieved should never exceed added
    EXPECT_EQ(consumedTasks.size(), tasksAdded.load());
    consumedTasks.verifyUnique();
}

}
