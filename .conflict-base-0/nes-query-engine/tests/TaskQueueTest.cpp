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
#include <cstddef>
#include <new>
#include <optional>
#include <random>
#include <stop_token>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

namespace NES
{

class TaskQueueTest : public ::testing::Test
{
protected:
    /// NOLINTNEXTLINE(readability-magic-numbers) 142 is roughly the current task size
    using Task = std::tuple<int, int, std::array<std::byte, 142>>; /// {thread_id, sequence_number, payload}
    TaskQueue<Task> queue{100};

    template <size_t NumberOfConsumers>
    struct ConsumedTasks
    {
        struct alignas(std::hardware_constructive_interference_size) LocalCounter
        {
            std::unordered_set<std::pair<int, int>> tasks;

            void add(const Task& task)
            {
                ASSERT_TRUE(tasks.emplace(std::get<0>(task), std::get<1>(task)).second) << "Duplicate task found";
            }
        };

        void verifyUnique()
        {
            std::unordered_set<std::pair<int, int>> merged;
            for (auto& counter : localCounters)
            {
                const size_t oldSize = merged.size();
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

/// Intentionally limited number of WorkerThreads should provoke backpressure on the Source Threads
/// The Source Threads are instructed to create a fixed number of sources and are not listening to any cooperative stop_token,
/// which means if for what ever reason the TaskQueue deadlocks this test will fail due to the global test timeout.
/// The worker side consumes tasks using a mix of tryNextTask and nextTask.
TEST_F(TaskQueueTest, BackpressureTest)
{
    constexpr int numberOfSources = 8;
    constexpr int numberOfWorkers = 2;
    constexpr int tasksPerSource = 10000;
    constexpr int totalTasks = numberOfSources * tasksPerSource;
    constexpr int totalThreads = numberOfSources + numberOfWorkers;

    /// Track all consumed tasks to verify no duplicates/losses
    ConsumedTasks<numberOfWorkers> consumedTasks;

    /// Barrier to synchronize all threads to start at the same time
    std::barrier syncBarrier{totalThreads};

    /// Sources produce tasks added to the admission queue
    std::vector<std::jthread> sources;
    sources.reserve(numberOfSources);
    for (int sourceId = 0; sourceId < numberOfSources; ++sourceId)
    {
        sources.emplace_back(
            [&, sourceId]
            {
                /// Wait for all threads to be ready before starting
                syncBarrier.arrive_and_wait();
                for (int i = 0; i < tasksPerSource; ++i)
                {
                    queue.addAdmissionTaskBlocking({}, {sourceId, i, {}});
                }
            });
    }

    /// WorkerThreads (mix nextTask and tryNextTask)
    std::vector<std::jthread> worker;
    worker.reserve(numberOfWorkers);
    for (int workerId = 0; workerId < numberOfWorkers; ++workerId)
    {
        worker.emplace_back(
            [&, workerId](const std::stop_token& stoken)
            {
                std::mt19937 rng(workerId);
                std::uniform_int_distribution dist(0, 1);

                /// Wait for all threads to be ready before starting
                syncBarrier.arrive_and_wait();
                while (!stoken.stop_requested())
                {
                    if (auto task = dist(rng) == 0 ? queue.getNextTaskBlocking(stoken) : queue.getNextTaskNonBlocking())
                    {
                        consumedTasks.localCounters.at(workerId).add(*task);
                    }
                }
            });
    }

    /// All threads will start simultaneously when the barrier is reached
    /// Wait for all threads to complete their work
    sources.clear();
    worker.clear();

    /// Drain remaining tasks
    while (auto task = queue.getNextTaskNonBlocking())
    {
        consumedTasks.localCounters.back().add(*task);
    }

    /// Verify all tasks were consumed
    EXPECT_EQ(consumedTasks.size(), totalTasks);
    consumedTasks.verifyUnique();
}

/// This test is designed to stress the TaskQueue by injecting Tasks as fast as possible. Worker Threads can simulate occasional large(ish)
/// join results. Stopping threads relies on cooperative multitasking, which is validated via the global test timeout.
TEST_F(TaskQueueTest, StressTest)
{
    constexpr int numberOfSources = 4;
    constexpr int numberOfWorkerThreads = 12;
    constexpr std::chrono::milliseconds testDuration{1000};

    std::atomic tasksAdded{0};
    ConsumedTasks<numberOfWorkerThreads> consumedTasks;

    /// Barrier to synchronize all threads to start at the same time
    std::barrier syncBarrier{numberOfWorkerThreads + numberOfSources};

    /// Source - rapid fire tasks into admission queue
    std::vector<std::jthread> sources;
    sources.reserve(numberOfSources);
    for (int sourceId = 0; sourceId < numberOfSources; ++sourceId)
    {
        sources.emplace_back(
            [&, sourceId](const std::stop_token& stoken)
            {
                /// Wait for all threads to be ready before starting
                syncBarrier.arrive_and_wait();

                int count = 0;
                while (queue.addAdmissionTaskBlocking(stoken, Task{sourceId, count++, {}}))
                {
                }
                tasksAdded.fetch_add(count - 1, std::memory_order::relaxed);
            });
    }

    /// Workers: Simulate a very (arguably unrealistic) TaskQueue heavy workload, which stresses both: many concurrent reads aswell as
    /// infrequent bursts of writes.
    std::vector<std::jthread> worker;
    worker.reserve(numberOfWorkerThreads);
    for (int workerId = 0; workerId < numberOfWorkerThreads; ++workerId)
    {
        worker.emplace_back(
            [&, workerId](const std::stop_token& stoken)
            {
                /// Simulate occasional large joins using a geometric distribution, (a.k.a frequent zeros) which is scaled by a scalar.
                /// 0,0,0,0,6000,0,0,0,0,10000,0....
                auto numberOfFollowUpTasks = [rng = std::mt19937(workerId), distribution = std::geometric_distribution<size_t>()]() mutable
                {
                    /// There are no good reasons for these numbers, we don't have any statistics of what real work loads look like so
                    /// this is just a best effort guess.
                    constexpr auto followUpTaskScaler = 1000;
                    constexpr auto cutOff = 6;
                    const auto value = distribution(rng);
                    return value > cutOff ? value * followUpTaskScaler : 0;
                };

                /// Wait for all threads to be ready before starting
                syncBarrier.arrive_and_wait();

                int count = 0;
                while (!stoken.stop_requested())
                {
                    if (auto task = queue.getNextTaskBlocking(stoken))
                    {
                        const size_t followUpTasks = numberOfFollowUpTasks();
                        for (size_t i = 0; i < followUpTasks; ++i)
                        {
                            queue.addInternalTaskNonBlocking(Task{workerId + numberOfSources, count++, {}});
                        }
                        consumedTasks.localCounters.at(workerId).add(*task);
                    }
                }
                tasksAdded.fetch_add(count, std::memory_order::relaxed);
            });
    }

    /// Run for fixed duration
    std::this_thread::sleep_for(testDuration);

    /// Wait for all threads to complete their work
    sources.clear();
    worker.clear();

    /// Drain remaining tasks
    while (auto task = queue.getNextTaskNonBlocking())
    {
        consumedTasks.localCounters.back().add(*task);
    }

    /// Some tasks might still be in flight, but retrieved should never exceed added
    EXPECT_EQ(consumedTasks.size(), tasksAdded.load());
    consumedTasks.verifyUnique();
}

}
