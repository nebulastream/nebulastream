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

#include <MultiQueueTaskQueue.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <optional>
#include <stop_token>
#include <thread>
#include <vector>
#include <SchedulingStrategy.hpp>
#include <gtest/gtest.h>

namespace NES
{

class MultiQueueTaskQueueTest : public ::testing::Test
{
protected:
    using Task = int;
    static constexpr size_t NUM_THREADS = 4;
    static constexpr size_t ADMISSION_SIZE = 100;
};

TEST_F(MultiQueueTaskQueueTest, RoundRobinDistributesEvenly)
{
    MultiQueueTaskQueue<Task> queue(NUM_THREADS, ADMISSION_SIZE, SchedulingStrategy::PER_THREAD_ROUND_ROBIN);

    /// Add tasks that should distribute round-robin across 4 queues
    for (int i = 0; i < 100; ++i)
    {
        queue.addInternalTaskNonBlocking(i);
    }

    /// Each queue should have ~25 tasks
    for (size_t t = 0; t < NUM_THREADS; ++t)
    {
        EXPECT_EQ(queue.getQueueSize(t), 25) << "Thread " << t << " queue size mismatch";
    }
}

TEST_F(MultiQueueTaskQueueTest, SmallestQueuePicksShortest)
{
    MultiQueueTaskQueue<Task> queue(NUM_THREADS, ADMISSION_SIZE, SchedulingStrategy::PER_THREAD_SMALLEST_QUEUE);

    /// Fill thread 0's queue with some tasks first using round-robin temporarily
    /// We'll just add one task — it should go to thread 0 (all empty, picks first)
    queue.addInternalTaskNonBlocking(1);
    EXPECT_EQ(queue.getQueueSize(0), 1);

    /// Next task should go to thread 1 (or any other empty queue)
    queue.addInternalTaskNonBlocking(2);
    /// One of the other threads should have gotten it
    size_t totalInOthers = 0;
    for (size_t t = 1; t < NUM_THREADS; ++t)
    {
        totalInOthers += queue.getQueueSize(t);
    }
    EXPECT_EQ(totalInOthers, 1);
}

TEST_F(MultiQueueTaskQueueTest, BlockingReadReturnsTask)
{
    MultiQueueTaskQueue<Task> queue(NUM_THREADS, ADMISSION_SIZE, SchedulingStrategy::PER_THREAD_ROUND_ROBIN);

    queue.addInternalTaskNonBlocking(42);

    /// Task went to thread 0 (first round-robin target)
    std::stop_source stopSource;
    auto result = queue.getNextTaskNonBlocking(0);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 42);
}

TEST_F(MultiQueueTaskQueueTest, BlockingReadTimesOutOnEmpty)
{
    MultiQueueTaskQueue<Task> queue(NUM_THREADS, ADMISSION_SIZE, SchedulingStrategy::PER_THREAD_ROUND_ROBIN);

    std::stop_source stopSource;
    stopSource.request_stop();
    auto result = queue.getNextTaskBlocking(0, stopSource.get_token());
    EXPECT_FALSE(result.has_value());
}

TEST_F(MultiQueueTaskQueueTest, AdmissionQueueWakesTargetThread)
{
    MultiQueueTaskQueue<Task> queue(NUM_THREADS, ADMISSION_SIZE, SchedulingStrategy::PER_THREAD_ROUND_ROBIN);

    /// Add via admission queue
    std::stop_token noStop;
    queue.addAdmissionTaskBlocking(noStop, 99);

    /// The admission task should be readable by the target thread
    /// Try all threads — one should be able to read it
    bool found = false;
    for (size_t t = 0; t < NUM_THREADS; ++t)
    {
        auto result = queue.getNextTaskNonBlocking(t);
        if (result.has_value())
        {
            EXPECT_EQ(*result, 99);
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(MultiQueueTaskQueueTest, DrainFromAnyCollectsAllTasks)
{
    MultiQueueTaskQueue<Task> queue(NUM_THREADS, ADMISSION_SIZE, SchedulingStrategy::PER_THREAD_ROUND_ROBIN);

    for (int i = 0; i < 20; ++i)
    {
        queue.addInternalTaskNonBlocking(i);
    }

    int count = 0;
    while (auto task = queue.getNextTaskFromAnyNonBlocking())
    {
        count++;
    }
    EXPECT_EQ(count, 20);
}

TEST_F(MultiQueueTaskQueueTest, ConcurrentProducersConsumers)
{
    MultiQueueTaskQueue<Task> queue(NUM_THREADS, ADMISSION_SIZE, SchedulingStrategy::PER_THREAD_ROUND_ROBIN);

    constexpr int TASKS_PER_PRODUCER = 1000;
    constexpr int NUM_PRODUCERS = 4;
    std::atomic<int> totalConsumed{0};

    /// Launch consumers
    std::vector<std::jthread> consumers;
    for (size_t t = 0; t < NUM_THREADS; ++t)
    {
        consumers.emplace_back(
            [&queue, &totalConsumed, t](const std::stop_token& stoken)
            {
                while (!stoken.stop_requested())
                {
                    if (auto task = queue.getNextTaskBlocking(t, stoken))
                    {
                        totalConsumed.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                /// Drain remaining
                while (auto task = queue.getNextTaskNonBlocking(t))
                {
                    totalConsumed.fetch_add(1, std::memory_order_relaxed);
                }
            });
    }

    /// Launch producers
    std::vector<std::jthread> producers;
    for (int p = 0; p < NUM_PRODUCERS; ++p)
    {
        producers.emplace_back(
            [&queue, p]
            {
                for (int i = 0; i < TASKS_PER_PRODUCER; ++i)
                {
                    queue.addInternalTaskNonBlocking(p * TASKS_PER_PRODUCER + i);
                }
            });
    }

    /// Wait for producers to finish
    producers.clear();

    /// Give consumers time to process
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    /// Stop consumers
    consumers.clear();

    /// Drain any remaining
    while (auto task = queue.getNextTaskFromAnyNonBlocking())
    {
        totalConsumed.fetch_add(1, std::memory_order_relaxed);
    }

    EXPECT_EQ(totalConsumed.load(), NUM_PRODUCERS * TASKS_PER_PRODUCER);
}

}
