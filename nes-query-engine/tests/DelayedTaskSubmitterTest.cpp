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

#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <RunningQueryPlan.hpp>
#include <Task.hpp>

namespace NES::Testing
{
class DelayedTaskSubmitterTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DelayedTaskSubmitterTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup DelayedTaskSubmitterTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

protected:
    mutable std::vector<Task> submittedTasks;
    mutable std::mutex submittedTasksMutex;
    mutable std::atomic<int> taskCounter{0};

    /// For tracking execution order in tests
    mutable std::vector<int> executionOrder;
    mutable std::mutex executionOrderMutex;

    void submitTask(Task task) const noexcept
    {
        std::lock_guard<std::mutex> lock(submittedTasksMutex);
        submittedTasks.push_back(std::move(task));
        taskCounter++;
    }

    void clearSubmittedTasks()
    {
        std::lock_guard<std::mutex> lock(submittedTasksMutex);
        submittedTasks.clear();
        taskCounter = 0;
    }

    size_t getSubmittedTaskCount()
    {
        std::lock_guard<std::mutex> lock(submittedTasksMutex);
        return submittedTasks.size();
    }
};

TEST_F(DelayedTaskSubmitterTest, testBasicTaskSubmission)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    /// Create a simple task
    auto task = WorkTask(LocalQueryId(1), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Submit task with immediate execution
    submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(10));

    /// Wait for task to be executed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    ASSERT_EQ(getSubmittedTaskCount(), 1);
}

TEST_F(DelayedTaskSubmitterTest, testMultipleTasksOrderedExecution)
{
    /// Clear execution order for this test
    {
        std::lock_guard<std::mutex> lock(executionOrderMutex);
        executionOrder.clear();
    }

    auto submitter = DelayedTaskSubmitter(
        [this](Task task) noexcept
        {
            submitTask(std::move(task));

            /// Track execution order through the submitFn callback
            if (std::holds_alternative<WorkTask>(task))
            {
                const auto& workTask = std::get<WorkTask>(task);
                std::lock_guard<std::mutex> lock(executionOrderMutex);
                executionOrder.push_back(workTask.queryId.getRawValue());
            }
        });

    /// Submit tasks with different delays
    auto task1 = WorkTask(LocalQueryId(1), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto task2 = WorkTask(LocalQueryId(2), PipelineId(2), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto task3 = WorkTask(LocalQueryId(3), PipelineId(3), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Submit in reverse order with increasing delays
    submitter.submitTaskIn(std::move(task3), std::chrono::milliseconds(30));
    submitter.submitTaskIn(std::move(task2), std::chrono::milliseconds(20));
    submitter.submitTaskIn(std::move(task1), std::chrono::milliseconds(10));

    /// Wait for all tasks to be executed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ASSERT_EQ(getSubmittedTaskCount(), 3);

    /// Check execution order
    {
        std::lock_guard<std::mutex> lock(executionOrderMutex);
        ASSERT_EQ(executionOrder.size(), 3);
        ASSERT_EQ(executionOrder[0], 1); /// Should execute first (10ms delay)
        ASSERT_EQ(executionOrder[1], 2); /// Should execute second (20ms delay)
        ASSERT_EQ(executionOrder[2], 3); /// Should execute third (30ms delay)
    }
}

TEST_F(DelayedTaskSubmitterTest, testTaskWithZeroDelay)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    auto task = WorkTask(LocalQueryId(1), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Submit task with zero delay
    submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(0));

    /// Wait a bit for task to be executed
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    ASSERT_EQ(getSubmittedTaskCount(), 1);
}

TEST_F(DelayedTaskSubmitterTest, testTaskWithLongDelay)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    auto task = WorkTask(LocalQueryId(1), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Submit task with longer delay
    submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(100));

    /// Check that task is not executed immediately
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_EQ(getSubmittedTaskCount(), 0);

    /// Wait for task to be executed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(getSubmittedTaskCount(), 1);
}

TEST_F(DelayedTaskSubmitterTest, testDifferentDurationTypes)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    auto task1 = WorkTask(LocalQueryId(1), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto task2 = WorkTask(LocalQueryId(2), PipelineId(2), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto task3 = WorkTask(LocalQueryId(3), PipelineId(3), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Test different duration types
    submitter.submitTaskIn(std::move(task1), std::chrono::microseconds(10000)); /// 10ms
    submitter.submitTaskIn(std::move(task2), std::chrono::milliseconds(10)); /// 10ms
    submitter.submitTaskIn(std::move(task3), std::chrono::seconds(0)); /// 0s

    /// Wait for all tasks to be executed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    ASSERT_EQ(getSubmittedTaskCount(), 3);
}

TEST_F(DelayedTaskSubmitterTest, testConcurrentTaskSubmission)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    std::vector<std::jthread> threads;
    const int numThreads = 10;
    const int tasksPerThread = 5;

    /// Create multiple threads submitting tasks concurrently
    for (int i = 0; i < numThreads; ++i)
    {
        threads.emplace_back(
            [&submitter, i]()
            {
                for (int j = 0; j < tasksPerThread; ++j)
                {
                    auto task = WorkTask(
                        LocalQueryId(i * tasksPerThread + j),
                        PipelineId(i * tasksPerThread + j),
                        std::weak_ptr<RunningQueryPlanNode>(),
                        TupleBuffer(),
                        {});
                    submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(10));
                }
            });
    }

    /// jthread automatically joins on destruction, no explicit join needed

    /// Wait for all tasks to be executed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_EQ(getSubmittedTaskCount(), numThreads * tasksPerThread);
}

TEST_F(DelayedTaskSubmitterTest, testTaskPriorityQueueOrder)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    std::vector<int> executionOrder;
    std::mutex orderMutex;

    /// Submit tasks with the same delay but in different order
    auto task1 = WorkTask(
        LocalQueryId(1),
        PipelineId(1),
        std::weak_ptr<RunningQueryPlanNode>(),
        TupleBuffer(),
        TaskCallback(TaskCallback::OnComplete(
            [&executionOrder, &orderMutex]
            {
                std::lock_guard<std::mutex> lock(orderMutex);
                executionOrder.push_back(1);
            })));

    auto task2 = WorkTask(
        LocalQueryId(2),
        PipelineId(2),
        std::weak_ptr<RunningQueryPlanNode>(),
        TupleBuffer(),
        TaskCallback(TaskCallback::OnComplete(
            [&executionOrder, &orderMutex]
            {
                std::lock_guard<std::mutex> lock(orderMutex);
                executionOrder.push_back(2);
            })));

    /// Submit task2 first, then task1 with same delay
    submitter.submitTaskIn(std::move(task2), std::chrono::milliseconds(20));
    submitter.submitTaskIn(std::move(task1), std::chrono::milliseconds(20));

    /// Wait for tasks to be executed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_EQ(getSubmittedTaskCount(), 2);
    /// Both tasks should be executed, order may vary due to same deadline
}

TEST_F(DelayedTaskSubmitterTest, testDestructorCleanup)
{
    std::atomic<int> submittedCount{0};
    std::atomic<int> failureCount{0};
    std::atomic<int> completeCount{0};

    {
        auto submitter = DelayedTaskSubmitter([&submittedCount](Task /*task*/) noexcept { submittedCount++; });

        /// Submit a task with long delay and custom onComplete and onFailure callbacks
        auto task = WorkTask(
            LocalQueryId(1),
            PipelineId(1),
            std::weak_ptr<RunningQueryPlanNode>(),
            TupleBuffer(),
            TaskCallback(
                TaskCallback::OnComplete([&completeCount] { completeCount++; }),
                TaskCallback::OnFailure([&failureCount](Exception) { failureCount++; })));
        submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(1000));

        /// Destructor should be called here, cleaning up pending tasks
    }

    /// Task should not be executed since submitter was destroyed, but the failure callback should be called
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(submittedCount.load(), 0);
    EXPECT_EQ(completeCount.load(), 0);
    EXPECT_EQ(failureCount.load(), 1);
}

TEST_F(DelayedTaskSubmitterTest, testDifferentTaskTypes)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    /// Test different task types
    auto workTask = WorkTask(LocalQueryId(1), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto startPipelineTask = StartPipelineTask(LocalQueryId(2), PipelineId(2), {}, std::weak_ptr<RunningQueryPlanNode>());
    auto stopSourceTask = StopSourceTask(LocalQueryId(3), std::weak_ptr<RunningSource>(), {});

    submitter.submitTaskIn(std::move(workTask), std::chrono::milliseconds(10));
    submitter.submitTaskIn(std::move(startPipelineTask), std::chrono::milliseconds(10));
    submitter.submitTaskIn(std::move(stopSourceTask), std::chrono::milliseconds(10));

    /// Wait for all tasks to be executed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_EQ(getSubmittedTaskCount(), 3);
}

TEST_F(DelayedTaskSubmitterTest, testStressRandomTasks)
{
    constexpr int NumThreads = 10;
    constexpr int TasksPerThread = 20;
    constexpr int TotalTasks = NumThreads * TasksPerThread;
    constexpr int MaxDelayMs = 50;

    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    {
        std::vector<std::jthread> threads;

        /// Create multiple threads submitting random tasks with random delays
        for (int threadId = 0; threadId < NumThreads; ++threadId)
        {
            threads.emplace_back(
                [&submitter, threadId]()
                {
                    /// Each thread gets its own random number generator
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<> dis(0, MaxDelayMs);

                    for (int i = 0; i < TasksPerThread; ++i)
                    {
                        auto task = WorkTask(
                            LocalQueryId(threadId * TasksPerThread + i),
                            PipelineId(threadId * TasksPerThread + i),
                            std::weak_ptr<RunningQueryPlanNode>(),
                            TupleBuffer(),
                            {});

                        /// Random delay between 0 and MaxDelayMs milliseconds
                        int randomDelay = dis(gen);
                        submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(randomDelay));
                    }
                });
        }

        /// threads vector goes out of scope here, automatically joining all threads
    }

    /// Now wait for the maximum delay to ensure all tasks have been processed
    std::this_thread::sleep_for(std::chrono::milliseconds(MaxDelayMs + 50));

    /// Verify all tasks were submitted
    ASSERT_EQ(getSubmittedTaskCount(), TotalTasks);
}

}
