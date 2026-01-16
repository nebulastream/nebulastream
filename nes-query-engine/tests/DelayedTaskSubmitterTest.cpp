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
#include <cstddef>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/UUID.hpp>
#include <folly/Synchronized.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <RunningQueryPlan.hpp>
#include <Task.hpp>

namespace NES::Testing
{

/// While the test attempts to bypass handling real time by using a TestClock, this test cannot ensure that the condition variable
/// is checked. Thus, we still need to rely on real time delay to ensure that condition variable was checked.
constexpr auto REAL_TIME_DELAY = std::chrono::milliseconds(400);

/// Testing the DelayedTaskSubmitter with an actual clock is impractical. The TestClock implements
/// the std::chrono::Clock concept and is injected into the DelayedTaskSubmitter, that way the test
/// can actually control how time is passing.
struct TestClock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<TestClock, duration>;
    constexpr static bool is_steady = true;
    static folly::Synchronized<time_point> now_;

    static time_point now() noexcept { return *now_.rlock(); }

    static void advance(duration advanceBy, bool doAnActualWait)
    {
        *now_.wlock() += advanceBy;
        /// Note that only advancing the clock does not allow logic that relies on the time to run immediatly and take 0 time.
        /// Thus we still have to rely on real time to give any actions caused by the advance of the Test clock to actually catch up.

        if (doAnActualWait)
        {
            std::this_thread::sleep_for(REAL_TIME_DELAY);
        }
    };

    /// This should only be called during test setup as this breaks the is_steady assumption
    static void reset() { *now_.wlock() = time_point(std::chrono::nanoseconds(0)); }
};

folly::Synchronized<TestClock::time_point> TestClock::now_{time_point(std::chrono::nanoseconds(0))};

using DelayedTaskSubmitter = NES::DelayedTaskSubmitter<TestClock>;

class DelayedTaskSubmitterTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DelayedTaskSubmitterTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup DelayedTaskSubmitterTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        TestClock::reset();
    }

protected:
    folly::Synchronized<std::vector<Task>> submittedTasks;
    std::atomic<int> taskCounter{0};

    /// For tracking execution order in tests
    folly::Synchronized<std::vector<LocalQueryId>> executionOrder;

    void submitTask(Task task)
    {
        submittedTasks.wlock()->push_back(std::move(task));
        ++taskCounter;
    }

    void clearSubmittedTasks()
    {
        submittedTasks.wlock()->clear();
        taskCounter = 0;
    }

    size_t getSubmittedTaskCount() { return submittedTasks.rlock()->size(); }
};

/// NOLINTBEGIN(readability-magic-numbers): It's a test and these are just random values
TEST_F(DelayedTaskSubmitterTest, testBasicTaskSubmission)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    /// Create a simple task
    auto task
        = WorkTask(LocalQueryId(UUIDToString(generateUUID())), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Submit task with immediate execution
    submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(10));

    TestClock::advance(std::chrono::milliseconds(9), true);
    ASSERT_EQ(getSubmittedTaskCount(), 0);

    TestClock::advance(std::chrono::milliseconds(1), true);
    ASSERT_EQ(getSubmittedTaskCount(), 1);
}

TEST_F(DelayedTaskSubmitterTest, testMultipleTasksOrderedExecution)
{
    /// Clear execution order for this test
    executionOrder.wlock()->clear();

    auto submitter = DelayedTaskSubmitter(
        [this](Task task) noexcept
        {
            /// Track execution order through the submitFn callback before moving
            if (std::holds_alternative<WorkTask>(task))
            {
                const auto& workTask = std::get<WorkTask>(task);
                executionOrder.wlock()->push_back(workTask.queryId);
            }
            submitTask(std::move(task));
        });

    /// Submit tasks with different delays
    auto id1 = LocalQueryId(UUIDToString(generateUUID()));
    auto id2 = LocalQueryId(UUIDToString(generateUUID()));
    auto id3 = LocalQueryId(UUIDToString(generateUUID()));
    auto task1 = WorkTask(id1, PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto task2 = WorkTask(id2, PipelineId(2), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto task3 = WorkTask(id3, PipelineId(3), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Submit in reverse order with increasing delays
    submitter.submitTaskIn(std::move(task3), std::chrono::milliseconds(30));
    submitter.submitTaskIn(std::move(task2), std::chrono::milliseconds(20));
    submitter.submitTaskIn(std::move(task1), std::chrono::milliseconds(10));

    TestClock::advance(std::chrono::milliseconds(10), true);
    ASSERT_EQ(getSubmittedTaskCount(), 1);
    TestClock::advance(std::chrono::milliseconds(10), true);
    ASSERT_EQ(getSubmittedTaskCount(), 2);
    TestClock::advance(std::chrono::milliseconds(10), true);
    ASSERT_EQ(getSubmittedTaskCount(), 3);

    /// Check execution order
    {
        auto order = executionOrder.rlock();
        ASSERT_EQ(order->size(), 3);
        EXPECT_THAT(*order, ::testing::ElementsAre(id1, id2, id3));
    }
}

TEST_F(DelayedTaskSubmitterTest, testTaskWithZeroDelay)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    auto task
        = WorkTask(LocalQueryId(UUIDToString(generateUUID())), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Submit task with zero delay
    submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(0));

    /// Wait a bit for task to be executed
    std::this_thread::sleep_for(REAL_TIME_DELAY);

    ASSERT_EQ(getSubmittedTaskCount(), 1);
}

TEST_F(DelayedTaskSubmitterTest, testDifferentDurationTypes)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    auto task1
        = WorkTask(LocalQueryId(UUIDToString(generateUUID())), PipelineId(1), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto task2
        = WorkTask(LocalQueryId(UUIDToString(generateUUID())), PipelineId(2), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
    auto task3
        = WorkTask(LocalQueryId(UUIDToString(generateUUID())), PipelineId(3), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

    /// Test different duration types
    submitter.submitTaskIn(std::move(task1), std::chrono::microseconds(10000)); /// 10ms
    submitter.submitTaskIn(std::move(task2), std::chrono::milliseconds(10)); /// 10ms
    submitter.submitTaskIn(std::move(task3), std::chrono::seconds(0)); /// 0s

    /// Wait for all tasks to be executed
    TestClock::advance(std::chrono::milliseconds(50), true);

    ASSERT_EQ(getSubmittedTaskCount(), 3);
}

TEST_F(DelayedTaskSubmitterTest, testConcurrentTaskSubmission)
{
    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });

    std::vector<std::jthread> threads;
    constexpr int numThreads = 10;
    constexpr int tasksPerThread = 5;

    /// Create multiple threads submitting tasks concurrently
    threads.reserve(numThreads);
    for (int i = 0; i < numThreads; ++i)
    {
        threads.emplace_back(
            [&submitter, i]()
            {
                const auto queryId = LocalQueryId(generateUUID());
                for (int j = 0; j < tasksPerThread; ++j)
                {
                    auto task
                        = WorkTask(queryId, PipelineId((i * tasksPerThread) + j), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});
                    if (j % 2 == 0)
                    {
                        TestClock::advance(std::chrono::milliseconds(1), false);
                    }
                    submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(10));
                }
            });
    }
    threads.clear();

    /// Wait for all tasks to be executed
    for (size_t i = 0; i < 5; ++i)
    {
        TestClock::advance(std::chrono::milliseconds(2), true);
    }

    EXPECT_EQ(getSubmittedTaskCount(), numThreads * tasksPerThread);
}

TEST_F(DelayedTaskSubmitterTest, testDestructorCleanup)
{
    std::atomic submittedCount{0};
    std::atomic failureCount{0};
    std::atomic completeCount{0};

    {
        auto submitter = DelayedTaskSubmitter([&submittedCount](Task /*task*/) noexcept { ++submittedCount; });

        /// Submit a task with long delay and custom onComplete and onFailure callbacks
        auto task = WorkTask(
            LocalQueryId(generateUUID()),
            PipelineId(1),
            std::weak_ptr<RunningQueryPlanNode>(),
            TupleBuffer(),
            TaskCallback(
                TaskCallback::OnComplete([&completeCount] { ++completeCount; }),
                TaskCallback::OnFailure([&failureCount](const Exception&) { ++failureCount; })));
        submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(1000));
        TestClock::advance(std::chrono::milliseconds(100), true);
        /// Destructor should be called here, cleaning up pending tasks
    }

    TestClock::advance(std::chrono::milliseconds(1000), true);
    /// Task should not be executed since submitter was destroyed, but the failure callback should be called
    EXPECT_EQ(submittedCount.load(), 0);
    EXPECT_EQ(completeCount.load(), 0);
    EXPECT_EQ(failureCount.load(), 1);
}

TEST_F(DelayedTaskSubmitterTest, testStressRandomTasks)
{
    constexpr int numThreads = 10;
    constexpr int tasksPerThread = 20;
    constexpr int totalTasks = numThreads * tasksPerThread;
    constexpr int maxDelayMs = 50;

    auto submitter = DelayedTaskSubmitter([this](Task task) noexcept { submitTask(std::move(task)); });
    std::vector<std::jthread> threads;
    threads.reserve(numThreads);
    /// Create multiple threads submitting random tasks with random delays
    for (int threadId = 0; threadId < numThreads; ++threadId)
    {
        threads.emplace_back(
            [&submitter, threadId]()
            {
                const auto queryId = LocalQueryId(generateUUID());
                /// Each thread gets its own random number generator
                std::random_device randomDevice;
                std::mt19937 gen(randomDevice());
                std::uniform_int_distribution<> dis(0, maxDelayMs);

                for (int i = 0; i < tasksPerThread; ++i)
                {
                    auto task = WorkTask(
                        queryId, PipelineId((threadId * tasksPerThread) + i), std::weak_ptr<RunningQueryPlanNode>(), TupleBuffer(), {});

                    /// Random delay between 0 and maxDelayMs milliseconds
                    const int randomDelay = dis(gen);
                    submitter.submitTaskIn(std::move(task), std::chrono::milliseconds(randomDelay));

                    TestClock::advance(std::chrono::milliseconds(1), false);
                }
            });
    }

    threads.clear();

    /// Now wait for the maximum delay to ensure all tasks have been processed
    TestClock::advance(std::chrono::milliseconds(maxDelayMs), true);
    /// Verify all tasks were submitted
    ASSERT_EQ(getSubmittedTaskCount(), totalTasks);
}

/// NOLINTEND(readability-magic-numbers)
}
