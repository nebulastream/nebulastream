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

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <ErrorHandling.hpp>

using namespace std::chrono_literals;

namespace NES
{

class QueryLogTest : public ::testing::Test
{
protected:
    void SetUp() override { queryLog = std::make_unique<QueryLog>(); }

    std::unique_ptr<QueryLog> queryLog;
    const QueryId testQueryId{42};
    const std::chrono::system_clock::time_point testTime = std::chrono::system_clock::now();
};

TEST_F(QueryLogTest, LogQueryStatusChangeBasic)
{
    EXPECT_TRUE(queryLog->logQueryStatusChange(testQueryId, QueryState::Started, testTime));
    EXPECT_TRUE(queryLog->logQueryStatusChange(testQueryId, QueryState::Running, testTime + 100ms));
    EXPECT_TRUE(queryLog->logQueryStatusChange(testQueryId, QueryState::Stopped, testTime + 200ms));
}

TEST_F(QueryLogTest, GetLogForQuery)
{
    queryLog->logQueryStatusChange(testQueryId, QueryState::Started, testTime);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Running, testTime + 100ms);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Stopped, testTime + 200ms);

    auto log = queryLog->getLogForQuery(testQueryId);
    ASSERT_TRUE(log.has_value());
    EXPECT_EQ(log->size(), 3);

    EXPECT_EQ(log->at(0).state, QueryState::Started);
    EXPECT_EQ(log->at(1).state, QueryState::Running);
    EXPECT_EQ(log->at(2).state, QueryState::Stopped);
}

TEST_F(QueryLogTest, GetLogForNonExistentQuery)
{
    const auto log = queryLog->getLogForQuery(QueryId{999});
    EXPECT_FALSE(log.has_value());
}

TEST_F(QueryLogTest, GetQuerySummarySuccessfulExecution)
{
    queryLog->logQueryStatusChange(testQueryId, QueryState::Started, testTime);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Running, testTime + 100ms);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Stopped, testTime + 200ms);

    auto summary = queryLog->getQueryStatus(testQueryId);
    ASSERT_TRUE(summary.has_value());

    EXPECT_EQ(summary->queryId, testQueryId);
    EXPECT_EQ(summary->state, QueryState::Stopped);

    EXPECT_TRUE(summary->metrics.start.has_value());
    EXPECT_TRUE(summary->metrics.running.has_value());
    EXPECT_TRUE(summary->metrics.stop.has_value());
    EXPECT_FALSE(summary->metrics.error.has_value());

    EXPECT_EQ(*summary->metrics.start, testTime);
    EXPECT_EQ(*summary->metrics.running, testTime + 100ms);
    EXPECT_EQ(*summary->metrics.stop, testTime + 200ms);
}

TEST_F(QueryLogTest, GetQuerySummaryWithFailure)
{
    const Exception testError{"Test error", 500};

    queryLog->logQueryStatusChange(testQueryId, QueryState::Started, testTime);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Running, testTime + 100ms);
    queryLog->logQueryFailure(testQueryId, testError, testTime + 200ms);

    auto summary = queryLog->getQueryStatus(testQueryId);
    ASSERT_TRUE(summary.has_value());

    EXPECT_EQ(summary->queryId, testQueryId);
    EXPECT_EQ(summary->state, QueryState::Failed);

    EXPECT_TRUE(summary->metrics.start.has_value());
    EXPECT_TRUE(summary->metrics.running.has_value());
    EXPECT_TRUE(summary->metrics.stop.has_value());
    EXPECT_TRUE(summary->metrics.error.has_value());

    EXPECT_EQ(summary->metrics.error->code(), 500);
    EXPECT_EQ(summary->metrics.error->what(), "Test error");
}

TEST_F(QueryLogTest, GetQuerySummaryPartialExecution)
{
    queryLog->logQueryStatusChange(testQueryId, QueryState::Started, testTime);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Running, testTime + 100ms);

    const auto status = queryLog->getQueryStatus(testQueryId);
    ASSERT_TRUE(status.has_value());

    EXPECT_EQ(status->queryId, testQueryId);
    EXPECT_EQ(status->state, QueryState::Running);

    EXPECT_TRUE(status->metrics.start.has_value());
    EXPECT_TRUE(status->metrics.running.has_value());
    EXPECT_FALSE(status->metrics.stop.has_value());
    EXPECT_FALSE(status->metrics.error.has_value());
}

TEST_F(QueryLogTest, GetQuerySummaryForNonExistentQuery)
{
    const auto status = queryLog->getQueryStatus(QueryId{999});
    EXPECT_FALSE(status.has_value());
}

TEST_F(QueryLogTest, MultipleQueriesIndependentLogs)
{
    constexpr QueryId query1{1};
    constexpr QueryId query2{2};

    queryLog->logQueryStatusChange(query1, QueryState::Started, testTime);
    queryLog->logQueryStatusChange(query2, QueryState::Started, testTime + 50ms);
    queryLog->logQueryStatusChange(query1, QueryState::Running, testTime + 100ms);
    queryLog->logQueryFailure(query2, Exception{"Query 2 failed", 400}, testTime + 150ms); /// NOLINT(*-magic-numbers)

    const auto status1 = queryLog->getQueryStatus(query1);
    const auto status2 = queryLog->getQueryStatus(query2);

    ASSERT_TRUE(status1.has_value());
    ASSERT_TRUE(status2.has_value());

    EXPECT_EQ(status1->state, QueryState::Running);
    EXPECT_EQ(status2->state, QueryState::Failed);

    EXPECT_FALSE(status1->metrics.error.has_value());
    EXPECT_TRUE(status2->metrics.error.has_value());
    EXPECT_EQ(status2->metrics.error->code(), 400);
}

TEST_F(QueryLogTest, QueryStateChangeConstructors)
{
    /// Test state-only constructor
    const QueryStateChange stateChange1(QueryState::Running, testTime);
    EXPECT_EQ(stateChange1.state, QueryState::Running);
    EXPECT_EQ(stateChange1.timestamp, testTime);
    EXPECT_FALSE(stateChange1.exception.has_value());

    /// Test exception constructor
    const Exception testError{"Test exception", 404};
    QueryStateChange stateChange2(testError, testTime);
    EXPECT_EQ(stateChange2.state, QueryState::Failed);
    EXPECT_EQ(stateChange2.timestamp, testTime);
    EXPECT_TRUE(stateChange2.exception.has_value());
    EXPECT_EQ(stateChange2.exception->code(), 404);
}

TEST_F(QueryLogTest, OutOfOrderEventsWithMonotonicTimestamps)
{
    /// Test that events can arrive out of order but with monotonic timestamps
    /// Events arrive in order: Running, Started, Stopped (out of logical order)
    /// But timestamps are monotonic: t0 <= t1 <= t2
    const auto t0 = testTime;
    const auto t1 = testTime + 100ms;
    const auto t2 = testTime + 200ms;

    /// Log events out of logical order but with monotonic timestamps
    queryLog->logQueryStatusChange(testQueryId, QueryState::Running, t1);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Started, t0);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Stopped, t2);

    /// Verify that the log preserves the order events were logged
    const auto log = queryLog->getLogForQuery(testQueryId);
    ASSERT_TRUE(log.has_value());
    EXPECT_EQ(log->size(), 3);

    /// Verify status uses the most recent state and appropriate timestamps
    const auto status = queryLog->getQueryStatus(testQueryId);
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->state, QueryState::Stopped);

    /// Metrics should reflect the actual timestamps, not arrival order
    EXPECT_TRUE(status->metrics.start.has_value());
    EXPECT_TRUE(status->metrics.running.has_value());
    EXPECT_TRUE(status->metrics.stop.has_value());

    EXPECT_EQ(*status->metrics.start, t0);
    EXPECT_EQ(*status->metrics.running, t1);
    EXPECT_EQ(*status->metrics.stop, t2);
}

TEST_F(QueryLogTest, EventsWithEqualTimestamps)
{
    /// Test behavior when multiple events have the same timestamp
    const auto sameTime = testTime;
    const Exception testError{"Test failure", 500};

    queryLog->logQueryStatusChange(testQueryId, QueryState::Registered, sameTime);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Started, sameTime);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Running, sameTime);
    queryLog->logQueryStatusChange(testQueryId, QueryState::Stopped, sameTime);
    queryLog->logQueryFailure(testQueryId, testError, sameTime);

    const auto log = queryLog->getLogForQuery(testQueryId);
    ASSERT_TRUE(log.has_value());
    EXPECT_EQ(log->size(), 5);

    /// All events should have the same timestamp
    for (const auto& entry : *log)
    {
        EXPECT_EQ(entry.timestamp, sameTime);
    }

    /// Status should show the final state (Failed)
    const auto status = queryLog->getQueryStatus(testQueryId);
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->state, QueryState::Failed);

    /// All metric timestamps should be the same
    EXPECT_EQ(*status->metrics.start, sameTime);
    EXPECT_EQ(*status->metrics.running, sameTime);
    EXPECT_EQ(*status->metrics.stop, sameTime);

    /// Error should be captured
    EXPECT_TRUE(status->metrics.error.has_value());
    EXPECT_EQ(status->metrics.error->code(), 500);
    EXPECT_STREQ(status->metrics.error->what(), "Test failure");
}

TEST_F(QueryLogTest, MultiThreadedLogging)
{
    /// Test that multiple threads can safely log to the same QueryLog instance
    constexpr int numThreads = 4;
    constexpr int eventsPerThread = 10;
    const auto baseTime = testTime;

    std::vector<std::thread> threads;

    /// Launch multiple threads that log events for different queries
    for (int threadId = 0; threadId < numThreads; ++threadId)
    {
        threads.emplace_back(
            [this, threadId, baseTime]()
            {
                const QueryId queryId{threadId + 100UL}; /// Unique query ID per thread

                /// Each thread logs a sequence of state changes
                for (int event = 0; event < eventsPerThread; ++event)
                {
                    const auto timestamp = baseTime + std::chrono::milliseconds{threadId * 100 + event * 10};

                    if (event == 0)
                    {
                        queryLog->logQueryStatusChange(queryId, QueryState::Started, timestamp);
                    }
                    else if (event == eventsPerThread - 1)
                    {
                        queryLog->logQueryStatusChange(queryId, QueryState::Stopped, timestamp);
                    }
                    else
                    {
                        queryLog->logQueryStatusChange(queryId, QueryState::Running, timestamp);
                    }
                }
            });
    }

    /// Wait for all threads to complete
    for (auto& thread : threads)
    {
        thread.join();
    }

    /// Verify that each thread's events were logged correctly
    for (int threadId = 0; threadId < numThreads; ++threadId)
    {
        const QueryId queryId{threadId + 100UL};

        const auto log = queryLog->getLogForQuery(queryId);
        ASSERT_TRUE(log.has_value()) << "Thread " << threadId << " events not found";
        EXPECT_EQ(log->size(), eventsPerThread) << "Thread " << threadId << " wrong event count";

        const auto status = queryLog->getQueryStatus(queryId);
        ASSERT_TRUE(status.has_value());
        EXPECT_EQ(status->state, QueryState::Stopped);
        EXPECT_TRUE(status->metrics.start.has_value());
        EXPECT_TRUE(status->metrics.stop.has_value());
    }

    /// Verify that we have logs for all expected queries and no extra ones
    const auto statusResults = queryLog->getStatus();
    EXPECT_EQ(statusResults.size(), numThreads);
}

}
