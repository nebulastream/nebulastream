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
#include <iterator>
#include <ranges>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DistributedQuery.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/UUID.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
class DistributedQueryTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DistributedQueryTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributedQueryTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down DistributedQueryTest class."); }
};

///NOLINTBEGIN(bugprone-unchecked-optional-access, readability-magic-numbers)
TEST_F(DistributedQueryTest, DefaultConstructor)
{
    const DistributedQuery query;
    EXPECT_EQ(std::ranges::distance(query.iterate()), 0);
}

TEST_F(DistributedQueryTest, ConstructorWithLocalQueries)
{
    const std::unordered_map<WorkerId, std::vector<QueryId>> localQueries
        = {{WorkerId("worker-1:8080"),
            {QueryId::createLocal(LocalQueryId(generateUUID())), QueryId::createLocal(LocalQueryId(generateUUID()))}},
           {WorkerId("worker-2:8080"), {QueryId::createLocal(LocalQueryId(generateUUID()))}}};

    const DistributedQuery query(localQueries);

    std::unordered_map<WorkerId, std::vector<QueryId>> collected;
    for (const auto& [grpc, queryId] : query.iterate())
    {
        collected[grpc].push_back(queryId);
    }

    EXPECT_EQ(collected.size(), 2);
    EXPECT_TRUE(collected.contains(WorkerId("worker-1:8080")));
    EXPECT_TRUE(collected.contains(WorkerId("worker-2:8080")));
}

TEST_F(DistributedQueryTest, Iteration)
{
    auto qid1 = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto qid2 = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto qid3 = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto qid4 = QueryId::createLocal(LocalQueryId(generateUUID()));
    const std::unordered_map<WorkerId, std::vector<QueryId>> localQueries
        = {{WorkerId("worker-1:8080"), {qid1, qid2}}, {WorkerId("worker-2:8080"), {qid3}}, {WorkerId("worker-3:8080"), {qid4}}};

    const DistributedQuery query(localQueries);

    EXPECT_THAT(
        query.iterate() | std::ranges::to<std::vector>(),
        testing::UnorderedElementsAre(
            std::pair{WorkerId("worker-1:8080"), qid1},
            std::pair{WorkerId("worker-1:8080"), qid2},
            std::pair{WorkerId("worker-2:8080"), qid3},
            std::pair{WorkerId("worker-3:8080"), qid4}));
}

TEST_F(DistributedQueryTest, Printing)
{
    auto qid1 = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto qid2 = QueryId::createLocal(LocalQueryId(generateUUID()));
    const std::unordered_map<WorkerId, std::vector<QueryId>> localQueries
        = {{WorkerId("worker-1:8080"), {qid1, qid2}}, {WorkerId("worker-2:8080"), {qid1}}, {WorkerId("worker-3:8080"), {qid1}}};

    const DistributedQuery query(localQueries);
    std::stringstream oss;
    oss << query;
    /// The output contains UUIDs and the order may vary, so just check it contains expected parts
    EXPECT_TRUE(oss.str().starts_with("Query ["));
    EXPECT_TRUE(oss.str().contains("@worker-1:8080"));
    EXPECT_TRUE(oss.str().contains("@worker-2:8080"));
    EXPECT_TRUE(oss.str().contains("@worker-3:8080"));
}

TEST_F(DistributedQueryTest, EqualityOperator)
{
    auto qid1 = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto qid2 = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto qid3 = QueryId::createLocal(LocalQueryId(generateUUID()));

    const std::unordered_map<WorkerId, std::vector<QueryId>> localQueries1
        = {{WorkerId("worker-1:8080"), {qid1}}, {WorkerId("worker-2:8080"), {qid2}}};

    const std::unordered_map<WorkerId, std::vector<QueryId>> localQueries2
        = {{WorkerId("worker-1:8080"), {qid1}}, {WorkerId("worker-2:8080"), {qid2}}};

    const std::unordered_map<WorkerId, std::vector<QueryId>> localQueries3 = {{WorkerId("worker-3:8080"), {qid3}}};

    const DistributedQuery query1(localQueries1);
    const DistributedQuery query2(localQueries2);
    const DistributedQuery query3(localQueries3);

    EXPECT_EQ(query1, query2);
    EXPECT_NE(query1, query3);
}

TEST_F(DistributedQueryTest, MultipleQueriesPerNode)
{
    auto qid1 = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto qid2 = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto qid3 = QueryId::createLocal(LocalQueryId(generateUUID()));
    const std::unordered_map<WorkerId, std::vector<QueryId>> localQueries = {{WorkerId("worker-1:8080"), {qid1, qid2, qid3}}};

    const DistributedQuery query(localQueries);

    std::vector<QueryId> collectedIds;
    for (const auto& [grpc, queryId] : query.iterate())
    {
        EXPECT_EQ(grpc, WorkerId("worker-1:8080"));
        collectedIds.push_back(queryId);
    }

    EXPECT_EQ(collectedIds.size(), 3);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllRegistered)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Registered, .metrics = {}};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid2), .state = QueryState::Registered, .metrics = {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Registered);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllRunning)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Running, .metrics = {}};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid2)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid2), .state = QueryState::Running, .metrics = {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Running);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllStopped)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Stopped, .metrics = {}};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid2)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid2), .state = QueryState::Stopped, .metrics = {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Stopped);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStatePartiallyStopped)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Running, .metrics = {}};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid2), .state = QueryState::Stopped, .metrics = {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::PartiallyStopped);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateFailed)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Running, .metrics = {}};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid2), .state = QueryState::Failed, .metrics = {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Failed);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateUnreachable)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Running, .metrics = {}};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid1)]
        = std::unexpected(QueryStatusFailed("Connection error"));

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Unreachable);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateFailedWithUnreachableWorker)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Failed, .metrics = {}};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid1)]
        = std::unexpected(QueryStatusFailed("Connection error"));

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Failed);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGetExceptionsEmpty)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Running, .metrics = {}};

    auto exceptions = status.getExceptions();
    EXPECT_TRUE(exceptions.empty());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGetExceptionsFromMetrics)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");

    QueryMetrics metrics;
    metrics.error = QueryStartFailed("Test error");

    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Failed, .metrics = metrics};

    auto exceptions = status.getExceptions();
    EXPECT_EQ(exceptions.size(), 1);
    EXPECT_TRUE(exceptions.contains(WorkerId("worker-1:8080")));
    EXPECT_EQ(exceptions.at(WorkerId("worker-1:8080")).size(), 1);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGetExceptionsFromExpected)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = std::unexpected(QueryStatusFailed("Communication error"));

    auto exceptions = status.getExceptions();
    EXPECT_EQ(exceptions.size(), 1);
    EXPECT_TRUE(exceptions.contains(WorkerId("worker-1:8080")));
    EXPECT_EQ(exceptions.at(WorkerId("worker-1:8080")).size(), 1);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceExceptionEmpty)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Running, .metrics = {}};

    auto exception = status.coalesceException();
    EXPECT_FALSE(exception.has_value());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceExceptionSingle)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");

    QueryMetrics metrics;
    metrics.error = QueryStartFailed("Test error");

    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Failed, .metrics = metrics};

    auto exception = status.coalesceException();
    EXPECT_TRUE(exception.has_value());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceExceptionMultiple)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    auto qid3 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");

    QueryMetrics metrics1;
    metrics1.error = QueryStartFailed("Error 1");

    QueryMetrics metrics2;
    metrics2.error = QueryStopFailed("Error 2");

    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Failed, .metrics = metrics1};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid2), .state = QueryState::Failed, .metrics = metrics2};
    status.localStatusSnapshots[WorkerId("worker-3:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid3), .state = QueryState::Running, .metrics = QueryMetrics{}};

    auto exception = status.coalesceException();
    EXPECT_TRUE(exception.has_value());
    EXPECT_TRUE(exception->what().contains("worker-1:8080"));
    EXPECT_TRUE(exception->what().contains("Error 1"));
    EXPECT_TRUE(exception->what().contains("worker-2:8080"));
    EXPECT_TRUE(exception->what().contains("Error 2"));
    EXPECT_FALSE(exception->what().contains("worker-3:8080"));
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceQueryMetricsEmpty)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Running, .metrics = {}};

    auto metrics = status.coalesceQueryMetrics();
    EXPECT_FALSE(metrics.start.has_value());
    EXPECT_FALSE(metrics.running.has_value());
    EXPECT_FALSE(metrics.stop.has_value());
    EXPECT_FALSE(metrics.error.has_value());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceQueryMetricsWithTimestamps)
{
    using namespace std::chrono;

    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");

    QueryMetrics metrics1;
    metrics1.start = system_clock::time_point{microseconds(1000)};
    metrics1.running = system_clock::time_point{microseconds(2000)};
    metrics1.stop = system_clock::time_point{microseconds(5000)};

    QueryMetrics metrics2;
    metrics2.start = system_clock::time_point{microseconds(1500)};
    metrics2.running = system_clock::time_point{microseconds(2500)};
    metrics2.stop = system_clock::time_point{microseconds(6000)};

    status.localStatusSnapshots[WorkerId("worker-1:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid1), .state = QueryState::Stopped, .metrics = metrics1};
    status.localStatusSnapshots[WorkerId("worker-2:8080")][QueryId::createLocal(qid1)]
        = LocalQueryStatus{.queryId = QueryId::createLocal(qid2), .state = QueryState::Stopped, .metrics = metrics2};

    auto coalescedMetrics = status.coalesceQueryMetrics();

    EXPECT_TRUE(coalescedMetrics.start.has_value());
    EXPECT_EQ(coalescedMetrics.start.value(), system_clock::time_point{microseconds(1000)});

    EXPECT_TRUE(coalescedMetrics.running.has_value());
    EXPECT_EQ(coalescedMetrics.running.value(), system_clock::time_point{microseconds(2500)});

    EXPECT_TRUE(coalescedMetrics.stop.has_value());
    EXPECT_EQ(coalescedMetrics.stop.value(), system_clock::time_point{microseconds(6000)});
}

///NOLINTEND(bugprone-unchecked-optional-access, readability-magic-numbers)
}
