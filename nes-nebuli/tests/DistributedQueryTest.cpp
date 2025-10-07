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

#include <DistributedQuery.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>

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

TEST_F(DistributedQueryTest, DefaultConstructor)
{
    DistributedQuery query;
    EXPECT_EQ(std::ranges::distance(query.iterate()), 0);
}

TEST_F(DistributedQueryTest, ConstructorWithLocalQueries)
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries
        = {{GrpcAddr("worker-1:8080"), {LocalQueryId(generateUUID()), LocalQueryId(generateUUID())}},
           {GrpcAddr("worker-2:8080"), {LocalQueryId(generateUUID())}}};

    DistributedQuery query(localQueries);

    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> collected;
    for (const auto& [grpc, localQueryId] : query.iterate())
    {
        collected[grpc].push_back(localQueryId);
    }

    EXPECT_EQ(collected.size(), 2);
    EXPECT_TRUE(collected.contains(GrpcAddr("worker-1:8080")));
    EXPECT_TRUE(collected.contains(GrpcAddr("worker-2:8080")));
}

TEST_F(DistributedQueryTest, Iteration)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    auto qid3 = LocalQueryId(generateUUID());
    auto qid4 = LocalQueryId(generateUUID());
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries
        = {{GrpcAddr("worker-1:8080"), {qid1, qid2}}, {GrpcAddr("worker-2:8080"), {qid3}}, {GrpcAddr("worker-3:8080"), {qid4}}};

    DistributedQuery query(localQueries);

    EXPECT_THAT(
        query.iterate() | std::ranges::to<std::vector>(),
        testing::UnorderedElementsAre(
            std::pair{GrpcAddr("worker-1:8080"), qid1},
            std::pair{GrpcAddr("worker-1:8080"), qid2},
            std::pair{GrpcAddr("worker-2:8080"), qid3},
            std::pair{GrpcAddr("worker-3:8080"), qid4}));
}

TEST_F(DistributedQueryTest, Printing)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries
        = {{GrpcAddr("worker-1:8080"), {qid1, qid2}}, {GrpcAddr("worker-2:8080"), {qid1}}, {GrpcAddr("worker-3:8080"), {qid1}}};

    DistributedQuery query(localQueries);
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
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    auto qid3 = LocalQueryId(generateUUID());

    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries1
        = {{GrpcAddr("worker-1:8080"), {qid1}}, {GrpcAddr("worker-2:8080"), {qid2}}};

    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries2
        = {{GrpcAddr("worker-1:8080"), {qid1}}, {GrpcAddr("worker-2:8080"), {qid2}}};

    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries3 = {{GrpcAddr("worker-3:8080"), {qid3}}};

    DistributedQuery query1(localQueries1);
    DistributedQuery query2(localQueries2);
    DistributedQuery query3(localQueries3);

    EXPECT_EQ(query1, query2);
    EXPECT_NE(query1, query3);
}

TEST_F(DistributedQueryTest, MultipleQueriesPerNode)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    auto qid3 = LocalQueryId(generateUUID());
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries = {{GrpcAddr("worker-1:8080"), {qid1, qid2, qid3}}};

    DistributedQuery query(localQueries);

    std::vector<LocalQueryId> collectedIds;
    for (const auto& [grpc, localQueryId] : query.iterate())
    {
        EXPECT_EQ(grpc, GrpcAddr("worker-1:8080"));
        collectedIds.push_back(localQueryId);
    }

    EXPECT_EQ(collectedIds.size(), 3);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllRegistered)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Registered, {}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = LocalQueryStatus{qid2, QueryState::Registered, {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Registered);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllRunning)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Running, {}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = LocalQueryStatus{qid2, QueryState::Running, {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Running);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllStopped)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Stopped, {}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = LocalQueryStatus{qid2, QueryState::Stopped, {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Stopped);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStatePartiallyStopped)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Running, {}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = LocalQueryStatus{qid2, QueryState::Stopped, {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::PartiallyStopped);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateFailed)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Running, {}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = LocalQueryStatus{qid2, QueryState::Failed, {}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Failed);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateUnreachable)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Running, {}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = std::unexpected(QueryStatusFailed("Connection error"));

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Unreachable);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateFailedWithUnreachableWorker)
{
    auto qid1 = LocalQueryId(generateUUID());
    auto qid2 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Failed, {}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = std::unexpected(QueryStatusFailed("Connection error"));

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Failed);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGetExceptionsEmpty)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Running, {}};

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

    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Failed, metrics};

    auto exceptions = status.getExceptions();
    EXPECT_EQ(exceptions.size(), 1);
    EXPECT_TRUE(exceptions.contains(GrpcAddr("worker-1:8080")));
    EXPECT_EQ(exceptions.at(GrpcAddr("worker-1:8080")).size(), 1);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGetExceptionsFromExpected)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = std::unexpected(QueryStatusFailed("Communication error"));

    auto exceptions = status.getExceptions();
    EXPECT_EQ(exceptions.size(), 1);
    EXPECT_TRUE(exceptions.contains(GrpcAddr("worker-1:8080")));
    EXPECT_EQ(exceptions.at(GrpcAddr("worker-1:8080")).size(), 1);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceExceptionEmpty)
{
    auto qid1 = LocalQueryId(generateUUID());
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId("test-distributed-query-1");
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Running, {}};

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

    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Failed, metrics};

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

    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Failed, metrics1};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = LocalQueryStatus{qid2, QueryState::Failed, metrics2};
    status.localStatusSnapshots[GrpcAddr("worker-3:8080")][qid3] = LocalQueryStatus{qid3, QueryState::Running, QueryMetrics{}};

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
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Running, {}};

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

    status.localStatusSnapshots[GrpcAddr("worker-1:8080")][qid1] = LocalQueryStatus{qid1, QueryState::Stopped, metrics1};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")][qid2] = LocalQueryStatus{qid2, QueryState::Stopped, metrics2};

    auto coalescedMetrics = status.coalesceQueryMetrics();

    EXPECT_TRUE(coalescedMetrics.start.has_value());
    EXPECT_EQ(coalescedMetrics.start.value(), system_clock::time_point{microseconds(1000)});

    EXPECT_TRUE(coalescedMetrics.running.has_value());
    EXPECT_EQ(coalescedMetrics.running.value(), system_clock::time_point{microseconds(2500)});

    EXPECT_TRUE(coalescedMetrics.stop.has_value());
    EXPECT_EQ(coalescedMetrics.stop.value(), system_clock::time_point{microseconds(6000)});
}

}
