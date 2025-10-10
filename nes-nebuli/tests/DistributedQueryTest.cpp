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
    EXPECT_EQ(std::ranges::distance(query.getLocalQueries()), 0);
}

TEST_F(DistributedQueryTest, ConstructorWithLocalQueries)
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries
        = {{GrpcAddr("worker-1:8080"), {LocalQueryId(1), LocalQueryId(2)}}, {GrpcAddr("worker-2:8080"), {LocalQueryId(3)}}};

    DistributedQuery query(localQueries);

    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> collected;
    for (const auto& [grpc, localQueryId] : query.getLocalQueries())
    {
        collected[grpc].push_back(localQueryId);
    }

    EXPECT_EQ(collected.size(), 2);
    EXPECT_TRUE(collected.contains(GrpcAddr("worker-1:8080")));
    EXPECT_TRUE(collected.contains(GrpcAddr("worker-2:8080")));
}

TEST_F(DistributedQueryTest, Iteration)
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries
        = {{GrpcAddr("worker-1:8080"), {LocalQueryId(1), LocalQueryId(2)}},
           {GrpcAddr("worker-2:8080"), {LocalQueryId(3)}},
           {GrpcAddr("worker-3:8080"), {LocalQueryId(4)}}};

    DistributedQuery query(localQueries);

    EXPECT_THAT(
        query.getLocalQueries()| std::ranges::to<std::vector>(),
        testing::UnorderedElementsAre(
            std::pair{GrpcAddr("worker-1:8080"), LocalQueryId(1)},
            std::pair{GrpcAddr("worker-1:8080"), LocalQueryId(2)},
            std::pair{GrpcAddr("worker-2:8080"), LocalQueryId(3)},
            std::pair{GrpcAddr("worker-3:8080"), LocalQueryId(4)}));
}

TEST_F(DistributedQueryTest, EqualityOperator)
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries1
        = {{GrpcAddr("worker-1:8080"), {LocalQueryId(1)}}, {GrpcAddr("worker-2:8080"), {LocalQueryId(2)}}};

    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries2
        = {{GrpcAddr("worker-1:8080"), {LocalQueryId(1)}}, {GrpcAddr("worker-2:8080"), {LocalQueryId(2)}}};

    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries3 = {{GrpcAddr("worker-3:8080"), {LocalQueryId(3)}}};

    DistributedQuery query1(localQueries1);
    DistributedQuery query2(localQueries2);
    DistributedQuery query3(localQueries3);

    EXPECT_EQ(query1, query2);
    EXPECT_NE(query1, query3);
}

TEST_F(DistributedQueryTest, MultipleQueriesPerNode)
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries
        = {{GrpcAddr("worker-1:8080"), {LocalQueryId(1), LocalQueryId(2), LocalQueryId(3)}}};

    DistributedQuery query(localQueries);

    std::vector<LocalQueryId> collectedIds;
    for (const auto& [grpc, localQueryId] : query.getLocalQueries())
    {
        EXPECT_EQ(grpc, GrpcAddr("worker-1:8080"));
        collectedIds.push_back(localQueryId);
    }

    EXPECT_EQ(collectedIds.size(), 3);
}

TEST_F(DistributedQueryTest, DistributedQueryIdGeneration)
{
    auto id1 = getNextDistributedQueryId();
    auto id2 = getNextDistributedQueryId();
    auto id3 = getNextDistributedQueryId();

    EXPECT_NE(id1, id2);
    EXPECT_NE(id2, id3);
    EXPECT_NE(id1, id3);
    EXPECT_LT(id1.getRawValue(), id2.getRawValue());
    EXPECT_LT(id2.getRawValue(), id3.getRawValue());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllRegistered)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Registered, {}}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")] = {LocalQueryStatus{LocalQueryId(2), QueryState::Registered, {}}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Registered);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllRunning)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Running, {}}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")] = {LocalQueryStatus{LocalQueryId(2), QueryState::Running, {}}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Running);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateAllStopped)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Stopped, {}}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")] = {LocalQueryStatus{LocalQueryId(2), QueryState::Stopped, {}}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Stopped);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStatePartiallyStopped)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Running, {}}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")] = {LocalQueryStatus{LocalQueryId(2), QueryState::Stopped, {}}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::PartiallyStopped);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGlobalStateFailed)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Running, {}}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")] = {LocalQueryStatus{LocalQueryId(2), QueryState::Failed, {}}};

    EXPECT_EQ(status.getGlobalQueryState(), DistributedQueryState::Failed);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGetExceptionsEmpty)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Running, {}}};

    auto exceptions = status.getExceptions();
    EXPECT_TRUE(exceptions.empty());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGetExceptionsFromMetrics)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);

    QueryMetrics metrics;
    metrics.error = QueryStartFailed("Test error");

    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Failed, metrics}};

    auto exceptions = status.getExceptions();
    EXPECT_EQ(exceptions.size(), 1);
    EXPECT_TRUE(exceptions.contains(GrpcAddr("worker-1:8080")));
    EXPECT_EQ(exceptions.at(GrpcAddr("worker-1:8080")).size(), 1);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusGetExceptionsFromExpected)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {std::unexpected(QueryStatusFailed("Communication error"))};

    auto exceptions = status.getExceptions();
    EXPECT_EQ(exceptions.size(), 1);
    EXPECT_TRUE(exceptions.contains(GrpcAddr("worker-1:8080")));
    EXPECT_EQ(exceptions.at(GrpcAddr("worker-1:8080")).size(), 1);
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceExceptionEmpty)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Running, {}}};

    auto exception = status.coalesceException();
    EXPECT_FALSE(exception.has_value());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceExceptionSingle)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);

    QueryMetrics metrics;
    metrics.error = QueryStartFailed("Test error");

    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Failed, metrics}};

    auto exception = status.coalesceException();
    EXPECT_TRUE(exception.has_value());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceExceptionMultiple)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);

    QueryMetrics metrics1;
    metrics1.error = QueryStartFailed("Error 1");

    QueryMetrics metrics2;
    metrics2.error = QueryStopFailed("Error 2");

    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Failed, metrics1}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")] = {LocalQueryStatus{LocalQueryId(2), QueryState::Failed, metrics2}};

    auto exception = status.coalesceException();
    EXPECT_TRUE(exception.has_value());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceQueryMetricsEmpty)
{
    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);
    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Running, {}}};

    auto metrics = status.coalesceQueryMetrics();
    EXPECT_FALSE(metrics.start.has_value());
    EXPECT_FALSE(metrics.running.has_value());
    EXPECT_FALSE(metrics.stop.has_value());
    EXPECT_FALSE(metrics.error.has_value());
}

TEST_F(DistributedQueryTest, DistributedQueryStatusCoalesceQueryMetricsWithTimestamps)
{
    using namespace std::chrono;

    DistributedQueryStatus status;
    status.queryId = DistributedQueryId(1);

    QueryMetrics metrics1;
    metrics1.start = system_clock::time_point{microseconds(1000)};
    metrics1.running = system_clock::time_point{microseconds(2000)};
    metrics1.stop = system_clock::time_point{microseconds(5000)};

    QueryMetrics metrics2;
    metrics2.start = system_clock::time_point{microseconds(1500)};
    metrics2.running = system_clock::time_point{microseconds(2500)};
    metrics2.stop = system_clock::time_point{microseconds(6000)};

    status.localStatusSnapshots[GrpcAddr("worker-1:8080")] = {LocalQueryStatus{LocalQueryId(1), QueryState::Stopped, metrics1}};
    status.localStatusSnapshots[GrpcAddr("worker-2:8080")] = {LocalQueryStatus{LocalQueryId(2), QueryState::Stopped, metrics2}};

    auto coalescedMetrics = status.coalesceQueryMetrics();

    EXPECT_TRUE(coalescedMetrics.start.has_value());
    EXPECT_EQ(coalescedMetrics.start.value(), system_clock::time_point{microseconds(1000)});

    EXPECT_TRUE(coalescedMetrics.running.has_value());
    EXPECT_EQ(coalescedMetrics.running.value(), system_clock::time_point{microseconds(2500)});

    EXPECT_TRUE(coalescedMetrics.stop.has_value());
    EXPECT_EQ(coalescedMetrics.stop.value(), system_clock::time_point{microseconds(6000)});
}


}