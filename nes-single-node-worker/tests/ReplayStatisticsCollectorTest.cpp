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

#include <ReplayStatisticsCollector.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <CompiledQueryPlan.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryId.hpp>

namespace NES
{
namespace
{
QueryId makeQueryId(const char* uuid)
{
    return QueryId::createLocal(LocalQueryId(std::string_view(uuid)));
}

ChronoClock::time_point atMs(const int64_t millis)
{
    return ChronoClock::time_point(std::chrono::milliseconds(millis));
}

std::unique_ptr<CompiledQueryPlan> makeCompiledQueryPlan(QueryId queryId, PipelineId pipelineId, std::string replayFingerprint)
{
    auto pipeline = ExecutablePipeline::create(pipelineId, nullptr, {});
    pipeline->replayStatisticsFingerprint = std::move(replayFingerprint);
    return CompiledQueryPlan::create(queryId, {std::move(pipeline)}, {}, {});
}
}

TEST(ReplayStatisticsCollectorTest, AggregatesExecutionAndTupleStatisticsAcrossTasks)
{
    ReplayStatisticsCollector collector;
    const auto queryId = makeQueryId("00000000-0000-0000-0000-000000000101");
    const auto pipelineId = PipelineId(7);
    auto compiledQueryPlan = makeCompiledQueryPlan(queryId, pipelineId, "operator@worker-1");
    collector.registerCompiledQueryPlan(queryId, *compiledQueryPlan);

    TaskExecutionStart firstStart(WorkerThreadId(0), queryId, pipelineId, TaskId(1), 10);
    firstStart.timestamp = atMs(1000);
    collector.onEvent(firstStart);

    TaskEmit firstEmit(WorkerThreadId(0), queryId, pipelineId, PipelineId(9), TaskId(1), 4);
    firstEmit.timestamp = atMs(1002);
    collector.onEvent(firstEmit);

    TaskExecutionComplete firstComplete(WorkerThreadId(0), queryId, pipelineId, TaskId(1));
    firstComplete.timestamp = atMs(1005);
    collector.onEvent(firstComplete);

    TaskExecutionStart secondStart(WorkerThreadId(0), queryId, pipelineId, TaskId(2), 8);
    secondStart.timestamp = atMs(2000);
    collector.onEvent(secondStart);

    TaskExecutionComplete secondComplete(WorkerThreadId(0), queryId, pipelineId, TaskId(2));
    secondComplete.timestamp = atMs(2003);
    collector.onEvent(secondComplete);

    const auto snapshot = collector.snapshot();
    ASSERT_EQ(snapshot.size(), 1U);

    const auto& statistics = snapshot.front();
    EXPECT_EQ(statistics.nodeFingerprint, "operator@worker-1");
    EXPECT_EQ(statistics.inputTuples, 18U);
    EXPECT_EQ(statistics.outputTuples, 12U);
    EXPECT_EQ(statistics.taskCount, 2U);
    EXPECT_EQ(statistics.executionTimeNanos, 8'000'000U);
    EXPECT_DOUBLE_EQ(statistics.averageExecutionTimeMs(), 4.0);
    EXPECT_DOUBLE_EQ(statistics.averageInputTuples(), 9.0);
    EXPECT_DOUBLE_EQ(statistics.averageOutputTuples(), 6.0);
    EXPECT_DOUBLE_EQ(statistics.selectivity(), 12.0 / 18.0);
}

TEST(ReplayStatisticsCollectorTest, ClearsPendingTasksAndIgnoresEventsAfterQueryRemoval)
{
    ReplayStatisticsCollector collector;
    const auto queryId = makeQueryId("00000000-0000-0000-0000-000000000102");
    const auto pipelineId = PipelineId(11);
    auto compiledQueryPlan = makeCompiledQueryPlan(queryId, pipelineId, "operator@worker-2");
    collector.registerCompiledQueryPlan(queryId, *compiledQueryPlan);

    TaskExecutionStart start(WorkerThreadId(0), queryId, pipelineId, TaskId(1), 5);
    start.timestamp = atMs(3000);
    collector.onEvent(start);

    QueryStopRequest stopRequest(WorkerThreadId(0), queryId);
    stopRequest.timestamp = atMs(3001);
    collector.onEvent(stopRequest);

    TaskEmit staleEmit(WorkerThreadId(0), queryId, pipelineId, PipelineId(12), TaskId(1), 3);
    staleEmit.timestamp = atMs(3002);
    collector.onEvent(staleEmit);

    TaskExecutionComplete staleComplete(WorkerThreadId(0), queryId, pipelineId, TaskId(1));
    staleComplete.timestamp = atMs(3003);
    collector.onEvent(staleComplete);

    EXPECT_TRUE(collector.snapshot().empty());

    collector.unregisterQuery(queryId);

    TaskExecutionStart ignoredStart(WorkerThreadId(0), queryId, pipelineId, TaskId(2), 7);
    ignoredStart.timestamp = atMs(4000);
    collector.onEvent(ignoredStart);

    TaskExecutionComplete ignoredComplete(WorkerThreadId(0), queryId, pipelineId, TaskId(2));
    ignoredComplete.timestamp = atMs(4005);
    collector.onEvent(ignoredComplete);

    EXPECT_TRUE(collector.snapshot().empty());
}

}
