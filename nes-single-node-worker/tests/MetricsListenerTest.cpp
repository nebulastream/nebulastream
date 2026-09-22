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

#include <cstddef>
#include <thread>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <MetricsListener.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>

namespace NES
{

namespace
{
QueryId randomQueryId()
{
    return QueryId::createLocal(LocalQueryId(generateUUID()));
}
}

TEST(MetricsListenerTest, countsTuplesTasksAndExpirations)
{
    MetricsListener listener;
    const auto queryId = randomQueryId();
    const auto otherQueryId = randomQueryId();
    const auto threadId = WorkerThreadId(1);
    const auto pipelineId = PipelineId(1);

    listener.onEvent(TaskExecutionStart{threadId, queryId, pipelineId, TaskId(1), 100});
    listener.onEvent(TaskExecutionComplete{threadId, queryId, pipelineId, TaskId(1)});
    listener.onEvent(TaskExecutionStart{threadId, queryId, pipelineId, TaskId(2), 23});
    /// Task 2 never completes (e.g. its pipeline threw): it must not count as processed.
    listener.onEvent(TaskExpired{threadId, queryId, pipelineId, TaskId(3)});
    listener.onEvent(TaskExecutionStart{threadId, otherQueryId, pipelineId, TaskId(4), 7});
    listener.onEvent(TaskExecutionComplete{threadId, otherQueryId, pipelineId, TaskId(4)});

    const auto counters = listener.getCounters(queryId);
    EXPECT_EQ(counters.processedTuples, 123);
    EXPECT_EQ(counters.processedTasks, 1);
    EXPECT_EQ(counters.expiredTasks, 1);

    const auto otherCounters = listener.getCounters(otherQueryId);
    EXPECT_EQ(otherCounters.processedTuples, 7);
    EXPECT_EQ(otherCounters.processedTasks, 1);
    EXPECT_EQ(otherCounters.expiredTasks, 0);
}

TEST(MetricsListenerTest, unknownQueryYieldsZeros)
{
    const MetricsListener listener;
    const auto counters = listener.getCounters(randomQueryId());
    EXPECT_EQ(counters.processedTuples, 0);
    EXPECT_EQ(counters.processedTasks, 0);
    EXPECT_EQ(counters.expiredTasks, 0);
}

TEST(MetricsListenerTest, largeThreadIdsAreShardedNotOutOfBounds)
{
    /// A worker id far beyond the shard count must still land in a valid shard (modulo indexing),
    /// not read out of bounds, and its events must still be counted.
    MetricsListener listener;
    const auto queryId = randomQueryId();
    const auto bigThreadId = WorkerThreadId(1U << 20);
    listener.onEvent(TaskExecutionStart{bigThreadId, queryId, PipelineId(1), TaskId(1), 42});
    listener.onEvent(TaskExecutionComplete{bigThreadId, queryId, PipelineId(1), TaskId(1)});
    EXPECT_EQ(listener.getCounters(queryId).processedTuples, 42);
    EXPECT_EQ(listener.getCounters(queryId).processedTasks, 1);
}

TEST(MetricsListenerTest, concurrentEventsAreCounted)
{
    MetricsListener listener;
    const auto queryId = randomQueryId();
    constexpr size_t threads = 8;
    constexpr size_t eventsPerThread = 1000;
    std::vector<std::jthread> workers;
    workers.reserve(threads);
    for (size_t threadIndex = 0; threadIndex < threads; ++threadIndex)
    {
        workers.emplace_back(
            [&listener, threadIndex, &queryId]
            {
                for (size_t i = 0; i < eventsPerThread; ++i)
                {
                    listener.onEvent(TaskExecutionStart{WorkerThreadId(threadIndex), queryId, PipelineId(1), TaskId(i + 1), 1});
                    listener.onEvent(TaskExecutionComplete{WorkerThreadId(threadIndex), queryId, PipelineId(1), TaskId(i + 1)});
                }
            });
    }
    workers.clear();

    EXPECT_EQ(listener.getCounters(queryId).processedTuples, threads * eventsPerThread);
    EXPECT_EQ(listener.getCounters(queryId).processedTasks, threads * eventsPerThread);
}

}
