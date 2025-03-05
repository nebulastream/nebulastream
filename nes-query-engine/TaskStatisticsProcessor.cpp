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

#include "TaskStatisticsProcessor.hpp"
#include <Util/Overloaded.hpp>

namespace NES::Runtime
{
namespace
{
void threadRoutine(
    const std::stop_token& token,
    folly::Synchronized<std::unordered_map<PipelineId, PipelineStatistics>>& pipelineStatistics,
    folly::Synchronized<std::unordered_map<QueryId, std::set<PipelineId>>> queryPipelines,
    folly::MPMCQueue<TaskStatisticsProcessor::CombinedEventType>& events)
{
    while (!token.stop_requested())
    {
        TaskStatisticsProcessor::CombinedEventType event = QueryStart{WorkerThreadId(0), QueryId(0)}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(100), event))
        {
            continue;
        }
        std::visit(
            Overloaded{
                [&](TaskExecutionComplete taskExecutionComplete)
                {
                    const auto pipelineId = taskExecutionComplete.pipelineId;
                    const auto queryId = taskExecutionComplete.queryId;
                    const PipelineStatistics::TaskStatistics taskStatistics{
                        taskExecutionComplete.throughput, taskExecutionComplete.latency, taskExecutionComplete.numberOfTuplesProcessed};

                    auto [pipelineStatisticsLocked, queryPipelinesLocked] = folly::acquireLocked(pipelineStatistics, queryPipelines);
                    (*queryPipelinesLocked)[queryId].insert(pipelineId);
                    (*pipelineStatisticsLocked)[pipelineId].updateTaskStatistics(taskStatistics);
                },
                [](auto) {}},
            event);
    }
}
}

uint64_t TaskStatisticsProcessor::getNumberOfTuplesPerBuffer(const PipelineId pipelineId) const
{
    /// For now we just return a constant value
    ((void)pipelineId);
    return 42;
}

void PipelineStatistics::updateTaskStatistics(const TaskStatistics taskStatistic)
{
    if (storedTaskStatistics.size() == windowSizeRollingAverage)
    {
        const auto& [throughput, latency, numberOfTuples] = storedTaskStatistics.front();
        sumThroughput -= throughput;
        sumLatency -= latency;
        sumNumberOfTuples -= numberOfTuples;
        storedTaskStatistics.pop();
    }
    storedTaskStatistics.push(taskStatistic);
    sumLatency += taskStatistic.latency;
    sumThroughput += taskStatistic.throughput;
    sumNumberOfTuples += taskStatistic.numberOfTuples;
}

double PipelineStatistics::getAverageThroughput() const
{
    return sumThroughput / storedTaskStatistics.size();
}

double PipelineStatistics::getAverageLatency() const
{
    return sumLatency / storedTaskStatistics.size();
}

void TaskStatisticsProcessor::onEvent(Event event)
{
    events.blockingWrite(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

TaskStatisticsProcessor::TaskStatisticsProcessor()
    : printThread([this](const std::stop_token& stopToken) { threadRoutine(stopToken, pipelineStatistics, queryPipelines, events); })

{
}
}