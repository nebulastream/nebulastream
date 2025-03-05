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
#include <cstdint>
#include <Util/Overloaded.hpp>
#include "PipelineStatistics.hpp"
#include "QueryEngineStatisticListener.hpp"

namespace NES::Runtime
{
namespace
{
void threadRoutine(
    const std::stop_token& token,
    folly::Synchronized<std::unordered_map<PipelineId, PipelineStatistics>>& pipelineStatistics,
    folly::Synchronized<std::unordered_map<QueryId, QueryInfo>> queryInfo,
    const TuplePerTaskComputer& tuplePerTaskComputer,
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
                [&](const SubmitQuerySystemEvent& submitQuerySystemEvent)
                {
                    const auto queryInfoLocked = queryInfo.wlock();
                    (*queryInfoLocked)[submitQuerySystemEvent.queryId].addQuerySLA(submitQuerySystemEvent.minThroughput, submitQuerySystemEvent.maxLatency);
                },
                [&](const TaskExecutionComplete& taskExecutionComplete)
                {
                    const auto pipelineId = taskExecutionComplete.pipelineId;
                    const auto queryId = taskExecutionComplete.queryId;
                    const PipelineStatistics::TaskStatistics taskStatistics{.throughput=taskExecutionComplete.throughput, .latency=taskExecutionComplete.latency, .numberOfTuples=taskExecutionComplete.numberOfTuplesProcessed};

                    /// Acquiring locks for the pipelineStatistics and queryInfo
                    auto [pipelineStatisticsLocked, queryInfoLocked] = folly::acquireLocked(pipelineStatistics, queryInfo);

                    /// Updating the current pipeline statistics and current throughput and latency for the query
                    /// For now, we assume that the current throughput and latency are the minimum throughput and maximum latency over all pipelines of a query
                    (*pipelineStatisticsLocked)[pipelineId].updateTaskStatistics(taskStatistics);
                    (*queryInfoLocked)[queryId].insert(pipelineId);
                    for (const auto& pipeline : (*queryInfoLocked)[queryId])
                    {
                        const auto averageThroughput = (*pipelineStatisticsLocked)[pipeline].getAverageThroughput();
                        const auto averageLatency = (*pipelineStatisticsLocked)[pipeline].getAverageLatency();
                        (*queryInfoLocked)[queryId].currentThroughput = (std::min((*queryInfoLocked)[queryId].currentThroughput, averageThroughput));
                        (*queryInfoLocked)[queryId].currentLatency = (std::max((*queryInfoLocked)[queryId].currentLatency, averageLatency));
                    }

                    /// Computing the new task sizes across all pipelines of all queries.
                    /// To ensure that the task sizes are computed in isolation, we pass the pipelineStatistics and queryInfo as arguments to the TuplePerTaskComputer
                    tuplePerTaskComputer.calculateOptimalTaskSize(*pipelineStatisticsLocked, *queryInfoLocked);
                },
                [](auto) {}},
            event);
    }
}
}

void TaskStatisticsProcessor::onEvent(Event event)
{
    events.blockingWrite(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

TaskStatisticsProcessor::TaskStatisticsProcessor(std::unique_ptr<TuplePerTaskComputer> tuplePerTaskComputer)
    : tuplePerTaskComputer(std::move(tuplePerTaskComputer)), printThread([this](const std::stop_token& stopToken) { threadRoutine(stopToken, pipelineStatistics, queryInfo, *this->tuplePerTaskComputer, events); })
{
}

uint64_t TaskStatisticsProcessor::getNumberOfTuplesPerBuffer(const PipelineId pipelineId) const
{
    const auto pipelineStatisticsLocked = pipelineStatistics.rlock();
    if (pipelineStatisticsLocked->contains(pipelineId))
    {
        return pipelineStatisticsLocked->at(pipelineId).getNextNumberOfTuplesPerTask();
    }
    return 42;
}

void QueryInfo::insert(const PipelineId pipelineId)
{
    pipelineIds.insert(pipelineId);
}

void QueryInfo::addQuerySLA(const double minThroughput, const std::chrono::microseconds maxLatency)
{
    sla.maxLatency = maxLatency;
    sla.minThroughput = minThroughput;
}
}