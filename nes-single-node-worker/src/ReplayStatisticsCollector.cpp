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

#include <algorithm>
#include <chrono>
#include <ranges>
#include <vector>

#include <Util/Overloaded.hpp>

namespace NES
{
void ReplayStatisticsCollector::registerCompiledQueryPlan(QueryId queryId, const CompiledQueryPlan& compiledQueryPlan)
{
    std::scoped_lock lock(mutex);
    for (const auto& pipeline : compiledQueryPlan.pipelines)
    {
        if (!pipeline->replayStatisticsFingerprint.has_value())
        {
            continue;
        }
        state.pipelineFingerprints.insert_or_assign(
            PipelineKey{.queryId = queryId, .pipelineId = pipeline->id}, *pipeline->replayStatisticsFingerprint);
    }

    for (const auto& sink : compiledQueryPlan.sinks)
    {
        if (!sink.replayStatisticsFingerprint.has_value())
        {
            continue;
        }
        state.pipelineFingerprints.insert_or_assign(PipelineKey{.queryId = queryId, .pipelineId = sink.id}, *sink.replayStatisticsFingerprint);
    }
}

void ReplayStatisticsCollector::unregisterQuery(QueryId queryId)
{
    std::scoped_lock lock(mutex);
    std::erase_if(state.pipelineFingerprints, [&](const auto& entry) { return entry.first.queryId == queryId; });
    clearPendingTasksForQuery(state, queryId);
}

std::vector<ReplayOperatorStatistics> ReplayStatisticsCollector::snapshot() const
{
    std::scoped_lock lock(mutex);
    auto snapshot = state.statisticsByFingerprint | std::views::values | std::ranges::to<std::vector>();
    std::ranges::sort(snapshot, {}, &ReplayOperatorStatistics::nodeFingerprint);
    return snapshot;
}

void ReplayStatisticsCollector::onEvent(Event event)
{
    std::scoped_lock lock(mutex);
    std::visit(
        Overloaded{
            [&](const TaskExecutionStart& start)
            {
                const PipelineKey pipelineKey{.queryId = start.queryId, .pipelineId = start.pipelineId};
                if (!state.pipelineFingerprints.contains(pipelineKey))
                {
                    return;
                }
                state.pendingTasks.insert_or_assign(
                    TaskKey{.queryId = start.queryId, .pipelineId = start.pipelineId, .taskId = start.taskId},
                    PendingTask{.startedAt = start.timestamp, .inputTuples = start.numberOfTuples, .outputTuples = 0});
            },
            [&](const TaskEmit& emit)
            {
                const TaskKey taskKey{.queryId = emit.queryId, .pipelineId = emit.fromPipeline, .taskId = emit.taskId};
                if (const auto it = state.pendingTasks.find(taskKey); it != state.pendingTasks.end())
                {
                    it->second.outputTuples += emit.numberOfTuples;
                }
            },
            [&](const TaskExecutionComplete& complete)
            {
                const PipelineKey pipelineKey{.queryId = complete.queryId, .pipelineId = complete.pipelineId};
                const auto fingerprintIt = state.pipelineFingerprints.find(pipelineKey);
                if (fingerprintIt == state.pipelineFingerprints.end())
                {
                    return;
                }

                const TaskKey taskKey{.queryId = complete.queryId, .pipelineId = complete.pipelineId, .taskId = complete.taskId};
                const auto pendingIt = state.pendingTasks.find(taskKey);
                if (pendingIt == state.pendingTasks.end())
                {
                    return;
                }

                const auto elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(complete.timestamp - pendingIt->second.startedAt).count();
                const auto outputTuples = pendingIt->second.outputTuples == 0 ? pendingIt->second.inputTuples : pendingIt->second.outputTuples;
                auto& statistics = state.statisticsByFingerprint[fingerprintIt->second];
                statistics.nodeFingerprint = fingerprintIt->second;
                statistics.inputTuples += pendingIt->second.inputTuples;
                statistics.outputTuples += outputTuples;
                statistics.taskCount += 1;
                statistics.executionTimeNanos += elapsedNanos > 0 ? static_cast<uint64_t>(elapsedNanos) : 0;
                state.pendingTasks.erase(pendingIt);
            },
            [&](const TaskExpired& expired)
            {
                state.pendingTasks.erase(TaskKey{.queryId = expired.queryId, .pipelineId = expired.pipelineId, .taskId = expired.taskId});
            },
            [&](const QueryStopRequest& stopRequest) { clearPendingTasksForQuery(state, stopRequest.queryId); },
            [&](const QueryStop& stop) { clearPendingTasksForQuery(state, stop.queryId); },
            [&](const QueryFail& failed) { clearPendingTasksForQuery(state, failed.queryId); },
            [&](const auto&) {}},
        std::move(event));
}

void ReplayStatisticsCollector::clearPendingTasksForQuery(State& currentState, QueryId queryId) const
{
    std::erase_if(currentState.pendingTasks, [&](const auto& entry) { return entry.first.queryId == queryId; });
}

}
