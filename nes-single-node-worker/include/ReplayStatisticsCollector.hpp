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

#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#include <QueryEngineStatisticListener.hpp>
#include <Replay/ReplayQueryStatus.hpp>
#include <Replay/ReplayExecutionStatistics.hpp>
#include <CompiledQueryPlan.hpp>

namespace NES
{

class ReplayStatisticsCollector final : public QueryEngineStatisticListener
{
public:
    void registerCompiledQueryPlan(QueryId queryId, const CompiledQueryPlan& compiledQueryPlan);
    void registerReplayQuery(QueryId queryId, ReplayQueryRuntimeControl runtimeControl);
    void unregisterQuery(QueryId queryId);
    [[nodiscard]] std::vector<ReplayOperatorStatistics> snapshot() const;
    [[nodiscard]] std::vector<ReplayQueryStatus> snapshotReplayQueries() const;

    void onEvent(Event event) override;

private:
    struct PipelineKey
    {
        QueryId queryId{INVALID_QUERY_ID};
        PipelineId pipelineId{INVALID<PipelineId>};

        [[nodiscard]] bool operator==(const PipelineKey& other) const = default;
    };

    struct PipelineKeyHash
    {
        [[nodiscard]] size_t operator()(const PipelineKey& key) const noexcept
        {
            return std::hash<QueryId>{}(key.queryId) ^ (std::hash<PipelineId>{}(key.pipelineId) << 1U);
        }
    };

    struct TaskKey
    {
        QueryId queryId{INVALID_QUERY_ID};
        PipelineId pipelineId{INVALID<PipelineId>};
        TaskId taskId{INVALID<TaskId>};

        [[nodiscard]] bool operator==(const TaskKey& other) const = default;
    };

    struct TaskKeyHash
    {
        [[nodiscard]] size_t operator()(const TaskKey& key) const noexcept
        {
            return std::hash<QueryId>{}(key.queryId) ^ (std::hash<PipelineId>{}(key.pipelineId) << 1U)
                ^ (std::hash<TaskId>{}(key.taskId) << 2U);
        }
    };

    struct PendingTask
    {
        ChronoClock::time_point startedAt{};
        uint64_t inputTuples = 0;
        uint64_t outputTuples = 0;
    };

    struct State
    {
        std::unordered_map<PipelineKey, std::string, PipelineKeyHash> pipelineFingerprints;
        std::unordered_map<TaskKey, PendingTask, TaskKeyHash> pendingTasks;
        std::unordered_map<std::string, ReplayOperatorStatistics> statisticsByFingerprint;
        std::unordered_map<QueryId, ReplayQueryRuntimeControl> replayQueryControls;
        std::unordered_map<QueryId, ReplayQueryStatus> replayQueryStatuses;
    };

    void clearPendingTasksForQuery(State& state, QueryId queryId) const;

    mutable std::mutex mutex;
    State state;
};

}
