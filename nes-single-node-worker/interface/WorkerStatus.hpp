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

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Replay/ReplayCheckpoint.hpp>
#include <Replay/ReplayQueryStatus.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Replay/ReplayExecutionStatistics.hpp>
#include <Replay/RecordingRuntimeStatus.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{

struct WorkerStatus
{
    struct ReplayMetrics
    {
        size_t recordingStorageBytes = 0;
        size_t recordingFileCount = 0;
        size_t activeQueryCount = 0;
        uint64_t replayReadBytes = 0;
        std::vector<ReplayOperatorStatistics> operatorStatistics;
        std::vector<Replay::RecordingRuntimeStatus> recordingStatuses;
        std::vector<ReplayCheckpointStatus> replayCheckpoints;
        std::vector<ReplayQueryStatus> replayQueryStatuses;

        [[nodiscard]] bool operator==(const ReplayMetrics& other) const = default;
    };

    struct ActiveQuery
    {
        QueryId queryId = INVALID_QUERY_ID;
        /// If the query is still starting, it does not have a started timestamp yet
        std::optional<std::chrono::system_clock::time_point> started;
    };

    /// Terminated Queries contain all queries that have either stopped or failed
    struct TerminatedQuery
    {
        QueryId queryId = INVALID_QUERY_ID;
        /// If a query fails, it might not have a started timestamp if it failed during startup
        std::optional<std::chrono::system_clock::time_point> started;
        std::chrono::system_clock::time_point terminated;
        std::optional<Exception> error;
    };

    /// Currently we will not store all historical data on the WorkerNode.
    /// This timestamp indicates which events are captured by the WorkerStatus
    std::chrono::system_clock::time_point after;
    std::chrono::system_clock::time_point until;
    std::vector<ActiveQuery> activeQueries;
    std::vector<TerminatedQuery> terminatedQueries;
    ReplayMetrics replayMetrics;
};

void serializeWorkerStatus(const WorkerStatus& status, WorkerStatusResponse* response);
WorkerStatus deserializeWorkerStatus(const WorkerStatusResponse* response);

}
