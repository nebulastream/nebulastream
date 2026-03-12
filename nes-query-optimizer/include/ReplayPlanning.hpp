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

#include <cstdint>
#include <expected>
#include <optional>
#include <vector>

#include <RecordingCatalog.hpp>
#include <Replay/ReplayCheckpoint.hpp>
#include <RecordingSelectionResult.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
class WorkerCatalog;

struct ReplayPlan
{
    QueryRecordingPlanRewrite queryPlanRewrite;
    std::vector<QueryRecordingPlanInsertion> selectedReplayBoundaries;
    std::vector<RecordingSelectionExplanation> explanations;
    std::optional<ReplayCheckpointReference> selectedCheckpoint;
    uint64_t warmupStartMs = 0;
    std::vector<RecordingId> selectedRecordingIds;
    double objectiveCost = 0.0;

    [[nodiscard]] bool operator==(const ReplayPlan& other) const = default;
};

class ReplayPlanner
{
public:
    explicit ReplayPlanner(SharedPtr<const WorkerCatalog> workerCatalog) : workerCatalog(std::move(workerCatalog)) { }

    [[nodiscard]] std::expected<ReplayPlan, Exception> plan(
        const ReplayableQueryMetadata& queryMetadata,
        const RecordingCatalog& recordingCatalog,
        uint64_t intervalStartMs,
        uint64_t intervalEndMs) const;

private:
    SharedPtr<const WorkerCatalog> workerCatalog;
};
}
