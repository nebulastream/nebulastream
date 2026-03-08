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

#include <algorithm>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <RecordingSelectionResult.hpp>

namespace NES
{

struct RecordingPlanEdge
{
    OperatorId parentId{INVALID_OPERATOR_ID};
    OperatorId childId{INVALID_OPERATOR_ID};

    [[nodiscard]] bool operator==(const RecordingPlanEdge& other) const = default;
};

struct RecordingCandidateOption
{
    RecordingSelectionDecision decision = RecordingSelectionDecision::CreateNewRecording;
    RecordingSelection selection;
    RecordingCostBreakdown cost;
    bool feasible = false;
    std::string infeasibilityReason;

    [[nodiscard]] bool operator==(const RecordingCandidateOption& other) const = default;
};

struct RecordingBoundaryCandidate
{
    struct QueryMaterializationTarget
    {
        std::string queryId;
        std::vector<RecordingPlanEdge> materializationEdges;

        [[nodiscard]] bool operator==(const QueryMaterializationTarget& other) const = default;
    };

    RecordingPlanEdge edge;
    Host upstreamNode{Host::INVALID};
    Host downstreamNode{Host::INVALID};
    std::vector<Host> routeNodes;
    std::vector<RecordingPlanEdge> materializationEdges;
    std::vector<QueryMaterializationTarget> activeQueryMaterializationTargets;
    std::vector<std::string> beneficiaryQueries;
    bool coversIncomingQuery = false;
    std::vector<RecordingCandidateOption> options;

    [[nodiscard]] bool hasFeasibleOptions() const;
    [[nodiscard]] bool operator==(const RecordingBoundaryCandidate& other) const = default;
};

struct RecordingCandidateSet
{
    struct ActiveQueryPlan
    {
        std::string queryId;
        LogicalPlan plan;

        [[nodiscard]] bool operator==(const ActiveQueryPlan& other) const = default;
    };

    struct OperatorReplayTime
    {
        OperatorId operatorId{INVALID_OPERATOR_ID};
        double replayTimeMs = 0.0;

        [[nodiscard]] bool operator==(const OperatorReplayTime& other) const = default;
    };

    OperatorId rootOperatorId{INVALID_OPERATOR_ID};
    std::optional<uint64_t> replayLatencyLimitMs;
    std::vector<OperatorId> leafOperatorIds;
    std::vector<RecordingPlanEdge> planEdges;
    std::vector<OperatorReplayTime> operatorReplayTimes;
    std::vector<ActiveQueryPlan> activeQueryPlans;
    std::vector<RecordingBoundaryCandidate> candidates;

    [[nodiscard]] bool empty() const { return candidates.empty(); }
    [[nodiscard]] bool operator==(const RecordingCandidateSet& other) const = default;
};

struct SelectedRecordingBoundary
{
    RecordingBoundaryCandidate candidate;
    RecordingCandidateOption chosenOption;
    std::vector<RecordingCandidateOption> alternatives;

    [[nodiscard]] bool operator==(const SelectedRecordingBoundary& other) const = default;
};

struct RecordingBoundarySelection
{
    std::vector<SelectedRecordingBoundary> selectedBoundary;
    double objectiveCost = 0.0;

    [[nodiscard]] bool empty() const { return selectedBoundary.empty(); }
    [[nodiscard]] bool operator==(const RecordingBoundarySelection& other) const = default;
};

inline bool RecordingBoundaryCandidate::hasFeasibleOptions() const
{
    return std::ranges::any_of(options, [](const auto& option) { return option.feasible; });
}

}
