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
#include <ranges>
#include <string>
#include <vector>

#include <Identifiers/Identifiers.hpp>
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
    RecordingPlanEdge edge;
    Host node{Host::INVALID};
    std::vector<RecordingCandidateOption> options;

    [[nodiscard]] bool hasFeasibleOptions() const;
    [[nodiscard]] bool operator==(const RecordingBoundaryCandidate& other) const = default;
};

struct RecordingCandidateSet
{
    OperatorId rootOperatorId{INVALID_OPERATOR_ID};
    std::vector<OperatorId> leafOperatorIds;
    std::vector<RecordingPlanEdge> planEdges;
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
