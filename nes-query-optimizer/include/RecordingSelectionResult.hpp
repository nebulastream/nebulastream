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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

using RecordingId = NESStrongStringType<struct RecordingId_, "invalid">;

enum class RecordingRepresentation : uint8_t
{
    BinaryStore,
    BinaryStoreZstdFast1,
    BinaryStoreZstd,
    BinaryStoreZstdFast6,
};

enum class RecordingSelectionDecision
{
    CreateNewRecording,
    UpgradeExistingRecording,
    ReuseExistingRecording,
};

struct RecordingCostBreakdown
{
    double decisionCost = 0.0;
    size_t estimatedStorageBytes = 0;
    size_t incrementalStorageBytes = 0;
    size_t operatorCount = 0;
    size_t estimatedReplayBandwidthBytesPerSecond = 0;
    uint64_t estimatedReplayLatencyMs = 0;
    double maintenanceCost = 0.0;
    double replayCost = 0.0;
    double recomputeCost = 0.0;
    double replayTimeMultiplier = 1.0;
    double boundaryCutCost = 0.0;
    double replayRecomputeCost = 0.0;
    bool fitsBudget = false;
    bool satisfiesReplayLatency = true;

    [[nodiscard]] double totalCost() const { return decisionCost; }
    [[nodiscard]] bool operator==(const RecordingCostBreakdown& other) const = default;
};

struct RecordingSelection
{
    RecordingId recordingId{RecordingId::INVALID};
    Host node{Host::INVALID};
    std::string filePath;
    std::string structuralFingerprint;
    RecordingRepresentation representation = RecordingRepresentation::BinaryStore;
    std::optional<uint64_t> retentionWindowMs;
    std::vector<std::string> beneficiaryQueries;
    bool coversIncomingQuery = true;

    [[nodiscard]] bool operator==(const RecordingSelection& other) const = default;
};

struct RecordingRewriteEdge
{
    OperatorId parentId{INVALID_OPERATOR_ID};
    OperatorId childId{INVALID_OPERATOR_ID};

    [[nodiscard]] bool operator==(const RecordingRewriteEdge& other) const = default;
};

struct QueryRecordingPlanInsertion
{
    RecordingSelection selection;
    std::vector<RecordingRewriteEdge> materializationEdges;

    [[nodiscard]] bool operator==(const QueryRecordingPlanInsertion& other) const = default;
};

struct QueryRecordingPlanRewrite
{
    std::string queryId;
    LogicalPlan basePlan;
    std::vector<QueryRecordingPlanInsertion> insertions;

    [[nodiscard]] bool operator==(const QueryRecordingPlanRewrite& other) const = default;
};

struct RecordingSelectionAlternative
{
    RecordingSelectionDecision decision = RecordingSelectionDecision::CreateNewRecording;
    RecordingCostBreakdown cost;

    [[nodiscard]] bool operator==(const RecordingSelectionAlternative& other) const = default;
};

struct RecordingSelectionExplanation
{
    RecordingSelection selection;
    RecordingSelectionDecision decision = RecordingSelectionDecision::CreateNewRecording;
    std::string reason;
    RecordingCostBreakdown chosenCost;
    std::vector<RecordingSelectionAlternative> alternatives;

    [[nodiscard]] bool operator==(const RecordingSelectionExplanation& other) const = default;
};

struct RecordingSelectionResult
{
    std::vector<RecordingSelectionExplanation> networkExplanations;
    std::vector<RecordingSelection> selectedRecordings;
    std::vector<RecordingSelectionExplanation> explanations;
    std::vector<QueryRecordingPlanRewrite> activeQueryPlanRewrites;

    [[nodiscard]] bool empty() const { return selectedRecordings.empty() && networkExplanations.empty(); }
    [[nodiscard]] bool operator==(const RecordingSelectionResult& other) const = default;
};

}
