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

#include <LegacyOptimizer/RecordingCandidateSelectionPhase.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <LegacyOptimizer/OperatorPlacement.hpp>
#include <LegacyOptimizer/RecordingCostModel.hpp>
#include <LegacyOptimizer/RecordingFingerprint.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Replay/ReplayStorage.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>

namespace NES
{
namespace
{
struct RecordingPlanEdgeHash
{
    [[nodiscard]] size_t operator()(const RecordingPlanEdge& edge) const
    {
        return std::hash<uint64_t>{}(edge.parentId.getRawValue()) ^ (std::hash<uint64_t>{}(edge.childId.getRawValue()) << 1U);
    }
};

std::string replayLatencyLimitDescription(const std::optional<ReplaySpecification>& replaySpecification)
{
    return replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; })
        .transform([](const auto latencyLimitMs) { return std::to_string(latencyLimitMs); })
        .value_or("none");
}

std::optional<uint64_t> requestedRetentionWindowMs(const std::optional<ReplaySpecification>& replaySpecification)
{
    return replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; });
}

bool satisfiesRetentionCoverage(const std::optional<uint64_t> availableRetentionWindowMs, const std::optional<uint64_t> requestedRetentionWindowMs)
{
    return availableRetentionWindowMs.value_or(0) >= requestedRetentionWindowMs.value_or(0);
}

RecordingCostBreakdown toCostBreakdown(const RecordingCostEstimate& estimate)
{
    return RecordingCostBreakdown{
        .decisionCost = estimate.totalCost(),
        .estimatedStorageBytes = estimate.estimatedStorageBytes,
        .operatorCount = estimate.operatorCount,
        .estimatedReplayBandwidthBytesPerSecond = estimate.estimatedReplayBandwidthBytesPerSecond,
        .estimatedReplayLatencyMs = static_cast<uint64_t>(std::max<int64_t>(estimate.estimatedReplayLatency.count(), 0)),
        .maintenanceCost = estimate.maintenanceCost,
        .replayCost = estimate.replayCost,
        .recomputeCost = estimate.recomputeCost,
        .fitsBudget = estimate.fitsBudget,
        .satisfiesReplayLatency = estimate.satisfiesReplayLatency};
}

std::string describeReplayReuseInfeasibility(
    const Host& placement,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<WorkerRuntimeMetrics>& workerRuntimeMetrics,
    const RecordingCostEstimate& reuseCost)
{
    return fmt::format(
        "reuse_existing_recording on {} would exceed the replay latency limit of {} ms (estimated latency {} ms,"
        " replay cost {:.2f}, recompute cost {:.2f}, write pressure {} B/s)",
        placement,
        replayLatencyLimitDescription(replaySpecification),
        reuseCost.estimatedReplayLatency.count(),
        reuseCost.replayCost,
        reuseCost.recomputeCost,
        workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0));
}

std::string describeRecordingUpgradeInfeasibility(
    const Host& placement,
    const WorkerConfig& worker,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<WorkerRuntimeMetrics>& workerRuntimeMetrics,
    const RecordingCostEstimate& upgradeCost,
    const std::optional<uint64_t> existingRetentionWindowMs)
{
    if (!upgradeCost.fitsBudget)
    {
        return fmt::format(
            "upgrade_existing_recording on {} from retention {} ms to {} ms requires an estimated {} bytes, but only {} bytes remain"
            " in the recording budget of {} bytes",
            placement,
            existingRetentionWindowMs.value_or(0),
            requestedRetentionWindowMs(replaySpecification).value_or(0),
            upgradeCost.estimatedStorageBytes,
            worker.recordingStorageBudget
                - std::min(
                    worker.recordingStorageBudget,
                    workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0)),
            worker.recordingStorageBudget);
    }

    return fmt::format(
        "upgrade_existing_recording on {} from retention {} ms to {} ms would exceed the replay latency limit of {} ms"
        " (estimated latency {} ms at {} B/s)",
        placement,
        existingRetentionWindowMs.value_or(0),
        requestedRetentionWindowMs(replaySpecification).value_or(0),
        replayLatencyLimitDescription(replaySpecification),
        upgradeCost.estimatedReplayLatency.count(),
        upgradeCost.estimatedReplayBandwidthBytesPerSecond);
}

std::string describeNewRecordingInfeasibility(
    const Host& placement,
    const WorkerConfig& worker,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<WorkerRuntimeMetrics>& workerRuntimeMetrics,
    const RecordingCostEstimate& newRecordingCost)
{
    if (!newRecordingCost.fitsBudget)
    {
        return fmt::format(
            "create_new_recording on {} requires an estimated {} bytes, but only {} bytes remain in the recording budget of {} bytes"
            " (maintenance cost {:.2f}, replay cost {:.2f}, recompute cost {:.2f}, worker currently reports {} bytes in replay storage"
            " across {} recording files, {} active queries, and {} B/s replay write pressure)",
            placement,
            newRecordingCost.estimatedStorageBytes,
            worker.recordingStorageBudget
                - std::min(
                    worker.recordingStorageBudget,
                    workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0)),
            worker.recordingStorageBudget,
            newRecordingCost.maintenanceCost,
            newRecordingCost.replayCost,
            newRecordingCost.recomputeCost,
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0),
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingFileCount; }).value_or(0),
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.activeQueryCount; }).value_or(0),
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0));
    }

    return fmt::format(
        "create_new_recording on {} would exceed the replay latency limit of {} ms (estimated latency {} ms at {} B/s,"
        " maintenance cost {:.2f}, replay cost {:.2f}, recompute cost {:.2f})",
        placement,
        replayLatencyLimitDescription(replaySpecification),
        newRecordingCost.estimatedReplayLatency.count(),
        newRecordingCost.estimatedReplayBandwidthBytesPerSecond,
        newRecordingCost.maintenanceCost,
        newRecordingCost.replayCost,
        newRecordingCost.recomputeCost);
}

RecordingBoundaryCandidate buildCandidate(
    const LogicalOperator& parent,
    const LogicalOperator& child,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingCatalog& recordingCatalog,
    const WorkerCatalog& workerCatalog)
{
    const auto placement = getPlacementFor(parent);
    const auto worker = workerCatalog.getWorker(placement);
    PRECONDITION(worker.has_value(), "Replay recording selection could not find worker {}", placement);
    const auto workerRuntimeMetrics = workerCatalog.getWorkerRuntimeMetrics(placement);
    const auto structuralFingerprint = createStructuralRecordingFingerprint(child, placement);
    const auto recordingFingerprint = createRecordingFingerprint(child, placement, replaySpecification);
    const auto recordingId = recordingIdFromFingerprint(recordingFingerprint);
    const auto recordingFilePath = Replay::getRecordingFilePath(recordingId.getRawValue());
    const auto costModel = RecordingCostModel{};
    const auto requestedRetention = requestedRetentionWindowMs(replaySpecification);
    const auto structurallyCompatibleRecordings = recordingCatalog.getRecordingsByStructuralFingerprint(structuralFingerprint);

    RecordingBoundaryCandidate candidate{
        .edge = {.parentId = parent.getId(), .childId = child.getId()},
        .node = placement,
        .options = {}};

    const auto newSelection = RecordingSelection{
        .recordingId = recordingId, .node = placement, .filePath = recordingFilePath, .structuralFingerprint = structuralFingerprint};
    const auto newRecordingCost = costModel.estimateNewRecording(child, *worker, workerRuntimeMetrics, replaySpecification);
    candidate.options.push_back(RecordingCandidateOption{
        .decision = RecordingSelectionDecision::CreateNewRecording,
        .selection = newSelection,
        .cost = toCostBreakdown(newRecordingCost),
        .feasible = newRecordingCost.fitsBudget && newRecordingCost.satisfiesReplayLatency,
        .infeasibilityReason = (newRecordingCost.fitsBudget && newRecordingCost.satisfiesReplayLatency)
            ? std::string{}
            : describeNewRecordingInfeasibility(placement, *worker, replaySpecification, workerRuntimeMetrics, newRecordingCost)});

    std::optional<RecordingEntry> bestReuseRecording;
    std::optional<RecordingEntry> bestUpgradeRecording;
    for (const auto& existingRecording : structurallyCompatibleRecordings)
    {
        if (satisfiesRetentionCoverage(existingRecording.retentionWindowMs, requestedRetention))
        {
            if (!bestReuseRecording.has_value()
                || existingRecording.retentionWindowMs.value_or(0) < bestReuseRecording->retentionWindowMs.value_or(0))
            {
                bestReuseRecording = existingRecording;
            }
            continue;
        }

        if (!bestUpgradeRecording.has_value()
            || existingRecording.retentionWindowMs.value_or(0) > bestUpgradeRecording->retentionWindowMs.value_or(0))
        {
            bestUpgradeRecording = existingRecording;
        }
    }

    if (bestReuseRecording.has_value())
    {
        const auto reuseCost = costModel.estimateReplayReuse(
            child, *worker, workerRuntimeMetrics, replaySpecification, bestReuseRecording->retentionWindowMs);
        candidate.options.push_back(RecordingCandidateOption{
            .decision = RecordingSelectionDecision::ReuseExistingRecording,
            .selection = RecordingSelection{
                .recordingId = bestReuseRecording->id,
                .node = bestReuseRecording->node,
                .filePath = bestReuseRecording->filePath,
                .structuralFingerprint = bestReuseRecording->structuralFingerprint},
            .cost = toCostBreakdown(reuseCost),
            .feasible = reuseCost.satisfiesReplayLatency,
            .infeasibilityReason = reuseCost.satisfiesReplayLatency
                ? std::string{}
                : describeReplayReuseInfeasibility(placement, replaySpecification, workerRuntimeMetrics, reuseCost)});
    }

    if (bestUpgradeRecording.has_value())
    {
        const auto upgradeCost = costModel.estimateRecordingUpgrade(
            child, *worker, workerRuntimeMetrics, replaySpecification, bestUpgradeRecording->retentionWindowMs);
        candidate.options.push_back(RecordingCandidateOption{
            .decision = RecordingSelectionDecision::UpgradeExistingRecording,
            .selection = newSelection,
            .cost = toCostBreakdown(upgradeCost),
            .feasible = upgradeCost.fitsBudget && upgradeCost.satisfiesReplayLatency,
            .infeasibilityReason = (upgradeCost.fitsBudget && upgradeCost.satisfiesReplayLatency)
                ? std::string{}
                : describeRecordingUpgradeInfeasibility(
                    placement, *worker, replaySpecification, workerRuntimeMetrics, upgradeCost, bestUpgradeRecording->retentionWindowMs)});
    }

    return candidate;
}

void collectCandidates(
    const LogicalOperator& current,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingCatalog& recordingCatalog,
    const WorkerCatalog& workerCatalog,
    RecordingCandidateSet& candidateSet,
    std::unordered_set<RecordingPlanEdge, RecordingPlanEdgeHash>& visitedEdges,
    std::unordered_set<OperatorId>& visitedLeaves)
{
    const auto children = current.getChildren();
    if (children.empty())
    {
        if (visitedLeaves.insert(current.getId()).second)
        {
            candidateSet.leafOperatorIds.push_back(current.getId());
        }
        return;
    }

    for (const auto& child : children)
    {
        const RecordingPlanEdge edge{.parentId = current.getId(), .childId = child.getId()};
        if (visitedEdges.insert(edge).second)
        {
            candidateSet.planEdges.push_back(edge);
            candidateSet.candidates.push_back(buildCandidate(current, child, replaySpecification, recordingCatalog, workerCatalog));
        }
        collectCandidates(child, replaySpecification, recordingCatalog, workerCatalog, candidateSet, visitedEdges, visitedLeaves);
    }
}
}

RecordingCandidateSet RecordingCandidateSelectionPhase::apply(
    const LogicalPlan& placedPlan,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingCatalog& recordingCatalog) const
{
    if (!replaySpecification.has_value())
    {
        return {};
    }

    PRECONDITION(workerCatalog != nullptr, "Recording selection requires a valid worker catalog");

    const auto roots = placedPlan.getRootOperators();
    PRECONDITION(roots.size() == 1, "Replay recording selection requires a single sink root, but got {} roots", roots.size());
    PRECONDITION(roots.front().tryGetAs<SinkLogicalOperator>().has_value(), "Replay recording selection requires a sink root");

    RecordingCandidateSet candidateSet{};
    candidateSet.rootOperatorId = roots.front().getId();
    std::unordered_set<RecordingPlanEdge, RecordingPlanEdgeHash> visitedEdges;
    std::unordered_set<OperatorId> visitedLeaves;
    collectCandidates(*roots.begin(), replaySpecification, recordingCatalog, *workerCatalog, candidateSet, visitedEdges, visitedLeaves);
    return candidateSet;
}
}
