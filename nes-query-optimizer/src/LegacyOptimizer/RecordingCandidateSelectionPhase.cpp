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
#include <unordered_map>
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
struct RoutePlacement
{
    Host node{Host::INVALID};
    size_t upstreamHopCount = 0;
    size_t downstreamHopCount = 0;
};

struct MergedReplayContext
{
    std::unordered_map<std::string, size_t> structuralOccurrences;
    std::unordered_map<std::string, size_t> installedRecordings;

    [[nodiscard]] size_t mergedOccurrenceCount(std::string_view structuralFingerprint) const
    {
        return std::max<size_t>(1, structuralOccurrences.contains(std::string{structuralFingerprint}) ? structuralOccurrences.at(std::string{structuralFingerprint}) : 1);
    }

    [[nodiscard]] size_t activeReplayConsumerCount(std::string_view structuralFingerprint) const
    {
        return mergedOccurrenceCount(structuralFingerprint) + installedRecordingCount(structuralFingerprint);
    }

    [[nodiscard]] size_t installedRecordingCount(std::string_view structuralFingerprint) const
    {
        return installedRecordings.contains(std::string{structuralFingerprint}) ? installedRecordings.at(std::string{structuralFingerprint}) : 0;
    }
};

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

std::string recordingRepresentationDescription(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return "binary_store";
    }
    std::unreachable();
}

std::vector<RecordingRepresentation> supportedRecordingRepresentations()
{
    return {RecordingRepresentation::BinaryStore};
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
        .replayTimeMultiplier = estimate.replayTimeMultiplier,
        .boundaryCutCost = estimate.boundaryCutCost(),
        .replayRecomputeCost = estimate.replayRecomputeCost(),
        .fitsBudget = estimate.fitsBudget,
        .satisfiesReplayLatency = estimate.satisfiesReplayLatency};
}

std::string describeReplayReuseInfeasibility(
    const Host& placement,
    const RecordingRepresentation representation,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<WorkerRuntimeMetrics>& workerRuntimeMetrics,
    const RecordingCostEstimate& reuseCost)
{
    return fmt::format(
        "reuse_existing_recording on {} with representation {} would exceed the replay latency limit of {} ms (estimated latency {} ms,"
        " replay cost {:.2f}, recompute cost {:.2f}, replay-time multiplier {:.2f}, write pressure {} B/s)",
        placement,
        recordingRepresentationDescription(representation),
        replayLatencyLimitDescription(replaySpecification),
        reuseCost.estimatedReplayLatency.count(),
        reuseCost.replayCost,
        reuseCost.recomputeCost,
        reuseCost.replayTimeMultiplier,
        workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0));
}

std::string describeRecordingUpgradeInfeasibility(
    const Host& placement,
    const RecordingRepresentation representation,
    const WorkerConfig& worker,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<WorkerRuntimeMetrics>& workerRuntimeMetrics,
    const RecordingCostEstimate& upgradeCost,
    const std::optional<uint64_t> existingRetentionWindowMs)
{
    if (!upgradeCost.fitsBudget)
    {
        return fmt::format(
            "upgrade_existing_recording on {} with representation {} from retention {} ms to {} ms requires an estimated {} bytes, but only {} bytes remain"
            " in the recording budget of {} bytes",
            placement,
            recordingRepresentationDescription(representation),
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
        "upgrade_existing_recording on {} with representation {} from retention {} ms to {} ms would exceed the replay latency limit of {} ms"
        " (estimated latency {} ms at {} B/s, replay-time multiplier {:.2f})",
        placement,
        recordingRepresentationDescription(representation),
        existingRetentionWindowMs.value_or(0),
        requestedRetentionWindowMs(replaySpecification).value_or(0),
        replayLatencyLimitDescription(replaySpecification),
        upgradeCost.estimatedReplayLatency.count(),
        upgradeCost.estimatedReplayBandwidthBytesPerSecond,
        upgradeCost.replayTimeMultiplier);
}

std::string describeNewRecordingInfeasibility(
    const Host& placement,
    const RecordingRepresentation representation,
    const WorkerConfig& worker,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<WorkerRuntimeMetrics>& workerRuntimeMetrics,
    const RecordingCostEstimate& newRecordingCost)
{
    if (!newRecordingCost.fitsBudget)
    {
        return fmt::format(
            "create_new_recording on {} with representation {} requires an estimated {} bytes, but only {} bytes remain in the recording budget of {} bytes"
            " (maintenance cost {:.2f}, replay cost {:.2f}, recompute cost {:.2f}, worker currently reports {} bytes in replay storage"
            " across {} recording files, {} active queries, and {} B/s replay write pressure, replay-time multiplier {:.2f})",
            placement,
            recordingRepresentationDescription(representation),
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
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0),
            newRecordingCost.replayTimeMultiplier);
    }

    return fmt::format(
        "create_new_recording on {} with representation {} would exceed the replay latency limit of {} ms (estimated latency {} ms at {} B/s,"
        " maintenance cost {:.2f}, replay cost {:.2f}, recompute cost {:.2f}, replay-time multiplier {:.2f})",
        placement,
        recordingRepresentationDescription(representation),
        replayLatencyLimitDescription(replaySpecification),
        newRecordingCost.estimatedReplayLatency.count(),
        newRecordingCost.estimatedReplayBandwidthBytesPerSecond,
        newRecordingCost.maintenanceCost,
        newRecordingCost.replayCost,
        newRecordingCost.recomputeCost,
        newRecordingCost.replayTimeMultiplier);
}

std::vector<RoutePlacement> enumerateRoutePlacements(const Host& sourcePlacement, const Host& sinkPlacement, const NetworkTopology& topology)
{
    if (sourcePlacement == sinkPlacement)
    {
        return {{.node = sourcePlacement, .upstreamHopCount = 0, .downstreamHopCount = 0}};
    }

    const auto paths = topology.findPaths(sourcePlacement, sinkPlacement, NetworkTopology::Direction::Downstream);
    PRECONDITION(!paths.empty(), "Replay recording selection could not find a route from {} to {}", sourcePlacement, sinkPlacement);

    const auto& route = std::ranges::min_element(
                            paths,
                            {},
                            [](const NetworkTopology::Path& path) { return path.path.size(); })
                            ->path;

    std::vector<RoutePlacement> placements;
    placements.reserve(route.size());
    for (size_t index = 0; index < route.size(); ++index)
    {
        placements.push_back(RoutePlacement{
            .node = route.at(index),
            .upstreamHopCount = index,
            .downstreamHopCount = route.size() - index - 1});
    }
    return placements;
}

void collectMergedReplayOccurrences(
    const LogicalOperator& current,
    const NetworkTopology& topology,
    std::unordered_map<std::string, size_t>& structuralOccurrences)
{
    for (const auto& child : current.getChildren())
    {
        const auto routePlacements = enumerateRoutePlacements(getPlacementFor(child), getPlacementFor(current), topology);
        for (const auto& placement : routePlacements)
        {
            ++structuralOccurrences[createStructuralRecordingFingerprint(child, placement.node)];
        }
        collectMergedReplayOccurrences(child, topology, structuralOccurrences);
    }
}

MergedReplayContext buildMergedReplayContext(
    const LogicalPlan& currentPlan, const RecordingCatalog& recordingCatalog, const WorkerCatalog& workerCatalog)
{
    MergedReplayContext context;
    const auto topology = workerCatalog.getTopology();

    for (const auto& root : currentPlan.getRootOperators())
    {
        collectMergedReplayOccurrences(root, topology, context.structuralOccurrences);
    }
    for (const auto& metadata : recordingCatalog.getQueryMetadata() | std::views::values)
    {
        if (!metadata.globalPlan.has_value())
        {
            continue;
        }
        for (const auto& root : metadata.globalPlan->getRootOperators())
        {
            collectMergedReplayOccurrences(root, topology, context.structuralOccurrences);
        }
    }
    for (const auto& recording : recordingCatalog.getRecordings() | std::views::values)
    {
        ++context.installedRecordings[recording.structuralFingerprint];
    }

    return context;
}

RecordingBoundaryCandidate buildCandidate(
    const LogicalOperator& parent,
    const LogicalOperator& child,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingCatalog& recordingCatalog,
    const MergedReplayContext& mergedReplayContext,
    const WorkerCatalog& workerCatalog)
{
    const auto upstreamPlacement = getPlacementFor(child);
    const auto downstreamPlacement = getPlacementFor(parent);
    const auto costModel = RecordingCostModel{};
    const auto requestedRetention = requestedRetentionWindowMs(replaySpecification);
    const auto routePlacements = enumerateRoutePlacements(upstreamPlacement, downstreamPlacement, workerCatalog.getTopology());

    RecordingBoundaryCandidate candidate{
        .edge = {.parentId = parent.getId(), .childId = child.getId()},
        .upstreamNode = upstreamPlacement,
        .downstreamNode = downstreamPlacement,
        .routeNodes = routePlacements | std::views::transform([](const auto& placement) { return placement.node; }) | std::ranges::to<std::vector>(),
        .options = {}};

    for (const auto& routePlacement : routePlacements)
    {
        const auto worker = workerCatalog.getWorker(routePlacement.node);
        PRECONDITION(worker.has_value(), "Replay recording selection could not find worker {}", routePlacement.node);
        const auto workerRuntimeMetrics = workerCatalog.getWorkerRuntimeMetrics(routePlacement.node);

        for (const auto representation : supportedRecordingRepresentations())
        {
            const auto structuralFingerprint = createStructuralRecordingFingerprint(child, routePlacement.node);
            const auto structurallyCompatibleRecordings = recordingCatalog.getRecordingsByStructuralFingerprint(structuralFingerprint)
                | std::views::filter([&](const RecordingEntry& entry) { return entry.representation == representation; })
                | std::ranges::to<std::vector>();
            const auto recordingFingerprint = createRecordingFingerprint(child, routePlacement.node, replaySpecification);
            const auto recordingId = recordingIdFromFingerprint(recordingFingerprint);
            const auto placementContext = RecordingPlacementContext{
                .sourcePlacement = upstreamPlacement,
                .sinkPlacement = downstreamPlacement,
                .recordingPlacement = routePlacement.node,
                .upstreamHopCount = routePlacement.upstreamHopCount,
                .downstreamHopCount = routePlacement.downstreamHopCount,
                .totalRouteHopCount = routePlacements.size() - 1,
                .mergedOccurrenceCount = mergedReplayContext.mergedOccurrenceCount(structuralFingerprint),
                .activeReplayConsumerCount = mergedReplayContext.activeReplayConsumerCount(structuralFingerprint),
                .installedRecordingCount = mergedReplayContext.installedRecordingCount(structuralFingerprint),
                .representation = representation};
            const auto newSelection = RecordingSelection{
                .recordingId = recordingId,
                .node = routePlacement.node,
                .filePath = Replay::getRecordingFilePath(recordingId.getRawValue()),
                .structuralFingerprint = structuralFingerprint,
                .representation = representation};

            const auto newRecordingCost = costModel.estimateNewRecording(child, placementContext, *worker, workerRuntimeMetrics, replaySpecification);
            candidate.options.push_back(RecordingCandidateOption{
                .decision = RecordingSelectionDecision::CreateNewRecording,
                .selection = newSelection,
                .cost = toCostBreakdown(newRecordingCost),
                .feasible = newRecordingCost.fitsBudget && newRecordingCost.satisfiesReplayLatency,
                .infeasibilityReason = (newRecordingCost.fitsBudget && newRecordingCost.satisfiesReplayLatency)
                    ? std::string{}
                    : describeNewRecordingInfeasibility(
                        routePlacement.node, representation, *worker, replaySpecification, workerRuntimeMetrics, newRecordingCost)});

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
                    child, placementContext, *worker, workerRuntimeMetrics, replaySpecification, bestReuseRecording->retentionWindowMs);
                candidate.options.push_back(RecordingCandidateOption{
                    .decision = RecordingSelectionDecision::ReuseExistingRecording,
                    .selection = RecordingSelection{
                        .recordingId = bestReuseRecording->id,
                        .node = bestReuseRecording->node,
                        .filePath = bestReuseRecording->filePath,
                        .structuralFingerprint = bestReuseRecording->structuralFingerprint,
                        .representation = bestReuseRecording->representation},
                    .cost = toCostBreakdown(reuseCost),
                    .feasible = reuseCost.satisfiesReplayLatency,
                    .infeasibilityReason = reuseCost.satisfiesReplayLatency
                        ? std::string{}
                        : describeReplayReuseInfeasibility(
                            routePlacement.node, representation, replaySpecification, workerRuntimeMetrics, reuseCost)});
            }

            if (bestUpgradeRecording.has_value())
            {
                const auto upgradeCost = costModel.estimateRecordingUpgrade(
                    child, placementContext, *worker, workerRuntimeMetrics, replaySpecification, bestUpgradeRecording->retentionWindowMs);
                candidate.options.push_back(RecordingCandidateOption{
                    .decision = RecordingSelectionDecision::UpgradeExistingRecording,
                    .selection = newSelection,
                    .cost = toCostBreakdown(upgradeCost),
                    .feasible = upgradeCost.fitsBudget && upgradeCost.satisfiesReplayLatency,
                    .infeasibilityReason = (upgradeCost.fitsBudget && upgradeCost.satisfiesReplayLatency)
                        ? std::string{}
                        : describeRecordingUpgradeInfeasibility(
                            routePlacement.node,
                            representation,
                            *worker,
                            replaySpecification,
                            workerRuntimeMetrics,
                            upgradeCost,
                            bestUpgradeRecording->retentionWindowMs)});
            }
        }
    }

    return candidate;
}

void collectCandidates(
    const LogicalOperator& current,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingCatalog& recordingCatalog,
    const MergedReplayContext& mergedReplayContext,
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
            candidateSet.candidates.push_back(buildCandidate(current, child, replaySpecification, recordingCatalog, mergedReplayContext, workerCatalog));
        }
        collectCandidates(child, replaySpecification, recordingCatalog, mergedReplayContext, workerCatalog, candidateSet, visitedEdges, visitedLeaves);
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
    const auto mergedReplayContext = buildMergedReplayContext(placedPlan, recordingCatalog, *workerCatalog);
    collectCandidates(*roots.begin(), replaySpecification, recordingCatalog, mergedReplayContext, *workerCatalog, candidateSet, visitedEdges, visitedLeaves);
    return candidateSet;
}
}
