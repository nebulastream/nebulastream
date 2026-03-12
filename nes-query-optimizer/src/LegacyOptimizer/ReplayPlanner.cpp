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

#include <ReplayPlanning.hpp>

#include <algorithm>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <LegacyOptimizer/OperatorPlacement.hpp>
#include <LegacyOptimizer/RecordingBoundarySolver.hpp>
#include <LegacyOptimizer/RecordingCostModel.hpp>
#include <LegacyOptimizer/RecordingSelectionTypes.hpp>
#include <ReplayCheckpointPlanning.hpp>
#include <Replay/ReplayNodeFingerprint.hpp>
#include <WorkerCatalog.hpp>
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

struct PlanGraph
{
    OperatorId rootOperatorId{INVALID_OPERATOR_ID};
    std::vector<OperatorId> leafOperatorIds{};
    std::vector<RecordingPlanEdge> planEdges{};
    std::unordered_map<OperatorId, LogicalOperator> operatorsById{};
};

struct RecordingPlanEdgeHash
{
    [[nodiscard]] size_t operator()(const RecordingPlanEdge& edge) const
    {
        return std::hash<uint64_t>{}(edge.parentId.getRawValue()) ^ (std::hash<uint64_t>{}(edge.childId.getRawValue()) << 1U);
    }
};

void collectPlanGraph(
    const LogicalOperator& current,
    PlanGraph& planGraph,
    std::unordered_set<OperatorId>& visitedNodes,
    std::unordered_set<RecordingPlanEdge, RecordingPlanEdgeHash>& visitedEdges)
{
    planGraph.operatorsById.insert_or_assign(current.getId(), current);
    const auto insertedNode = visitedNodes.insert(current.getId()).second;
    if (!insertedNode)
    {
        return;
    }

    const auto children = current.getChildren();
    if (children.empty() && !std::ranges::contains(planGraph.leafOperatorIds, current.getId()))
    {
        planGraph.leafOperatorIds.push_back(current.getId());
    }

    for (const auto& child : children)
    {
        const auto edge = RecordingPlanEdge{.parentId = current.getId(), .childId = child.getId()};
        if (visitedEdges.insert(edge).second)
        {
            planGraph.planEdges.push_back(edge);
        }
        collectPlanGraph(child, planGraph, visitedNodes, visitedEdges);
    }
}

std::expected<PlanGraph, Exception> buildPlanGraph(const QueryRecordingPlanRewrite& queryPlanRewrite)
{
    const auto& roots = queryPlanRewrite.basePlan.getRootOperators();
    if (roots.size() != 1U)
    {
        return std::unexpected(UnsupportedQuery(
            "Replay planning currently supports a single-root replay base plan, but found {} roots",
            roots.size()));
    }

    PlanGraph planGraph{.rootOperatorId = roots.front().getId()};
    std::unordered_set<OperatorId> visitedNodes;
    std::unordered_set<RecordingPlanEdge, RecordingPlanEdgeHash> visitedEdges;
    collectPlanGraph(roots.front(), planGraph, visitedNodes, visitedEdges);
    if (planGraph.leafOperatorIds.empty())
    {
        return std::unexpected(UnsupportedQuery("Replay planning requires at least one leaf operator in the replay base plan"));
    }
    return planGraph;
}

std::vector<RoutePlacement> enumerateRoutePlacements(const Host& sourcePlacement, const Host& sinkPlacement, const NetworkTopology& topology)
{
    if (sourcePlacement == sinkPlacement)
    {
        return {{.node = sourcePlacement, .upstreamHopCount = 0, .downstreamHopCount = 0}};
    }

    const auto paths = topology.findPaths(sourcePlacement, sinkPlacement, NetworkTopology::Direction::Downstream);
    PRECONDITION(!paths.empty(), "Replay planner could not find a route from {} to {}", sourcePlacement, sinkPlacement);

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

RecordingCostBreakdown toReplayCostBreakdown(const RecordingCostEstimate& estimate)
{
    return RecordingCostBreakdown{
        .decisionCost = estimate.replayCost,
        .estimatedStorageBytes = estimate.estimatedStorageBytes,
        .incrementalStorageBytes = 0,
        .operatorCount = estimate.operatorCount,
        .estimatedReplayBandwidthBytesPerSecond = estimate.estimatedReplayBandwidthBytesPerSecond,
        .estimatedReplayLatencyMs = static_cast<uint64_t>(std::max<int64_t>(estimate.estimatedReplayLatency.count(), 0)),
        .maintenanceCost = estimate.replayCost,
        .replayCost = estimate.replayCost,
        .recomputeCost = estimate.recomputeCost,
        .replayTimeMultiplier = estimate.replayTimeMultiplier,
        .boundaryCutCost = estimate.replayCost,
        .replayRecomputeCost = estimate.replayRecomputeCost(),
        .fitsBudget = true,
        .satisfiesReplayLatency = estimate.satisfiesReplayLatency};
}

std::optional<ReplaySpecification> replaySpecificationForInterval(
    const ReplayableQueryMetadata& queryMetadata, const uint64_t scanStartMs, const uint64_t intervalEndMs)
{
    if (intervalEndMs <= scanStartMs)
    {
        return std::nullopt;
    }

    auto replaySpecification = queryMetadata.replaySpecification.value_or(ReplaySpecification{});
    replaySpecification.retentionWindowMs = intervalEndMs - scanStartMs;
    return replaySpecification;
}

std::expected<RecordingCandidateOption, std::string> buildReplayOption(
    const ReplayableQueryMetadata& queryMetadata,
    const RecordingEntry& recording,
    const LogicalOperator& recordedSubplanRoot,
    const Host& sourcePlacement,
    const Host& sinkPlacement,
    const WorkerCatalog& workerCatalog,
    const uint64_t intervalStartMs,
    const uint64_t intervalEndMs)
{
    if (recording.lifecycleState != Replay::RecordingLifecycleState::Ready)
    {
        return std::unexpected(
            fmt::format("recording {} is not READY", recording.id));
    }
    if (recording.filePath.empty())
    {
        return std::unexpected(fmt::format("recording {} has no concrete file path", recording.id));
    }
    if (!recording.retainedStartWatermark.has_value() || !recording.retainedEndWatermark.has_value() || !recording.fillWatermark.has_value())
    {
        return std::unexpected(fmt::format("recording {} does not expose retained-range watermarks", recording.id));
    }
    if (recording.retainedStartWatermark->getRawValue() > intervalStartMs)
    {
        return std::unexpected(
            fmt::format(
                "recording {} only retains data from {} ms onward, which does not cover replay start {} ms",
                recording.id,
                recording.retainedStartWatermark->getRawValue(),
                intervalStartMs));
    }
    if (recording.retainedEndWatermark->getRawValue() < intervalEndMs)
    {
        return std::unexpected(
            fmt::format(
                "recording {} only retains data until {} ms, which does not cover replay end {} ms",
                recording.id,
                recording.retainedEndWatermark->getRawValue(),
                intervalEndMs));
    }
    if (recording.fillWatermark->getRawValue() < intervalEndMs)
    {
        return std::unexpected(
            fmt::format(
                "recording {} is only filled until {} ms, which does not cover replay end {} ms",
                recording.id,
                recording.fillWatermark->getRawValue(),
                intervalEndMs));
    }

    const auto worker = workerCatalog.getWorker(recording.node);
    if (!worker.has_value())
    {
        return std::unexpected(fmt::format("recording {} is placed on unknown worker {}", recording.id, recording.node));
    }

    const auto routePlacements = enumerateRoutePlacements(sourcePlacement, sinkPlacement, workerCatalog.getTopology());
    const auto routePlacementIt = std::ranges::find(routePlacements, recording.node, &RoutePlacement::node);
    if (routePlacementIt == routePlacements.end())
    {
        return std::unexpected(
            fmt::format(
                "recording {} on {} is not on the replay route from {} to {}",
                recording.id,
                recording.node,
                sourcePlacement,
                sinkPlacement));
    }

    const auto replaySpecification = replaySpecificationForInterval(
        queryMetadata,
        recording.retainedStartWatermark->getRawValue(),
        intervalEndMs);
    if (!replaySpecification.has_value())
    {
        return std::unexpected(fmt::format("recording {} yields an empty replay scan window", recording.id));
    }

    const auto placementContext = RecordingPlacementContext{
        .sourcePlacement = sourcePlacement,
        .sinkPlacement = sinkPlacement,
        .recordingPlacement = recording.node,
        .estimatedIngressWriteBytesPerSecond = 0,
        .upstreamHopCount = routePlacementIt->upstreamHopCount,
        .downstreamHopCount = routePlacementIt->downstreamHopCount,
        .totalRouteHopCount = routePlacements.size() - 1,
        .mergedOccurrenceCount = 1,
        .activeReplayConsumerCount = 1,
        .installedRecordingCount = 0,
        .representation = recording.representation};
    const auto costEstimate = RecordingCostModel{}.estimateReplayReuse(
        recordedSubplanRoot,
        placementContext,
        *worker,
        workerCatalog.getWorkerRuntimeMetrics(recording.node),
        replaySpecification,
        replaySpecification->retentionWindowMs);
    const auto selection = RecordingSelection{
        .recordingId = recording.id,
        .node = recording.node,
        .filePath = recording.filePath,
        .structuralFingerprint = recording.structuralFingerprint,
        .representation = recording.representation,
        .retentionWindowMs = recording.retentionWindowMs,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    return RecordingCandidateOption{
        .decision = RecordingSelectionDecision::ReuseExistingRecording,
        .selection = selection,
        .cost = toReplayCostBreakdown(costEstimate),
        .feasible = costEstimate.satisfiesReplayLatency,
        .infeasibilityReason = costEstimate.satisfiesReplayLatency
            ? std::string{}
            : fmt::format(
                "recording {} exceeds the replay latency limit with estimated replay latency {} ms",
                recording.id,
                costEstimate.estimatedReplayLatency.count())};
}

std::vector<RecordingCandidateOption> replayOptionsForBoundary(
    const ReplayableQueryMetadata& queryMetadata,
    const QueryRecordingPlanInsertion& replayBoundary,
    const LogicalOperator& recordedSubplanRoot,
    const Host& sourcePlacement,
    const Host& sinkPlacement,
    const RecordingCatalog& recordingCatalog,
    const WorkerCatalog& workerCatalog,
    const uint64_t intervalStartMs,
    const uint64_t intervalEndMs)
{
    std::vector<RecordingCandidateOption> options;
    for (const auto& recording : recordingCatalog.getRecordingsByStructuralFingerprint(replayBoundary.selection.structuralFingerprint))
    {
        if (recording.structuralFingerprint != replayBoundary.selection.structuralFingerprint)
        {
            continue;
        }

        if (const auto option = buildReplayOption(
                queryMetadata,
                recording,
                recordedSubplanRoot,
                sourcePlacement,
                sinkPlacement,
                workerCatalog,
                intervalStartMs,
                intervalEndMs);
            option.has_value())
        {
            options.push_back(*option);
        }
        else
        {
            options.push_back(RecordingCandidateOption{
                .decision = RecordingSelectionDecision::ReuseExistingRecording,
                .selection =
                    RecordingSelection{
                        .recordingId = recording.id,
                        .node = recording.node,
                        .filePath = recording.filePath,
                        .structuralFingerprint = recording.structuralFingerprint,
                        .representation = recording.representation,
                        .retentionWindowMs = recording.retentionWindowMs,
                        .beneficiaryQueries = {},
                        .coversIncomingQuery = true},
                .cost = {},
                .feasible = false,
                .infeasibilityReason = option.error()});
        }
    }

    if (!options.empty())
    {
        return options;
    }

    return {RecordingCandidateOption{
        .decision = RecordingSelectionDecision::ReuseExistingRecording,
        .selection = replayBoundary.selection,
        .cost = {},
        .feasible = false,
        .infeasibilityReason = fmt::format(
            "no READY maintained recording matches structural fingerprint {}",
            replayBoundary.selection.structuralFingerprint)}};
}

std::vector<RecordingCandidateSet::OperatorReplayTime>
operatorReplayTimesForPlan(const PlanGraph& planGraph, const WorkerCatalog& workerCatalog)
{
    std::vector<RecordingCandidateSet::OperatorReplayTime> operatorReplayTimes;
    operatorReplayTimes.reserve(planGraph.operatorsById.size());
    for (const auto& [operatorId, logicalOperator] : planGraph.operatorsById)
    {
        const auto placement = getPlacementFor(logicalOperator);
        if (placement == Host(Host::INVALID))
        {
            continue;
        }

        const auto nodeFingerprint = Replay::createStructuralReplayNodeFingerprint(logicalOperator);
        if (nodeFingerprint.empty())
        {
            continue;
        }

        const auto statistics = workerCatalog.getReplayOperatorStatistics(placement, nodeFingerprint);
        if (!statistics.has_value() || statistics->averageExecutionTimeMs() <= 0.0)
        {
            continue;
        }
        operatorReplayTimes.push_back(RecordingCandidateSet::OperatorReplayTime{
            .operatorId = operatorId,
            .replayTimeMs = statistics->averageExecutionTimeMs()});
    }
    return operatorReplayTimes;
}

std::string replayPlanningReason(
    const SelectedRecordingBoundary& selectedBoundary, const size_t candidateCount, const double objectiveCost)
{
    return fmt::format(
        "selected by replay min-cut over {} candidate edges with total cut cost {:.2f}; reuse_existing_recording on {} was the"
        " lowest-cost feasible replay option for this boundary",
        candidateCount,
        objectiveCost,
        selectedBoundary.chosenOption.selection.node);
}

RecordingSelectionExplanation buildReplayPlanningExplanation(
    const SelectedRecordingBoundary& selectedBoundary, const size_t candidateCount, const double objectiveCost)
{
    return RecordingSelectionExplanation{
        .selection = selectedBoundary.chosenOption.selection,
        .decision = selectedBoundary.chosenOption.decision,
        .reason = replayPlanningReason(selectedBoundary, candidateCount, objectiveCost),
        .chosenCost = selectedBoundary.chosenOption.cost,
        .alternatives =
            selectedBoundary.alternatives
            | std::views::transform(
                  [](const auto& alternative)
                  {
                      return RecordingSelectionAlternative{.decision = alternative.decision, .cost = alternative.cost};
                  })
            | std::ranges::to<std::vector>()};
}

std::vector<ReplayCheckpointReference> selectReplayCheckpoints(
    const ReplayableQueryMetadata& queryMetadata,
    const WorkerCatalog& workerCatalog,
    const uint64_t intervalStartMs)
{
    if (!queryMetadata.queryPlanRewrite.has_value() || queryMetadata.replayCheckpointRequirements.empty())
    {
        return {};
    }

    std::unordered_map<Host, std::vector<ReplayCheckpointStatus>> replayCheckpointsByHost;
    std::vector<uint64_t> candidateWatermarks;
    std::unordered_set<Host> replayCheckpointHosts;
    for (const auto& requirement : queryMetadata.replayCheckpointRequirements)
    {
        const auto host = Host(requirement.host);
        if (!replayCheckpointHosts.insert(host).second)
        {
            continue;
        }

        const auto runtimeMetrics = workerCatalog.getWorkerRuntimeMetrics(host);
        if (!runtimeMetrics.has_value())
        {
            return {};
        }

        auto& hostCheckpoints = replayCheckpointsByHost[host];
        for (const auto& replayCheckpoint : runtimeMetrics->replayCheckpoints)
        {
            if (!replayCheckpoint.queryId.isDistributed()
                || replayCheckpoint.queryId.getDistributedQueryId() != DistributedQueryId(queryMetadata.queryPlanRewrite->queryId)
                || replayCheckpoint.checkpointWatermarkMs > intervalStartMs || replayCheckpoint.replayRestoreFingerprint.empty())
            {
                continue;
            }
            hostCheckpoints.push_back(replayCheckpoint);
            candidateWatermarks.push_back(replayCheckpoint.checkpointWatermarkMs);
        }
    }

    if (candidateWatermarks.empty())
    {
        return {};
    }

    std::ranges::sort(candidateWatermarks);
    candidateWatermarks.erase(std::unique(candidateWatermarks.begin(), candidateWatermarks.end()), candidateWatermarks.end());

    auto requirements = queryMetadata.replayCheckpointRequirements;
    std::ranges::sort(
        requirements,
        [](const auto& lhs, const auto& rhs)
        {
            if (lhs.host != rhs.host)
            {
                return lhs.host < rhs.host;
            }
            return lhs.replayRestoreFingerprint < rhs.replayRestoreFingerprint;
        });

    for (auto watermarkIt = candidateWatermarks.rbegin(); watermarkIt != candidateWatermarks.rend(); ++watermarkIt)
    {
        const auto checkpointWatermarkMs = *watermarkIt;
        std::vector<ReplayCheckpointReference> selectedCheckpoints;
        std::unordered_map<Host, std::unordered_set<std::string>> usedBundlesByHost;
        bool completeCheckpointSet = true;

        for (const auto& requirement : requirements)
        {
            const auto host = Host(requirement.host);
            const auto hostCheckpointsIt = replayCheckpointsByHost.find(host);
            if (hostCheckpointsIt == replayCheckpointsByHost.end())
            {
                completeCheckpointSet = false;
                break;
            }

            const auto checkpointIt = std::ranges::find_if(
                hostCheckpointsIt->second,
                [&](const auto& replayCheckpoint)
                {
                    return replayCheckpoint.checkpointWatermarkMs == checkpointWatermarkMs
                        && replayCheckpoint.replayRestoreFingerprint == requirement.replayRestoreFingerprint
                        && !usedBundlesByHost[host].contains(replayCheckpoint.bundleName);
                });
            if (checkpointIt == hostCheckpointsIt->second.end())
            {
                completeCheckpointSet = false;
                break;
            }

            usedBundlesByHost[host].insert(checkpointIt->bundleName);
            selectedCheckpoints.push_back(ReplayCheckpointReference{
                .host = host.getRawValue(),
                .bundleName = checkpointIt->bundleName,
                .planFingerprint = checkpointIt->planFingerprint,
                .replayRestoreFingerprint = checkpointIt->replayRestoreFingerprint,
                .checkpointWatermarkMs = checkpointIt->checkpointWatermarkMs});
        }

        if (completeCheckpointSet)
        {
            return selectedCheckpoints;
        }
    }

    return {};
}
}

std::expected<ReplayPlan, Exception> ReplayPlanner::plan(
    const ReplayableQueryMetadata& queryMetadata,
    const RecordingCatalog& recordingCatalog,
    const uint64_t intervalStartMs,
    const uint64_t intervalEndMs) const
{
    if (workerCatalog == nullptr)
    {
        return std::unexpected(UnsupportedQuery("Replay planning requires a valid worker catalog"));
    }
    if (intervalEndMs <= intervalStartMs)
    {
        return std::unexpected(InvalidQuerySyntax(
            "Replay interval end {} must be greater than start {}",
            intervalEndMs,
            intervalStartMs));
    }
    if (!queryMetadata.queryPlanRewrite.has_value())
    {
        return std::unexpected(UnsupportedQuery("Replay planning requires persisted query rewrite metadata"));
    }
    if (queryMetadata.replayBoundaries.empty())
    {
        return std::unexpected(UnsupportedQuery("Replay planning requires at least one persisted replay boundary"));
    }

    const auto planGraph = buildPlanGraph(*queryMetadata.queryPlanRewrite);
    if (!planGraph.has_value())
    {
        return std::unexpected(planGraph.error());
    }

    RecordingCandidateSet candidateSet;
    candidateSet.rootOperatorId = planGraph->rootOperatorId;
    candidateSet.replayLatencyLimitMs = queryMetadata.replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; });
    candidateSet.leafOperatorIds = planGraph->leafOperatorIds;
    candidateSet.planEdges = planGraph->planEdges;
    candidateSet.operatorReplayTimes = operatorReplayTimesForPlan(*planGraph, *workerCatalog);
    candidateSet.candidates.reserve(queryMetadata.replayBoundaries.size());

    for (const auto& replayBoundary : queryMetadata.replayBoundaries)
    {
        if (replayBoundary.materializationEdges.size() != 1U)
        {
            return std::unexpected(UnsupportedQuery(
                "Replay planning currently supports replay boundaries with exactly one materialization edge, but found {}",
                replayBoundary.materializationEdges.size()));
        }

        const auto edge = replayBoundary.materializationEdges.front();
        if (!planGraph->operatorsById.contains(edge.parentId) || !planGraph->operatorsById.contains(edge.childId))
        {
            return std::unexpected(UnsupportedQuery(
                "Replay planning could not resolve replay boundary {} -> {} in the persisted replay base plan",
                edge.parentId,
                edge.childId));
        }

        const auto& parentOperator = planGraph->operatorsById.at(edge.parentId);
        const auto& childOperator = planGraph->operatorsById.at(edge.childId);
        const auto sourcePlacement = getPlacementFor(childOperator);
        const auto sinkPlacement = getPlacementFor(parentOperator);
        if (sourcePlacement == Host(Host::INVALID) || sinkPlacement == Host(Host::INVALID))
        {
            return std::unexpected(UnsupportedQuery(
                "Replay planning requires placement traits on replay boundary {} -> {}",
                edge.parentId,
                edge.childId));
        }

        const auto options = replayOptionsForBoundary(
            queryMetadata,
            replayBoundary,
            childOperator,
            sourcePlacement,
            sinkPlacement,
            recordingCatalog,
            *workerCatalog,
            intervalStartMs,
            intervalEndMs);
        candidateSet.candidates.push_back(RecordingBoundaryCandidate{
            .edge = {.parentId = edge.parentId, .childId = edge.childId},
            .upstreamNode = sourcePlacement,
            .downstreamNode = sinkPlacement,
            .routeNodes =
                enumerateRoutePlacements(sourcePlacement, sinkPlacement, workerCatalog->getTopology())
                | std::views::transform([](const auto& placement) { return placement.node; })
                | std::ranges::to<std::vector>(),
            .materializationEdges = {RecordingPlanEdge{.parentId = edge.parentId, .childId = edge.childId}},
            .activeQueryMaterializationTargets = {},
            .beneficiaryQueries = {},
            .coversIncomingQuery = true,
            .options = options});
    }

    try
    {
        const auto selection = RecordingBoundarySolver(copyPtr(workerCatalog)).solve(candidateSet);
        const auto selectedCheckpoints = selectReplayCheckpoints(queryMetadata, *workerCatalog, intervalStartMs);

        ReplayPlan replayPlan{
            .queryPlanRewrite =
                QueryRecordingPlanRewrite{
                    .queryId = queryMetadata.queryPlanRewrite->queryId,
                    .basePlan = queryMetadata.queryPlanRewrite->basePlan,
                    .insertions = {}},
            .replayCheckpointBoundaries = {},
            .replayCheckpointRequirements = queryMetadata.replayCheckpointRequirements,
            .selectedReplayBoundaries = {},
            .explanations = {},
            .selectedCheckpoints = selectedCheckpoints,
            .warmupStartMs = selectedCheckpoints.empty() ? 0 : selectedCheckpoints.front().checkpointWatermarkMs,
            .selectedRecordingIds = {},
            .objectiveCost = selection.objectiveCost};
        replayPlan.selectedReplayBoundaries.reserve(selection.selectedBoundary.size());
        replayPlan.explanations.reserve(selection.selectedBoundary.size());
        replayPlan.selectedRecordingIds.reserve(selection.selectedBoundary.size());

        for (const auto& selectedBoundary : selection.selectedBoundary)
        {
            const auto selectedReplayBoundary = QueryRecordingPlanInsertion{
                .selection = selectedBoundary.chosenOption.selection,
                .materializationEdges =
                    selectedBoundary.candidate.materializationEdges
                    | std::views::transform(
                          [](const auto& materializationEdge)
                          {
                              return RecordingRewriteEdge{
                                  .parentId = materializationEdge.parentId,
                                  .childId = materializationEdge.childId};
                          })
                    | std::ranges::to<std::vector>()};
            replayPlan.selectedReplayBoundaries.push_back(selectedReplayBoundary);
            replayPlan.queryPlanRewrite.insertions.push_back(selectedReplayBoundary);
            replayPlan.explanations.push_back(buildReplayPlanningExplanation(
                selectedBoundary,
                candidateSet.candidates.size(),
                selection.objectiveCost));
            replayPlan.selectedRecordingIds.push_back(selectedBoundary.chosenOption.selection.recordingId);
        }
        const auto replayCheckpointBoundaries = buildReplayCheckpointBoundaries(replayPlan.queryPlanRewrite);
        if (!replayCheckpointBoundaries)
        {
            return std::unexpected(replayCheckpointBoundaries.error());
        }
        replayPlan.replayCheckpointBoundaries = *replayCheckpointBoundaries;
        return replayPlan;
    }
    catch (const Exception& exception)
    {
        return std::unexpected(exception);
    }
}
}
