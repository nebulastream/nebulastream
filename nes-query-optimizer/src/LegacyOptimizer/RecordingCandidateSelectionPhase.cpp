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
#include <cmath>
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
#include <LegacyOptimizer/RecordingPlanRewriter.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Replay/ReplayNodeFingerprint.hpp>
#include <Replay/ReplayStorage.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>

namespace NES
{
namespace
{
constexpr size_t DEFAULT_RECORDING_BOTTOM_UP_PLACEMENT_LIMIT = 4;

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

struct QueryReplayPlan
{
    std::optional<std::string> activeQueryId;
    bool incoming = false;
    std::optional<ReplaySpecification> replaySpecification;
    LogicalPlan plan;
};

struct MergedBoundaryCandidateAccumulator
{
    RecordingBoundaryCandidate candidate;
    LogicalOperator recordedSubplanRoot;
    std::optional<uint64_t> requiredRetentionWindowMs;
    std::optional<uint64_t> replayLatencyLimitMs;
};

struct MergedReplayGraphBuilder
{
    std::unordered_map<std::string, OperatorId> nodeIdsByFingerprint;
    std::unordered_map<OperatorId, LogicalOperator> operatorsBySyntheticId;
    std::unordered_map<RecordingPlanEdge, MergedBoundaryCandidateAccumulator, RecordingPlanEdgeHash> candidatesByEdge;
    std::unordered_set<OperatorId> rootOperatorIds;
    std::unordered_set<OperatorId> leafOperatorIds;
    MergedReplayContext replayContext;
    uint64_t nextSyntheticOperatorId = 1;
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
        case RecordingRepresentation::BinaryStoreZstdFast1:
            return "binary_store_zstd_fast1";
        case RecordingRepresentation::BinaryStoreZstd:
            return "binary_store_zstd";
        case RecordingRepresentation::BinaryStoreZstdFast6:
            return "binary_store_zstd_fast6";
    }
    std::unreachable();
}

std::vector<RecordingRepresentation> supportedRecordingRepresentations()
{
    return {
        RecordingRepresentation::BinaryStore,
        RecordingRepresentation::BinaryStoreZstdFast1,
        RecordingRepresentation::BinaryStoreZstd,
        RecordingRepresentation::BinaryStoreZstdFast6};
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
        .incrementalStorageBytes = estimate.incrementalStorageBytes,
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
            "upgrade_existing_recording on {} with representation {} from retention {} ms to {} ms requires an estimated {} additional bytes"
            " ({} bytes retained total), but only {} bytes remain in the recording budget of {} bytes",
            placement,
            recordingRepresentationDescription(representation),
            existingRetentionWindowMs.value_or(0),
            requestedRetentionWindowMs(replaySpecification).value_or(0),
            upgradeCost.incrementalStorageBytes,
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

std::vector<RoutePlacement> selectBottomUpRoutePlacements(
    const std::vector<RoutePlacement>& routePlacements, const size_t placementLimit = DEFAULT_RECORDING_BOTTOM_UP_PLACEMENT_LIMIT)
{
    const auto boundedPlacementCount = std::min(routePlacements.size(), placementLimit);
    std::vector<RoutePlacement> placements;
    placements.reserve(boundedPlacementCount);
    for (size_t index = 0; index < boundedPlacementCount; ++index)
    {
        placements.push_back(routePlacements.at(index));
    }
    return placements;
}

LogicalOperator normalizeReplayFingerprintOperator(const LogicalOperator& current)
{
    if (current.tryGetAs<StoreLogicalOperator>().has_value())
    {
        const auto children = current.getChildren();
        PRECONDITION(children.size() == 1, "Replay normalization expected store operators to have a single child");
        return normalizeReplayFingerprintOperator(children.front());
    }

    auto normalizedChildren
        = current.getChildren() | std::views::transform(normalizeReplayFingerprintOperator) | std::ranges::to<std::vector>();
    return current.withChildren(std::move(normalizedChildren));
}

LogicalOperator unwrapReplayOperator(const LogicalOperator& current)
{
    if (current.tryGetAs<StoreLogicalOperator>().has_value())
    {
        const auto children = current.getChildren();
        PRECONDITION(children.size() == 1, "Replay normalization expected store operators to have a single child");
        return unwrapReplayOperator(children.front());
    }
    return current;
}

std::vector<LogicalOperator> getReplayChildren(const LogicalOperator& current)
{
    std::vector<LogicalOperator> children;
    children.reserve(current.getChildren().size());
    for (const auto& child : current.getChildren())
    {
        children.push_back(unwrapReplayOperator(child));
    }
    return children;
}

std::string structuralNodeFingerprint(const LogicalOperator& logicalOperator) { return Replay::createStructuralReplayNodeFingerprint(logicalOperator); }

OperatorId getOrCreateSyntheticOperatorId(MergedReplayGraphBuilder& builder, const std::string& fingerprint)
{
    if (const auto it = builder.nodeIdsByFingerprint.find(fingerprint); it != builder.nodeIdsByFingerprint.end())
    {
        return it->second;
    }

    const auto syntheticId = OperatorId(builder.nextSyntheticOperatorId++);
    builder.nodeIdsByFingerprint.emplace(fingerprint, syntheticId);
    return syntheticId;
}

void rememberSyntheticOperator(MergedReplayGraphBuilder& builder, const OperatorId operatorId, const LogicalOperator& logicalOperator)
{
    builder.operatorsBySyntheticId.insert_or_assign(operatorId, normalizeReplayFingerprintOperator(logicalOperator));
}

double estimateHeuristicOperatorReplayTimeMs(const LogicalOperator& logicalOperator)
{
    size_t schemaBytes = 1;
    if (logicalOperator.tryGetAs<SinkLogicalOperator>().has_value())
    {
        const auto children = logicalOperator.getChildren();
        if (!children.empty())
        {
            schemaBytes = std::max(children.front().getOutputSchema().getSizeOfSchemaInBytes(), size_t{1});
        }
    }
    else
    {
        schemaBytes = std::max(logicalOperator.getOutputSchema().getSizeOfSchemaInBytes(), size_t{1});
    }
    const auto arityFactor = 1.0 + (logicalOperator.getChildren().size() * 0.5);
    return std::max(1.0, std::ceil((static_cast<double>(schemaBytes) / 256.0) * arityFactor));
}

double estimateOperatorReplayTimeMs(const LogicalOperator& logicalOperator, const WorkerCatalog& workerCatalog)
{
    const auto replayStatistics = workerCatalog.getReplayOperatorStatistics(getPlacementFor(logicalOperator), structuralNodeFingerprint(logicalOperator));
    if (replayStatistics.has_value() && replayStatistics->taskCount > 0)
    {
        return std::max(1.0, replayStatistics->averageExecutionTimeMs());
    }
    return estimateHeuristicOperatorReplayTimeMs(logicalOperator);
}

size_t estimateRecordingIngressWriteBytesPerSecond(const LogicalOperator& logicalOperator, const WorkerCatalog& workerCatalog)
{
    const auto schemaBytes = std::max(logicalOperator.getOutputSchema().getSizeOfSchemaInBytes(), size_t{1});
    const auto replayStatistics = workerCatalog.getReplayOperatorStatistics(getPlacementFor(logicalOperator), structuralNodeFingerprint(logicalOperator));
    if (replayStatistics.has_value() && replayStatistics->executionTimeNanos > 0 && replayStatistics->outputTuples > 0)
    {
        const auto outputBytes = static_cast<double>(replayStatistics->outputTuples) * static_cast<double>(schemaBytes);
        const auto executionSeconds = static_cast<double>(replayStatistics->executionTimeNanos) / 1'000'000'000.0;
        return std::max<size_t>(1, static_cast<size_t>(std::ceil(outputBytes / executionSeconds)));
    }
    return 0;
}

QueryReplayPlan makeIncomingReplayPlan(const LogicalPlan& placedPlan, const std::optional<ReplaySpecification>& replaySpecification)
{
    return QueryReplayPlan{
        .activeQueryId = std::nullopt,
        .incoming = true,
        .replaySpecification = replaySpecification,
        .plan = placedPlan};
}

std::vector<QueryReplayPlan> buildReplayPlans(
    const LogicalPlan& incomingPlacedPlan,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingCatalog& recordingCatalog)
{
    std::vector<QueryReplayPlan> replayPlans;
    replayPlans.push_back(makeIncomingReplayPlan(incomingPlacedPlan, replaySpecification));
    for (const auto& [queryId, metadata] : recordingCatalog.getQueryMetadata())
    {
        if (!metadata.globalPlan.has_value() || !metadata.replaySpecification.has_value())
        {
            continue;
        }
        replayPlans.push_back(QueryReplayPlan{
            .activeQueryId = queryId.getRawValue(),
            .incoming = false,
            .replaySpecification = metadata.replaySpecification,
            .plan = stripReplayStores(*metadata.globalPlan)});
    }
    return replayPlans;
}

void mergeReplayRequirements(MergedBoundaryCandidateAccumulator& accumulator, const std::optional<ReplaySpecification>& replaySpecification)
{
    if (const auto retentionWindowMs = replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; }); retentionWindowMs.has_value())
    {
        accumulator.requiredRetentionWindowMs
            = std::max(accumulator.requiredRetentionWindowMs.value_or(0), *retentionWindowMs);
    }
    if (const auto replayLatencyLimitMs = replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; });
        replayLatencyLimitMs.has_value())
    {
        accumulator.replayLatencyLimitMs = accumulator.replayLatencyLimitMs.has_value()
            ? std::min(accumulator.replayLatencyLimitMs.value(), *replayLatencyLimitMs)
            : std::optional(*replayLatencyLimitMs);
    }
}

std::optional<ReplaySpecification> effectiveReplaySpecification(
    const MergedBoundaryCandidateAccumulator& accumulator, const std::optional<ReplaySpecification>& defaultReplaySpecification)
{
    const auto incomingReplaySpecification = accumulator.candidate.coversIncomingQuery ? defaultReplaySpecification : std::nullopt;
    auto specification = incomingReplaySpecification.value_or(ReplaySpecification{});
    if (const auto defaultRetentionWindowMs = incomingReplaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; });
        defaultRetentionWindowMs.has_value())
    {
        specification.retentionWindowMs = std::max(accumulator.requiredRetentionWindowMs.value_or(0), *defaultRetentionWindowMs);
    }
    else
    {
        specification.retentionWindowMs = accumulator.requiredRetentionWindowMs;
    }
    if (const auto defaultReplayLatencyLimitMs = incomingReplaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; });
        defaultReplayLatencyLimitMs.has_value())
    {
        specification.replayLatencyLimitMs = accumulator.replayLatencyLimitMs.has_value()
            ? std::min(accumulator.replayLatencyLimitMs.value(), *defaultReplayLatencyLimitMs)
            : std::optional(*defaultReplayLatencyLimitMs);
    }
    else
    {
        specification.replayLatencyLimitMs = accumulator.replayLatencyLimitMs;
    }

    if (!specification.retentionWindowMs.has_value() && !specification.replayLatencyLimitMs.has_value())
    {
        return std::nullopt;
    }
    return specification;
}

void collectMergedGraph(
    const LogicalOperator& current,
    const QueryReplayPlan& queryReplayPlan,
    const WorkerCatalog& workerCatalog,
    MergedReplayGraphBuilder& builder,
    std::unordered_set<RecordingPlanEdge, RecordingPlanEdgeHash>& visitedEdges,
    std::unordered_set<OperatorId>& visitedLeaves)
{
    const auto currentFingerprint = structuralNodeFingerprint(current);
    const auto currentSyntheticId = getOrCreateSyntheticOperatorId(builder, currentFingerprint);
    rememberSyntheticOperator(builder, currentSyntheticId, current);
    const auto children = getReplayChildren(current);
    if (children.empty())
    {
        if (visitedLeaves.insert(current.getId()).second)
        {
            builder.leafOperatorIds.insert(currentSyntheticId);
        }
        return;
    }

    for (const auto& child : children)
    {
        const RecordingPlanEdge actualEdge{.parentId = current.getId(), .childId = child.getId()};
        if (!visitedEdges.insert(actualEdge).second)
        {
            continue;
        }

        const auto childFingerprint = structuralNodeFingerprint(child);
        const auto childSyntheticId = getOrCreateSyntheticOperatorId(builder, childFingerprint);
        rememberSyntheticOperator(builder, childSyntheticId, child);
        const auto routePlacements = enumerateRoutePlacements(getPlacementFor(child), getPlacementFor(current), workerCatalog.getTopology());
        for (const auto& placement : selectBottomUpRoutePlacements(routePlacements))
        {
            ++builder.replayContext.structuralOccurrences[createStructuralRecordingFingerprint(
                normalizeReplayFingerprintOperator(child), placement.node)];
        }

        const RecordingPlanEdge mergedEdge{.parentId = currentSyntheticId, .childId = childSyntheticId};
        auto [candidateIt, inserted] = builder.candidatesByEdge.try_emplace(
            mergedEdge,
            MergedBoundaryCandidateAccumulator{
                .candidate =
                    RecordingBoundaryCandidate{
                        .edge = mergedEdge,
                        .upstreamNode = getPlacementFor(child),
                        .downstreamNode = getPlacementFor(current),
                        .routeNodes = routePlacements | std::views::transform([](const auto& placement) { return placement.node; }) | std::ranges::to<std::vector>(),
                        .materializationEdges = {},
                        .activeQueryMaterializationTargets = {},
                        .beneficiaryQueries = {},
                        .coversIncomingQuery = false,
                        .options = {}},
                .recordedSubplanRoot = child,
                .requiredRetentionWindowMs = std::nullopt,
                .replayLatencyLimitMs = std::nullopt});
        auto& candidate = candidateIt->second.candidate;
        if (queryReplayPlan.incoming)
        {
            candidate.coversIncomingQuery = true;
            if (!std::ranges::contains(candidate.materializationEdges, actualEdge))
            {
                candidate.materializationEdges.push_back(actualEdge);
            }
        }
        else if (queryReplayPlan.activeQueryId.has_value() && !std::ranges::contains(candidate.beneficiaryQueries, *queryReplayPlan.activeQueryId))
        {
            candidate.beneficiaryQueries.push_back(*queryReplayPlan.activeQueryId);
        }
        if (queryReplayPlan.activeQueryId.has_value())
        {
            auto targetIt = std::ranges::find(
                candidate.activeQueryMaterializationTargets,
                *queryReplayPlan.activeQueryId,
                &RecordingBoundaryCandidate::QueryMaterializationTarget::queryId);
            if (targetIt == candidate.activeQueryMaterializationTargets.end())
            {
                candidate.activeQueryMaterializationTargets.push_back(
                    RecordingBoundaryCandidate::QueryMaterializationTarget{.queryId = *queryReplayPlan.activeQueryId, .materializationEdges = {actualEdge}});
            }
            else if (!std::ranges::contains(targetIt->materializationEdges, actualEdge))
            {
                targetIt->materializationEdges.push_back(actualEdge);
            }
        }
        mergeReplayRequirements(candidateIt->second, queryReplayPlan.replaySpecification);

        collectMergedGraph(child, queryReplayPlan, workerCatalog, builder, visitedEdges, visitedLeaves);
    }
}

void collectMergedGraph(const QueryReplayPlan& queryReplayPlan, const WorkerCatalog& workerCatalog, MergedReplayGraphBuilder& builder)
{
    const auto roots = queryReplayPlan.plan.getRootOperators();
    PRECONDITION(roots.size() == 1, "Replay recording selection requires a single sink root per query plan, but got {} roots", roots.size());
    PRECONDITION(roots.front().tryGetAs<SinkLogicalOperator>().has_value(), "Replay recording selection requires sink roots");

    const auto rootFingerprint = structuralNodeFingerprint(roots.front());
    builder.rootOperatorIds.insert(getOrCreateSyntheticOperatorId(builder, rootFingerprint));

    std::unordered_set<RecordingPlanEdge, RecordingPlanEdgeHash> visitedEdges;
    std::unordered_set<OperatorId> visitedLeaves;
    collectMergedGraph(*roots.begin(), queryReplayPlan, workerCatalog, builder, visitedEdges, visitedLeaves);
}

void populateInstalledRecordings(MergedReplayContext& replayContext, const RecordingCatalog& recordingCatalog)
{
    for (const auto& recording : recordingCatalog.getRecordings() | std::views::values)
    {
        ++replayContext.installedRecordings[recording.structuralFingerprint];
    }
}

std::vector<RecordingBoundaryCandidate> buildCandidates(
    const std::unordered_map<RecordingPlanEdge, MergedBoundaryCandidateAccumulator, RecordingPlanEdgeHash>& candidatesByEdge,
    const std::optional<ReplaySpecification>& defaultReplaySpecification,
    const RecordingCatalog& recordingCatalog,
    const MergedReplayContext& mergedReplayContext,
    const WorkerCatalog& workerCatalog)
{
    std::vector<RecordingBoundaryCandidate> candidates;
    candidates.reserve(candidatesByEdge.size());
    const auto costModel = RecordingCostModel{};

    for (const auto& accumulator : candidatesByEdge | std::views::values)
    {
        auto candidate = accumulator.candidate;
        std::ranges::sort(candidate.beneficiaryQueries);
        for (auto& target : candidate.activeQueryMaterializationTargets)
        {
            std::ranges::sort(
                target.materializationEdges,
                {},
                [](const RecordingPlanEdge& edge) { return std::pair{edge.parentId.getRawValue(), edge.childId.getRawValue()}; });
        }
        std::ranges::sort(
            candidate.activeQueryMaterializationTargets,
            {},
            &RecordingBoundaryCandidate::QueryMaterializationTarget::queryId);
        const auto replaySpecification = effectiveReplaySpecification(accumulator, defaultReplaySpecification);
        const auto requestedRetention = requestedRetentionWindowMs(replaySpecification);
        const auto normalizedRecordedSubplanRoot = normalizeReplayFingerprintOperator(accumulator.recordedSubplanRoot);
        const auto estimatedIngressWriteBytesPerSecond
            = estimateRecordingIngressWriteBytesPerSecond(accumulator.recordedSubplanRoot, workerCatalog);

        const auto routePlacements = enumerateRoutePlacements(candidate.upstreamNode, candidate.downstreamNode, workerCatalog.getTopology());
        const auto feasibleRoutePlacements = selectBottomUpRoutePlacements(routePlacements);
        for (const auto& routePlacement : feasibleRoutePlacements)
        {
            const auto worker = workerCatalog.getWorker(routePlacement.node);
            PRECONDITION(worker.has_value(), "Replay recording selection could not find worker {}", routePlacement.node);
            const auto workerRuntimeMetrics = workerCatalog.getWorkerRuntimeMetrics(routePlacement.node);

            for (const auto representation : supportedRecordingRepresentations())
            {
                const auto structuralFingerprint = createStructuralRecordingFingerprint(normalizedRecordedSubplanRoot, routePlacement.node);
                const auto structurallyCompatibleRecordings = recordingCatalog.getRecordingsByStructuralFingerprint(
                                                                 structuralFingerprint)
                    | std::views::filter([&](const RecordingEntry& entry) { return entry.representation == representation; })
                    | std::ranges::to<std::vector>();
                const auto recordingFingerprint
                    = createRecordingFingerprint(normalizedRecordedSubplanRoot, routePlacement.node, replaySpecification, representation);
                const auto recordingId = recordingIdFromFingerprint(recordingFingerprint);
                const auto placementContext = RecordingPlacementContext{
                    .sourcePlacement = candidate.upstreamNode,
                    .sinkPlacement = candidate.downstreamNode,
                    .recordingPlacement = routePlacement.node,
                    .estimatedIngressWriteBytesPerSecond = estimatedIngressWriteBytesPerSecond,
                    .upstreamHopCount = routePlacement.upstreamHopCount,
                    .downstreamHopCount = routePlacement.downstreamHopCount,
                    .totalRouteHopCount = routePlacements.size() - 1,
                    .mergedOccurrenceCount = mergedReplayContext.mergedOccurrenceCount(structuralFingerprint),
                    .activeReplayConsumerCount = mergedReplayContext.activeReplayConsumerCount(structuralFingerprint),
                    .installedRecordingCount = mergedReplayContext.installedRecordingCount(structuralFingerprint),
                    .representation = representation};
                const auto selection = RecordingSelection{
                    .recordingId = recordingId,
                    .node = routePlacement.node,
                    .filePath = Replay::getRecordingFilePath(recordingId.getRawValue()),
                    .structuralFingerprint = structuralFingerprint,
                    .representation = representation,
                    .retentionWindowMs = replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; }),
                    .beneficiaryQueries = candidate.beneficiaryQueries,
                    .coversIncomingQuery = candidate.coversIncomingQuery};

                const auto newRecordingCost = costModel.estimateNewRecording(
                    accumulator.recordedSubplanRoot, placementContext, *worker, workerRuntimeMetrics, replaySpecification);
                candidate.options.push_back(RecordingCandidateOption{
                    .decision = RecordingSelectionDecision::CreateNewRecording,
                    .selection = selection,
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
                        accumulator.recordedSubplanRoot,
                        placementContext,
                        *worker,
                        workerRuntimeMetrics,
                        replaySpecification,
                        bestReuseRecording->retentionWindowMs);
                    candidate.options.push_back(RecordingCandidateOption{
                        .decision = RecordingSelectionDecision::ReuseExistingRecording,
                        .selection =
                            RecordingSelection{
                                .recordingId = bestReuseRecording->id,
                                .node = bestReuseRecording->node,
                                .filePath = bestReuseRecording->filePath,
                                .structuralFingerprint = bestReuseRecording->structuralFingerprint,
                                .representation = bestReuseRecording->representation,
                                .retentionWindowMs = bestReuseRecording->retentionWindowMs,
                                .beneficiaryQueries = candidate.beneficiaryQueries,
                                .coversIncomingQuery = candidate.coversIncomingQuery},
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
                        accumulator.recordedSubplanRoot,
                        placementContext,
                        *worker,
                        workerRuntimeMetrics,
                        replaySpecification,
                        bestUpgradeRecording->retentionWindowMs);
                    candidate.options.push_back(RecordingCandidateOption{
                        .decision = RecordingSelectionDecision::UpgradeExistingRecording,
                        .selection = selection,
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

        candidates.push_back(std::move(candidate));
    }

    return candidates;
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

    auto replayPlans = buildReplayPlans(placedPlan, replaySpecification, recordingCatalog);
    PRECONDITION(!replayPlans.empty(), "Replay recording selection requires at least one replayable query plan");

    MergedReplayGraphBuilder builder;
    for (const auto& replayPlan : replayPlans)
    {
        collectMergedGraph(replayPlan, *workerCatalog, builder);
    }
    populateInstalledRecordings(builder.replayContext, recordingCatalog);

    RecordingCandidateSet candidateSet{};
    candidateSet.replayLatencyLimitMs = replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; });
    candidateSet.activeQueryPlans = replayPlans
        | std::views::filter([](const QueryReplayPlan& replayPlan) { return replayPlan.activeQueryId.has_value(); })
        | std::views::transform(
              [](const QueryReplayPlan& replayPlan)
              {
                  return RecordingCandidateSet::ActiveQueryPlan{
                      .queryId = *replayPlan.activeQueryId,
                      .plan = replayPlan.plan};
              })
        | std::ranges::to<std::vector>();
    candidateSet.candidates = buildCandidates(builder.candidatesByEdge, replaySpecification, recordingCatalog, builder.replayContext, *workerCatalog);
    candidateSet.planEdges = builder.candidatesByEdge | std::views::keys | std::ranges::to<std::vector>();
    candidateSet.leafOperatorIds = builder.leafOperatorIds | std::ranges::to<std::vector>();
    candidateSet.operatorReplayTimes.reserve(builder.operatorsBySyntheticId.size());
    for (const auto& [operatorId, logicalOperator] : builder.operatorsBySyntheticId)
    {
        if (builder.leafOperatorIds.contains(operatorId))
        {
            continue;
        }
        candidateSet.operatorReplayTimes.push_back(
            RecordingCandidateSet::OperatorReplayTime{
                .operatorId = operatorId,
                .replayTimeMs = estimateOperatorReplayTimeMs(logicalOperator, *workerCatalog)});
    }

    if (builder.rootOperatorIds.size() == 1)
    {
        candidateSet.rootOperatorId = *builder.rootOperatorIds.begin();
    }
    else
    {
        candidateSet.rootOperatorId = OperatorId(builder.nextSyntheticOperatorId++);
        for (const auto rootOperatorId : builder.rootOperatorIds)
        {
            candidateSet.planEdges.push_back(RecordingPlanEdge{.parentId = candidateSet.rootOperatorId, .childId = rootOperatorId});
        }
    }

    std::ranges::sort(candidateSet.planEdges, {}, [](const RecordingPlanEdge& edge) { return std::pair{edge.parentId.getRawValue(), edge.childId.getRawValue()}; });
    std::ranges::sort(candidateSet.leafOperatorIds, {}, &OperatorId::getRawValue);
    std::ranges::sort(candidateSet.operatorReplayTimes, {}, [](const auto& entry) { return entry.operatorId.getRawValue(); });
    std::ranges::sort(candidateSet.activeQueryPlans, {}, &RecordingCandidateSet::ActiveQueryPlan::queryId);
    std::ranges::sort(candidateSet.candidates, {}, [](const RecordingBoundaryCandidate& candidate) { return std::pair{candidate.edge.parentId.getRawValue(), candidate.edge.childId.getRawValue()}; });
    return candidateSet;
}
}
