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

#include <LegacyOptimizer/RecordingBoundarySolver.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <lemon/list_graph.h>
#include <lemon/preflow.h>

#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{
namespace
{
constexpr double STORAGE_PENALTY_NORMALIZATION_BYTES = 4096.0;
constexpr size_t MAX_SHADOW_PRICE_ITERATIONS = 16;
constexpr double EPSILON = 1e-9;
constexpr double CAPACITY_SCALE = 1000.0;
constexpr int64_t MAX_FLOW_CAPACITY = std::numeric_limits<int64_t>::max() / 4;

using LemonGraph = lemon::ListDigraph;
using LemonCapacity = int64_t;

struct RecordingPlanEdgeHash
{
    [[nodiscard]] size_t operator()(const RecordingPlanEdge& edge) const
    {
        return std::hash<uint64_t>{}(edge.parentId.getRawValue()) ^ (std::hash<uint64_t>{}(edge.childId.getRawValue()) << 1U);
    }
};

struct SelectedBoundarySignatureHash
{
    [[nodiscard]] size_t operator()(const std::string& value) const { return std::hash<std::string>{}(value); }
};

struct WeightedCandidateChoice
{
    RecordingCandidateOption option;
    double adjustedDataflowEdgeCost = 0.0;
};

int decisionPreference(const RecordingSelectionDecision decision)
{
    switch (decision)
    {
        case RecordingSelectionDecision::ReuseExistingRecording:
            return 0;
        case RecordingSelectionDecision::UpgradeExistingRecording:
            return 1;
        case RecordingSelectionDecision::CreateNewRecording:
            return 2;
    }
    std::unreachable();
}

[[nodiscard]] double weightedReplayScanTimeMs(const RecordingCandidateOption& option)
{
    return static_cast<double>(option.cost.estimatedReplayLatencyMs) * option.cost.replayTimeMultiplier;
}

[[nodiscard]] double warmStartLatencyPrice(const RecordingCandidateSet& candidateSet)
{
    return candidateSet.replayLatencyLimitMs.has_value() ? 1.0 / std::max<double>(*candidateSet.replayLatencyLimitMs, 1.0) : 0.0;
}

[[nodiscard]] double latencyPriceStep(const RecordingCandidateSet& candidateSet)
{
    return warmStartLatencyPrice(candidateSet);
}

[[nodiscard]] LemonCapacity checkedAddCapacity(const LemonCapacity total, const LemonCapacity increment)
{
    if (increment > MAX_FLOW_CAPACITY - total)
    {
        throw PlacementFailure("Replay selection is infeasible: replay min-cut capacity sentinel overflowed");
    }
    return total + increment;
}

[[nodiscard]] LemonCapacity scaledCapacityFromCost(const double cost)
{
    PRECONDITION(std::isfinite(cost), "Replay selection requires finite min-cut capacities");
    PRECONDITION(cost >= -EPSILON, "Replay selection requires non-negative min-cut capacities");

    if (cost <= EPSILON)
    {
        return 0;
    }

    const auto scaled = std::ceil(cost * CAPACITY_SCALE);
    if (scaled >= static_cast<double>(MAX_FLOW_CAPACITY))
    {
        throw PlacementFailure("Replay selection is infeasible: replay min-cut capacity sentinel overflowed");
    }
    return static_cast<LemonCapacity>(scaled);
}

std::optional<WeightedCandidateChoice> chooseBestOption(
    const RecordingBoundaryCandidate& candidate,
    const std::unordered_map<Host, double>& storageShadowPrices,
    const double replayTimePrice)
{
    std::optional<WeightedCandidateChoice> bestChoice;
    for (const auto& option : candidate.options)
    {
        if (!option.feasible)
        {
            continue;
        }

        double adjustedDataflowEdgeCost = option.cost.maintenanceCost + (replayTimePrice * weightedReplayScanTimeMs(option));
        if (option.decision != RecordingSelectionDecision::ReuseExistingRecording)
        {
            adjustedDataflowEdgeCost += storageShadowPrices.contains(option.selection.node)
                ? storageShadowPrices.at(option.selection.node)
                    * (static_cast<double>(option.cost.incrementalStorageBytes) / STORAGE_PENALTY_NORMALIZATION_BYTES)
                : 0.0;
        }

        if (!bestChoice.has_value() || adjustedDataflowEdgeCost < bestChoice->adjustedDataflowEdgeCost - EPSILON
            || (std::abs(adjustedDataflowEdgeCost - bestChoice->adjustedDataflowEdgeCost) <= EPSILON
                && decisionPreference(option.decision) < decisionPreference(bestChoice->option.decision)))
        {
            bestChoice = WeightedCandidateChoice{.option = option, .adjustedDataflowEdgeCost = adjustedDataflowEdgeCost};
        }
    }
    return bestChoice;
}

std::string describeCandidate(const RecordingBoundaryCandidate& candidate)
{
    return fmt::format(
        "edge {} -> {} from {} to {}",
        candidate.edge.parentId.getRawValue(),
        candidate.edge.childId.getRawValue(),
        candidate.upstreamNode.getRawValue(),
        candidate.downstreamNode.getRawValue());
}

std::string describeInfeasibleBoundary(const RecordingCandidateSet& candidateSet)
{
    std::vector<std::string> reasons;
    for (const auto& candidate : candidateSet.candidates)
    {
        if (candidate.hasFeasibleOptions())
        {
            continue;
        }
        for (const auto& option : candidate.options)
        {
            if (!option.infeasibilityReason.empty())
            {
                reasons.push_back(fmt::format("{}: {}", describeCandidate(candidate), option.infeasibilityReason));
            }
        }
    }

    if (reasons.empty())
    {
        return "Replay selection is infeasible: the replay min-cut did not find a finite recording boundary";
    }

    return fmt::format("Replay selection is infeasible: {}", fmt::join(reasons, "; "));
}

std::string describeBudgetViolation(
    const std::unordered_map<Host, size_t>& selectedStorageBytesByHost,
    const std::unordered_map<Host, size_t>& availableStorageBytesByHost)
{
    std::vector<std::string> violations;
    for (const auto& [host, selectedBytes] : selectedStorageBytesByHost)
    {
        const auto availableBytes = availableStorageBytesByHost.contains(host) ? availableStorageBytesByHost.at(host) : 0;
        if (selectedBytes > availableBytes)
        {
            violations.push_back(fmt::format("{} selected {} bytes but only {} bytes are available", host, selectedBytes, availableBytes));
        }
    }
    return fmt::format("Replay selection is infeasible: selected recording boundary exceeds worker budgets ({})", fmt::join(violations, ", "));
}

std::string describeReplayLatencyViolation(const double selectedReplayTimeMs, const double replayLatencyLimitMs)
{
    return fmt::format(
        "Replay selection is infeasible: selected replay time {:.2f} ms exceeds the replay latency limit of {:.2f} ms",
        selectedReplayTimeMs,
        replayLatencyLimitMs);
}

std::string makeSelectionSignature(const std::vector<SelectedRecordingBoundary>& selectedBoundary)
{
    std::vector<std::string> parts;
    parts.reserve(selectedBoundary.size());
    for (const auto& selected : selectedBoundary)
    {
        parts.push_back(fmt::format(
            "{}:{}:{}",
            selected.candidate.edge.parentId.getRawValue(),
            selected.candidate.edge.childId.getRawValue(),
            fmt::format(
                "{}:{}:{}",
                static_cast<int>(selected.chosenOption.decision),
                selected.chosenOption.selection.node.getRawValue(),
                static_cast<int>(selected.chosenOption.selection.representation))));
    }
    std::ranges::sort(parts);
    return fmt::format("{}", fmt::join(parts, "|"));
}
}

RecordingBoundarySelection RecordingBoundarySolver::solve(const RecordingCandidateSet& candidateSet) const
{
    PRECONDITION(workerCatalog != nullptr, "Recording boundary solving requires a valid worker catalog");
    PRECONDITION(candidateSet.rootOperatorId != INVALID_OPERATOR_ID, "Recording boundary solving requires a valid root operator");
    PRECONDITION(!candidateSet.leafOperatorIds.empty(), "Recording boundary solving requires at least one leaf operator");

    std::unordered_map<Host, size_t> availableStorageBytesByHost;
    for (const auto& candidate : candidateSet.candidates)
    {
        for (const auto& option : candidate.options)
        {
            if (availableStorageBytesByHost.contains(option.selection.node))
            {
                continue;
            }
            const auto worker = workerCatalog->getWorker(option.selection.node);
            PRECONDITION(worker.has_value(), "Recording boundary solving could not find worker {}", option.selection.node);
            const auto runtimeMetrics = workerCatalog->getWorkerRuntimeMetrics(option.selection.node);
            const auto usedBytes = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0);
            availableStorageBytesByHost[option.selection.node] = usedBytes >= worker->recordingStorageBudget ? 0 : worker->recordingStorageBudget - usedBytes;
        }
    }

    std::unordered_map<Host, double> storageShadowPrices;
    std::unordered_set<std::string, SelectedBoundarySignatureHash> seenSelections;
    auto replayTimePrice = warmStartLatencyPrice(candidateSet);

    for (size_t iteration = 0; iteration < MAX_SHADOW_PRICE_ITERATIONS; ++iteration)
    {
        std::unordered_map<RecordingPlanEdge, WeightedCandidateChoice, RecordingPlanEdgeHash> bestChoiceByEdge;
        std::unordered_map<OperatorId, double> replayEdgeCostByOperatorId;
        LemonCapacity totalFiniteCapacity = 0;
        for (const auto& candidate : candidateSet.candidates)
        {
            if (const auto bestChoice = chooseBestOption(candidate, storageShadowPrices, replayTimePrice); bestChoice.has_value())
            {
                bestChoiceByEdge.emplace(candidate.edge, *bestChoice);
                totalFiniteCapacity = checkedAddCapacity(totalFiniteCapacity, scaledCapacityFromCost(bestChoice->adjustedDataflowEdgeCost));
            }
        }
        for (const auto& operatorReplayTime : candidateSet.operatorReplayTimes)
        {
            const auto replayEdgeCost = replayTimePrice * operatorReplayTime.replayTimeMs;
            replayEdgeCostByOperatorId.emplace(operatorReplayTime.operatorId, replayEdgeCost);
            totalFiniteCapacity = checkedAddCapacity(totalFiniteCapacity, scaledCapacityFromCost(replayEdgeCost));
        }

        if (bestChoiceByEdge.empty())
        {
            throw PlacementFailure("{}", describeInfeasibleBoundary(candidateSet));
        }

        const auto infiniteCapacity = checkedAddCapacity(std::max<LemonCapacity>(1, totalFiniteCapacity), 1);

        LemonGraph graph;
        std::unordered_map<OperatorId, LemonGraph::Node> graphNodesByOperator;
        auto getOrCreateNode = [&](const OperatorId operatorId) -> LemonGraph::Node
        {
            if (const auto it = graphNodesByOperator.find(operatorId); it != graphNodesByOperator.end())
            {
                return it->second;
            }
            const auto node = graph.addNode();
            graphNodesByOperator.emplace(operatorId, node);
            return node;
        };

        getOrCreateNode(candidateSet.rootOperatorId);
        for (const auto& edge : candidateSet.planEdges)
        {
            getOrCreateNode(edge.parentId);
            getOrCreateNode(edge.childId);
        }
        for (const auto leafId : candidateSet.leafOperatorIds)
        {
            getOrCreateNode(leafId);
        }

        const auto superSource = graph.addNode();
        LemonGraph::ArcMap<LemonCapacity> capacities(graph);

        for (const auto leafId : candidateSet.leafOperatorIds)
        {
            const auto arc = graph.addArc(superSource, graphNodesByOperator.at(leafId));
            capacities[arc] = infiniteCapacity;
        }
        for (const auto& operatorReplayTime : candidateSet.operatorReplayTimes)
        {
            const auto arc = graph.addArc(superSource, graphNodesByOperator.at(operatorReplayTime.operatorId));
            capacities[arc] = scaledCapacityFromCost(replayEdgeCostByOperatorId.at(operatorReplayTime.operatorId));
        }
        for (const auto& edge : candidateSet.planEdges)
        {
            const auto arc = graph.addArc(graphNodesByOperator.at(edge.childId), graphNodesByOperator.at(edge.parentId));
            capacities[arc] = bestChoiceByEdge.contains(edge) ? scaledCapacityFromCost(bestChoiceByEdge.at(edge).adjustedDataflowEdgeCost) : infiniteCapacity;
        }

        lemon::Preflow<LemonGraph, LemonGraph::ArcMap<LemonCapacity>>
            preflow(graph, capacities, superSource, graphNodesByOperator.at(candidateSet.rootOperatorId));
        preflow.runMinCut();

        LemonGraph::NodeMap<bool> minCutPartition(graph);
        preflow.minCutMap(minCutPartition);

        bool cutUsesInfiniteEdge = false;
        for (const auto& edge : candidateSet.planEdges)
        {
            const auto childOnRecordedSide = minCutPartition[graphNodesByOperator.at(edge.childId)];
            const auto parentOnRecordedSide = minCutPartition[graphNodesByOperator.at(edge.parentId)];
            if (childOnRecordedSide && !parentOnRecordedSide && !bestChoiceByEdge.contains(edge))
            {
                cutUsesInfiniteEdge = true;
                break;
            }
        }
        for (const auto leafId : candidateSet.leafOperatorIds)
        {
            if (!minCutPartition[graphNodesByOperator.at(leafId)])
            {
                cutUsesInfiniteEdge = true;
                break;
            }
        }

        std::vector<SelectedRecordingBoundary> selectedBoundary;
        selectedBoundary.reserve(candidateSet.candidates.size());
        for (const auto& candidate : candidateSet.candidates)
        {
            const auto childOnRecordedSide = minCutPartition[graphNodesByOperator.at(candidate.edge.childId)];
            const auto parentOnRecordedSide = minCutPartition[graphNodesByOperator.at(candidate.edge.parentId)];
            if (!(childOnRecordedSide && !parentOnRecordedSide))
            {
                continue;
            }

            if (!bestChoiceByEdge.contains(candidate.edge))
            {
                cutUsesInfiniteEdge = true;
                continue;
            }

            const auto& chosenOption = bestChoiceByEdge.at(candidate.edge).option;
            auto alternatives = candidate.options
                | std::views::filter(
                      [&](const auto& option)
                      {
                          return option.feasible
                              && !(option.decision == chosenOption.decision && option.selection == chosenOption.selection && option.cost == chosenOption.cost);
                      })
                | std::ranges::to<std::vector>();
            selectedBoundary.push_back(SelectedRecordingBoundary{
                .candidate = candidate,
                .chosenOption = chosenOption,
                .alternatives = std::move(alternatives)});
        }

        if (cutUsesInfiniteEdge || selectedBoundary.empty())
        {
            throw PlacementFailure("{}", describeInfeasibleBoundary(candidateSet));
        }

        std::unordered_map<Host, size_t> selectedStorageBytesByHost;
        double selectedReplayTimeMs = 0.0;
        for (const auto& selected : selectedBoundary)
        {
            if (selected.chosenOption.decision != RecordingSelectionDecision::ReuseExistingRecording)
            {
                selectedStorageBytesByHost[selected.chosenOption.selection.node] += selected.chosenOption.cost.incrementalStorageBytes;
            }
            selectedReplayTimeMs += weightedReplayScanTimeMs(selected.chosenOption);
        }
        for (const auto& operatorReplayTime : candidateSet.operatorReplayTimes)
        {
            if (!minCutPartition[graphNodesByOperator.at(operatorReplayTime.operatorId)])
            {
                selectedReplayTimeMs += operatorReplayTime.replayTimeMs;
            }
        }

        bool violatesBudget = false;
        for (const auto& [host, selectedBytes] : selectedStorageBytesByHost)
        {
            if (selectedBytes > availableStorageBytesByHost[host])
            {
                violatesBudget = true;
                const auto overflowBytes = selectedBytes - availableStorageBytesByHost[host];
                const auto overflowFactor = static_cast<double>(overflowBytes) / std::max<double>(availableStorageBytesByHost[host], 1.0);
                storageShadowPrices[host] += 1.0 + overflowFactor;
            }
        }

        const auto violatesReplayLatency = candidateSet.replayLatencyLimitMs.has_value()
            && selectedReplayTimeMs > static_cast<double>(*candidateSet.replayLatencyLimitMs);
        if (violatesReplayLatency)
        {
            const auto overflowMs = selectedReplayTimeMs - static_cast<double>(*candidateSet.replayLatencyLimitMs);
            const auto overflowFactor = overflowMs / std::max<double>(*candidateSet.replayLatencyLimitMs, 1.0);
            replayTimePrice += latencyPriceStep(candidateSet) * (1.0 + overflowFactor);
        }

        if (!violatesBudget && !violatesReplayLatency)
        {
            const auto maintenanceCost = std::accumulate(
                selectedBoundary.begin(),
                selectedBoundary.end(),
                0.0,
                [](const double total, const SelectedRecordingBoundary& selected)
                { return total + selected.chosenOption.cost.maintenanceCost; });
            return RecordingBoundarySelection{
                .selectedBoundary = std::move(selectedBoundary),
                .objectiveCost = maintenanceCost + (replayTimePrice * selectedReplayTimeMs)};
        }

        const auto selectionSignature = makeSelectionSignature(selectedBoundary);
        if (!seenSelections.insert(selectionSignature).second)
        {
            if (violatesBudget)
            {
                throw PlacementFailure("{}", describeBudgetViolation(selectedStorageBytesByHost, availableStorageBytesByHost));
            }
            if (violatesReplayLatency && candidateSet.replayLatencyLimitMs.has_value())
            {
                throw PlacementFailure(
                    "{}",
                    describeReplayLatencyViolation(selectedReplayTimeMs, static_cast<double>(*candidateSet.replayLatencyLimitMs)));
            }
        }
    }

    throw PlacementFailure("Replay selection is infeasible: replay min-cut did not converge within {} iterations", MAX_SHADOW_PRICE_ITERATIONS);
}
}
