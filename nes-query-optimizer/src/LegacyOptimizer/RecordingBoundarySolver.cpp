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
constexpr double SHADOW_PRICE_STEP = 1.0;
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
    double adjustedBoundaryCutCost = 0.0;
    double replayRecomputeCost = 0.0;
    double adjustedTotalCost = 0.0;
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

std::optional<WeightedCandidateChoice>
chooseBestOption(const RecordingBoundaryCandidate& candidate, const std::unordered_map<Host, double>& shadowPrices)
{
    std::optional<WeightedCandidateChoice> bestChoice;
    for (const auto& option : candidate.options)
    {
        if (!option.feasible)
        {
            continue;
        }

        double adjustedBoundaryCutCost = option.cost.boundaryCutCost;
        if (option.decision != RecordingSelectionDecision::ReuseExistingRecording)
        {
            adjustedBoundaryCutCost += shadowPrices.contains(option.selection.node)
                ? shadowPrices.at(option.selection.node) * (static_cast<double>(option.cost.estimatedStorageBytes) / STORAGE_PENALTY_NORMALIZATION_BYTES)
                : 0.0;
        }
        const auto replayRecomputeCost = option.cost.replayRecomputeCost;
        const auto adjustedTotalCost = adjustedBoundaryCutCost + replayRecomputeCost;

        if (!bestChoice.has_value() || adjustedTotalCost < bestChoice->adjustedTotalCost - EPSILON
            || (std::abs(adjustedTotalCost - bestChoice->adjustedTotalCost) <= EPSILON
                && decisionPreference(option.decision) < decisionPreference(bestChoice->option.decision)))
        {
            bestChoice = WeightedCandidateChoice{
                .option = option,
                .adjustedBoundaryCutCost = adjustedBoundaryCutCost,
                .replayRecomputeCost = replayRecomputeCost,
                .adjustedTotalCost = adjustedTotalCost};
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

    std::unordered_map<Host, double> shadowPrices;
    std::unordered_set<std::string, SelectedBoundarySignatureHash> seenSelections;

    for (size_t iteration = 0; iteration < MAX_SHADOW_PRICE_ITERATIONS; ++iteration)
    {
        std::unordered_map<RecordingPlanEdge, WeightedCandidateChoice, RecordingPlanEdgeHash> bestChoiceByEdge;
        LemonCapacity totalFiniteCapacity = 0;
        for (const auto& candidate : candidateSet.candidates)
        {
            if (const auto bestChoice = chooseBestOption(candidate, shadowPrices); bestChoice.has_value())
            {
                bestChoiceByEdge.emplace(candidate.edge, *bestChoice);
                totalFiniteCapacity = checkedAddCapacity(totalFiniteCapacity, scaledCapacityFromCost(bestChoice->adjustedBoundaryCutCost));
                totalFiniteCapacity = checkedAddCapacity(totalFiniteCapacity, scaledCapacityFromCost(bestChoice->replayRecomputeCost));
            }
        }

        // LEMON::Preflow cannot represent true infinity, so every mandatory edge uses a
        // sentinel capacity that is strictly larger than any realizable all-finite cut.
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

        const auto terminalNode = graph.addNode();
        LemonGraph::ArcMap<LemonCapacity> capacities(graph);

        for (const auto& edge : candidateSet.planEdges)
        {
            const auto arc = graph.addArc(graphNodesByOperator.at(edge.parentId), graphNodesByOperator.at(edge.childId));
            capacities[arc] = bestChoiceByEdge.contains(edge) ? scaledCapacityFromCost(bestChoiceByEdge.at(edge).adjustedBoundaryCutCost) : infiniteCapacity;
            if (bestChoiceByEdge.contains(edge))
            {
                const auto recomputeArc = graph.addArc(graphNodesByOperator.at(edge.childId), terminalNode);
                capacities[recomputeArc] = scaledCapacityFromCost(bestChoiceByEdge.at(edge).replayRecomputeCost);
            }
        }
        for (const auto leafId : candidateSet.leafOperatorIds)
        {
            const auto arc = graph.addArc(graphNodesByOperator.at(leafId), terminalNode);
            capacities[arc] = infiniteCapacity;
        }

        lemon::Preflow<LemonGraph, LemonGraph::ArcMap<LemonCapacity>>
            preflow(graph, capacities, graphNodesByOperator.at(candidateSet.rootOperatorId), terminalNode);
        preflow.runMinCut();

        LemonGraph::NodeMap<bool> minCutPartition(graph);
        preflow.minCutMap(minCutPartition);

        bool cutUsesInfiniteEdge = false;
        for (const auto& edge : candidateSet.planEdges)
        {
            const auto parentOnSourceSide = minCutPartition[graphNodesByOperator.at(edge.parentId)];
            const auto childOnSourceSide = minCutPartition[graphNodesByOperator.at(edge.childId)];
            if (parentOnSourceSide && !childOnSourceSide && !bestChoiceByEdge.contains(edge))
            {
                cutUsesInfiniteEdge = true;
                break;
            }
        }
        for (const auto leafId : candidateSet.leafOperatorIds)
        {
            if (minCutPartition[graphNodesByOperator.at(leafId)])
            {
                cutUsesInfiniteEdge = true;
                break;
            }
        }

        std::vector<SelectedRecordingBoundary> selectedBoundary;
        selectedBoundary.reserve(candidateSet.candidates.size());
        for (const auto& candidate : candidateSet.candidates)
        {
            const auto parentOnSourceSide = minCutPartition[graphNodesByOperator.at(candidate.edge.parentId)];
            const auto childOnSourceSide = minCutPartition[graphNodesByOperator.at(candidate.edge.childId)];
            if (!(parentOnSourceSide && !childOnSourceSide))
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
        for (const auto& selected : selectedBoundary)
        {
            if (selected.chosenOption.decision != RecordingSelectionDecision::ReuseExistingRecording)
            {
                selectedStorageBytesByHost[selected.chosenOption.selection.node] += selected.chosenOption.cost.estimatedStorageBytes;
            }
        }

        bool violatesBudget = false;
        for (const auto& [host, selectedBytes] : selectedStorageBytesByHost)
        {
            if (selectedBytes > availableStorageBytesByHost[host])
            {
                violatesBudget = true;
                const auto overflowBytes = selectedBytes - availableStorageBytesByHost[host];
                const auto overflowFactor
                    = static_cast<double>(overflowBytes) / std::max<double>(availableStorageBytesByHost[host], 1.0);
                shadowPrices[host] += SHADOW_PRICE_STEP + overflowFactor;
            }
        }

        if (!violatesBudget)
        {
            const auto boundaryCost = std::accumulate(
                selectedBoundary.begin(),
                selectedBoundary.end(),
                0.0,
                [&](const double total, const SelectedRecordingBoundary& selected)
                { return total + bestChoiceByEdge.at(selected.candidate.edge).adjustedBoundaryCutCost; });
            const auto replayRecomputeCost = std::accumulate(
                candidateSet.candidates.begin(),
                candidateSet.candidates.end(),
                0.0,
                [&](const double total, const RecordingBoundaryCandidate& candidate)
                {
                    if (!bestChoiceByEdge.contains(candidate.edge))
                    {
                        return total;
                    }
                    return minCutPartition[graphNodesByOperator.at(candidate.edge.childId)] ? total + bestChoiceByEdge.at(candidate.edge).replayRecomputeCost
                                                                                            : total;
                });
            return RecordingBoundarySelection{
                .selectedBoundary = std::move(selectedBoundary),
                .objectiveCost = boundaryCost + replayRecomputeCost};
        }

        const auto selectionSignature = makeSelectionSignature(selectedBoundary);
        if (!seenSelections.insert(selectionSignature).second)
        {
            throw PlacementFailure("{}", describeBudgetViolation(selectedStorageBytesByHost, availableStorageBytesByHost));
        }
    }

    throw PlacementFailure("Replay selection is infeasible: replay min-cut did not converge within {} iterations", MAX_SHADOW_PRICE_ITERATIONS);
}
}
