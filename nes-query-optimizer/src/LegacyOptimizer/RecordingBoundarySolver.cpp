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
#include <queue>
#include <ranges>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

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
    double adjustedCost = 0.0;
};

struct FlowEdge
{
    int to = -1;
    int reverse = -1;
    double capacity = 0.0;
};

class DinicMaxFlow
{
public:
    explicit DinicMaxFlow(size_t nodeCount) : graph(nodeCount), level(nodeCount), progress(nodeCount) { }

    void addEdge(const int from, const int to, const double capacity)
    {
        const auto forwardIndex = static_cast<int>(graph[from].size());
        const auto backwardIndex = static_cast<int>(graph[to].size());
        graph[from].push_back(FlowEdge{.to = to, .reverse = backwardIndex, .capacity = capacity});
        graph[to].push_back(FlowEdge{.to = from, .reverse = forwardIndex, .capacity = 0.0});
    }

    [[nodiscard]] double maxFlow(const int source, const int sink)
    {
        double flow = 0.0;
        while (buildLevelGraph(source, sink))
        {
            std::ranges::fill(progress, 0);
            while (true)
            {
                const auto pushed = sendFlow(source, sink, std::numeric_limits<double>::max());
                if (pushed <= EPSILON)
                {
                    break;
                }
                flow += pushed;
            }
        }
        return flow;
    }

    [[nodiscard]] std::vector<bool> reachableFrom(const int source) const
    {
        std::vector<bool> reachable(graph.size(), false);
        std::queue<int> queue;
        queue.push(source);
        reachable[source] = true;
        while (!queue.empty())
        {
            const auto current = queue.front();
            queue.pop();
            for (const auto& edge : graph[current])
            {
                if (edge.capacity > EPSILON && !reachable[edge.to])
                {
                    reachable[edge.to] = true;
                    queue.push(edge.to);
                }
            }
        }
        return reachable;
    }

private:
    [[nodiscard]] bool buildLevelGraph(const int source, const int sink)
    {
        std::ranges::fill(level, -1);
        std::queue<int> queue;
        queue.push(source);
        level[source] = 0;

        while (!queue.empty())
        {
            const auto current = queue.front();
            queue.pop();
            for (const auto& edge : graph[current])
            {
                if (edge.capacity > EPSILON && level[edge.to] < 0)
                {
                    level[edge.to] = level[current] + 1;
                    queue.push(edge.to);
                }
            }
        }
        return level[sink] >= 0;
    }

    double sendFlow(const int current, const int sink, const double pushed)
    {
        if (current == sink)
        {
            return pushed;
        }

        for (auto& edgeIndex = progress[current]; edgeIndex < static_cast<int>(graph[current].size()); ++edgeIndex)
        {
            auto& edge = graph[current][edgeIndex];
            if (edge.capacity <= EPSILON || level[edge.to] != level[current] + 1)
            {
                continue;
            }

            const auto flow = sendFlow(edge.to, sink, std::min(pushed, edge.capacity));
            if (flow <= EPSILON)
            {
                continue;
            }

            edge.capacity -= flow;
            graph[edge.to][edge.reverse].capacity += flow;
            return flow;
        }
        return 0.0;
    }

    std::vector<std::vector<FlowEdge>> graph;
    std::vector<int> level;
    std::vector<int> progress;
};

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

        double adjustedCost = option.cost.totalCost();
        if (option.decision == RecordingSelectionDecision::CreateNewRecording)
        {
            adjustedCost += shadowPrices.contains(candidate.node)
                ? shadowPrices.at(candidate.node) * (static_cast<double>(option.cost.estimatedStorageBytes) / STORAGE_PENALTY_NORMALIZATION_BYTES)
                : 0.0;
        }

        if (!bestChoice.has_value() || adjustedCost < bestChoice->adjustedCost - EPSILON
            || (std::abs(adjustedCost - bestChoice->adjustedCost) <= EPSILON
                && option.decision == RecordingSelectionDecision::ReuseExistingRecording
                && bestChoice->option.decision == RecordingSelectionDecision::CreateNewRecording))
        {
            bestChoice = WeightedCandidateChoice{.option = option, .adjustedCost = adjustedCost};
        }
    }
    return bestChoice;
}

std::string describeCandidate(const RecordingBoundaryCandidate& candidate)
{
    return fmt::format(
        "edge {} -> {} on {}",
        candidate.edge.parentId.getRawValue(),
        candidate.edge.childId.getRawValue(),
        candidate.node.getRawValue());
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
            static_cast<int>(selected.chosenOption.decision)));
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
        if (availableStorageBytesByHost.contains(candidate.node))
        {
            continue;
        }
        const auto worker = workerCatalog->getWorker(candidate.node);
        PRECONDITION(worker.has_value(), "Recording boundary solving could not find worker {}", candidate.node);
        const auto runtimeMetrics = workerCatalog->getWorkerRuntimeMetrics(candidate.node);
        const auto usedBytes = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0);
        availableStorageBytesByHost[candidate.node] = usedBytes >= worker->recordingStorageBudget ? 0 : worker->recordingStorageBudget - usedBytes;
    }

    std::unordered_map<Host, double> shadowPrices;
    std::unordered_set<std::string, SelectedBoundarySignatureHash> seenSelections;

    for (size_t iteration = 0; iteration < MAX_SHADOW_PRICE_ITERATIONS; ++iteration)
    {
        std::unordered_map<RecordingPlanEdge, WeightedCandidateChoice, RecordingPlanEdgeHash> bestChoiceByEdge;
        double finiteCapacitySum = 0.0;
        for (const auto& candidate : candidateSet.candidates)
        {
            if (const auto bestChoice = chooseBestOption(candidate, shadowPrices); bestChoice.has_value())
            {
                bestChoiceByEdge.emplace(candidate.edge, *bestChoice);
                finiteCapacitySum += std::max(1.0, bestChoice->adjustedCost);
            }
        }

        const auto infiniteCapacity = std::max(1.0, finiteCapacitySum + 1.0) * 1024.0;

        std::unordered_map<OperatorId, int> nodeIndex;
        auto assignIndex = [&nodeIndex](const OperatorId id)
        {
            if (!nodeIndex.contains(id))
            {
                nodeIndex.emplace(id, static_cast<int>(nodeIndex.size()));
            }
        };

        assignIndex(candidateSet.rootOperatorId);
        for (const auto& edge : candidateSet.planEdges)
        {
            assignIndex(edge.parentId);
            assignIndex(edge.childId);
        }
        for (const auto leafId : candidateSet.leafOperatorIds)
        {
            assignIndex(leafId);
        }
        const auto terminalIndex = static_cast<int>(nodeIndex.size());

        DinicMaxFlow maxFlow(nodeIndex.size() + 1);
        for (const auto& edge : candidateSet.planEdges)
        {
            const auto capacity = bestChoiceByEdge.contains(edge) ? bestChoiceByEdge.at(edge).adjustedCost : infiniteCapacity;
            maxFlow.addEdge(nodeIndex.at(edge.parentId), nodeIndex.at(edge.childId), capacity);
        }
        for (const auto leafId : candidateSet.leafOperatorIds)
        {
            maxFlow.addEdge(nodeIndex.at(leafId), terminalIndex, infiniteCapacity);
        }

        const auto sourceIndex = nodeIndex.at(candidateSet.rootOperatorId);
        [[maybe_unused]] const auto cutCost = maxFlow.maxFlow(sourceIndex, terminalIndex);
        const auto reachable = maxFlow.reachableFrom(sourceIndex);

        bool cutUsesInfiniteEdge = false;
        std::vector<SelectedRecordingBoundary> selectedBoundary;
        selectedBoundary.reserve(candidateSet.candidates.size());
        for (const auto& candidate : candidateSet.candidates)
        {
            const auto parentReachable = reachable.at(nodeIndex.at(candidate.edge.parentId));
            const auto childReachable = reachable.at(nodeIndex.at(candidate.edge.childId));
            if (!(parentReachable && !childReachable))
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
            if (selected.chosenOption.decision == RecordingSelectionDecision::CreateNewRecording)
            {
                selectedStorageBytesByHost[selected.candidate.node] += selected.chosenOption.cost.estimatedStorageBytes;
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
            return RecordingBoundarySelection{
                .selectedBoundary = std::move(selectedBoundary),
                .objectiveCost = std::accumulate(
                    bestChoiceByEdge.begin(),
                    bestChoiceByEdge.end(),
                    0.0,
                    [](const double total, const auto& choice) { return total + choice.second.adjustedCost; })};
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
