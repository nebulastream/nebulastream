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

#include <LegacyOptimizer/BottomUpPlacement.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <map>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <highs/interfaces/highs_c_api.h>
#include <util/HighsInt.h>
#include <ErrorHandling.hpp>
#include <NetworkTopology.hpp>
#include <WorkerConfig.hpp>
#include <scope_guard.hpp>

namespace NES
{
namespace
{
size_t operatorCapacityDemand(const LogicalOperator& op)
{
    if (op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        return 0;
    }
    if (op.tryGetAs<SinkLogicalOperator>())
    {
        return 0;
    }
    return 1;
}

LogicalOperator addPlacementTrait(const LogicalOperator& op, const std::unordered_map<OperatorId, NetworkTopology::NodeId>& placement)
{
    auto oldTraitSet = op.getTraitSet();
    USED_IN_DEBUG auto addedTrait = oldTraitSet.tryInsert(PlacementTrait(placement.at(op.getId())));
    INVARIANT(addedTrait, "There should not have been a placement trait");

    return op.withTraitSet(oldTraitSet)
        .withChildren(
            op.getChildren()
            | std::views::transform(
                [&placement](const LogicalOperator& child) -> LogicalOperator { return addPlacementTrait(child, placement); })
            | std::ranges::to<std::vector>());
}

constexpr auto MODEL_STATUS_STRINGS = std::to_array<std::string_view>(
    {"Not Set",
     "Load Error",
     "Model Error",
     "Presolve Error",
     "Solve Error",
     "Postsolve Error",
     "Model Empty",
     "Optimal",
     "Infeasible",
     "Primal Infeasible or Unbounded",
     "Unbounded",
     "Bound on Objective Reached",
     "Target for Objective Reached",
     "Time Limit Reached",
     "Iteration Limit Reached",
     "Solution Limit Reached",
     "Interrupt",
     "Unknown"});

constexpr void checkError(HighsInt status, void* highs)
{
    if (status == kHighsStatusError)
    {
        if (highs != nullptr)
        {
            auto status = Highs_getModelStatus(highs);
            throw UnknownException("Highs Failed with an error: {}", MODEL_STATUS_STRINGS.at(status));
        }
        throw UnknownException("Highs Failed with an unknown error: {}");
    }

    if (status == kHighsStatusWarning)
    {
        if (highs)
        {
            auto status = Highs_getModelStatus(highs);
            NES_WARNING("Highs Warning: {}", MODEL_STATUS_STRINGS.at(status));
            return;
        }
        NES_WARNING("Highs produced a warning without context");
        return;
    }
    INVARIANT(status == kHighsStatusOk, "Highs returned an unexpected status: {}", status);
}

void validatePlan(const NetworkTopology& topology, const LogicalPlan& plan)
{
    std::vector<std::string> errors;
    for (const auto& sourceOperator : getOperatorByType<SourceDescriptorLogicalOperator>(plan))
    {
        if (auto placement = sourceOperator->getSourceDescriptor().getWorkerId(); !topology.contains(placement))
        {
            errors.emplace_back(fmt::format("Source '{}' was placed on non-existing worker '{}'", sourceOperator.getId(), placement));
        }
    }


    for (const auto& sinkOperator : getOperatorByType<SinkLogicalOperator>(plan))
    {
        if (auto placement = sinkOperator->getSinkDescriptor()->getWorkerId(); !topology.contains(placement))
        {
            errors.emplace_back(fmt::format("Sink '{}' was placed on non-existing worker '{}'", sinkOperator.getId(), placement));
        }
    }

    if (!errors.empty())
    {
        throw PlacementFailure(fmt::format("Found errors in query plan:\n{}", fmt::join(errors, "\n")));
    }
}

std::optional<std::unordered_map<OperatorId, NetworkTopology::NodeId>> solvePlacement(
    const LogicalPlan& logicalPlan, const NetworkTopology& topology, const std::unordered_map<NetworkTopology::NodeId, size_t>& capacity)
{
    auto* highs = Highs_create();
    SCOPE_EXIT
    {
        Highs_destroy(highs);
    };

    checkError(Highs_setBoolOptionValue(highs, "output_flag", 0), highs);
    checkError(Highs_changeObjectiveSense(highs, kHighsObjSenseMinimize), highs); /// minimize
    checkError(Highs_setBoolOptionValue(highs, "log_to_console", 0), highs);
    checkError(Highs_setDoubleOptionValue(highs, "time_limit", 5.0), highs); /// 1 second
    checkError(Highs_setDoubleOptionValue(highs, "mip_rel_gap", 0.01), highs); /// 1% optimality gap

    std::map<std::pair<OperatorId, NetworkTopology::NodeId>, int> operatorPlacementMatrix;
    std::vector<std::pair<OperatorId, NetworkTopology::NodeId>> reverseIndex;
    ///         x₁   x₂   x₃  ... (variables/columns)
    ///       ┌────┬────┬────┐
    /// row 1 │ a₁₁│ a₁₂│ a₁₃│  ≤ b₁  (constraint 1)
    /// row 2 │ a₂₁│ a₂₂│ a₂₃│  ≤ b₂  (constraint 2)
    /// row 3 │ a₃₁│ a₃₂│ a₃₃│  ≤ b₃  (constraint 3)
    ///       └────┴────┴────┘

    /// You have variables: placement[op1][node1], placement[op1][node2], etc.
    /// Each of these becomes a COLUMN in the matrix
    /// Each row is a constraint lowerBound <= x1*ar1 + x2*ar2 + ... + <= upperBound
    /// Where lowerBound and upperBound for columns restrict the values of individual variables, i.e., placement of a operator on a node.

    /// Example with 2 operators, 3 nodes = 6 variables = 6 columns:
    ///   p[0][0] p[0][1] p[0][2] p[1][0] p[1][1] p[1][2]
    ///      ↓       ↓       ↓       ↓       ↓       ↓
    ///    col 0   col 1   col 2   col 3   col 4   col 5
    ///
    /// To make it easier to work with operatorPlacementMatrix implements a mapping from the placement matrix to the columns in the
    /// constraint model. If an operator o is placed on a node n then placementMatrix[o][n] = true
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        for (const NetworkTopology::NodeId& node : topology | std::views::keys)
        {
            auto index = static_cast<int>(reverseIndex.size());
            operatorPlacementMatrix[{op.getId(), node}] = index;
            /// By default we allow placement on every node. we allow values from (0,1) and limit the solution to integers which gives us
            /// exactly {0,1}. Thus if a variable is set to 0 `op` is not placed on `node` and vice versa.
            checkError(Highs_addCol(highs, index, 0, 1, 0, nullptr, nullptr), highs);
            checkError(Highs_changeColIntegrality(highs, index, kHighsVarTypeInteger), highs);
            reverseIndex.emplace_back(op.getId(), node);
        }
    }

    /// Constraint 1: Each operator assigned to exactly one node. a.k.a the sum of all rows (placementMatrix) is exactly 1
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        std::vector<int> index;
        std::vector<double> value;
        index.reserve(topology.size());
        value.reserve(topology.size());

        for (const NetworkTopology::NodeId& nodeId : topology | std::views::keys)
        {
            index.push_back(operatorPlacementMatrix.at({op.getId(), nodeId}));
            value.push_back(1.0);
        }
        checkError(Highs_addRow(highs, 1.0, 1.0, static_cast<HighsInt>(index.size()), index.data(), value.data()), highs);
    }

    /// Constraint 2: Capacity constraints
    for (const auto& nodeId : topology | std::views::keys)
    {
        std::vector<int> index;
        std::vector<double> value;
        for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
        {
            index.push_back(operatorPlacementMatrix.at({op.getId(), nodeId}));
            value.push_back(static_cast<double>(operatorCapacityDemand(op)));
        }
        checkError(
            Highs_addRow(
                highs, 0, static_cast<double>(capacity.at(nodeId)), static_cast<HighsInt>(index.size()), index.data(), value.data()),
            highs);
    }

    /// Constraint 3: Fixed source placements. Adds constraint to the placement matrix for source operators.
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        if (auto sourceOperator = op.tryGetAs<SourceDescriptorLogicalOperator>())
        {
            auto placement = sourceOperator->get().getSourceDescriptor().getWorkerId();
            /// Fix placement of source on source host node.
            const size_t var = operatorPlacementMatrix.at({op.getId(), placement});
            checkError(Highs_changeColBounds(highs, static_cast<HighsInt>(var), 1.0, 1.0), highs);
        }
    }

    /// Constraint 4: Sink placement
    auto rootOperatorId = logicalPlan.getRootOperators().front().getId();
    auto sinkOperator = logicalPlan.getRootOperators().front().getAs<SinkLogicalOperator>().get();
    const auto& sinkDescriptorOpt = sinkOperator.getSinkDescriptor();
    INVARIANT(sinkDescriptorOpt, "BUG: sink operator must have a sink descriptor");
    auto sinkPlacement = sinkDescriptorOpt->getWorkerId();

    /// Fix placement of sink on sink host node
    const auto sinkVar = operatorPlacementMatrix.at({rootOperatorId, sinkPlacement});
    checkError(Highs_changeColBounds(highs, sinkVar, 1.0, 1.0), highs);

    /// Constraint 5: Parent-child placement must respect network connectivity
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        for (const LogicalOperator& child : op.getChildren())
        {
            for (const NetworkTopology::NodeId& nodeId1 : topology | std::views::keys)
            {
                for (const NetworkTopology::NodeId& nodeId2 : topology | std::views::keys)
                {
                    if (nodeId1 != nodeId2 && topology.findPaths(nodeId1, nodeId2, NetworkTopology::Upstream).empty())
                    {
                        /// Cannot place parent at n1 and child at n2 if no path
                        /// This constraint does not allow op to be placed at node1 and child to be placed at node2, because
                        /// there is no path between the nodes.
                        std::array<int, 2> index{
                            operatorPlacementMatrix.at({op.getId(), nodeId1}), operatorPlacementMatrix.at({child.getId(), nodeId2})};
                        std::array values{1.0, 1.0};
                        checkError(Highs_addRow(highs, 0, 1.0, index.size(), index.data(), values.data()), highs);
                    }
                }
            }
        }
    }

    /// Optimization goal: minimize the sum of distances for all operator placements to the placement of its child sources
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        for (const auto& nodeId : topology | std::views::keys)
        {
            /// This calculates the sum of distances (network hops) if `op` was placed on `nodeId` to all the sources that are descended
            /// operators of `op`.
            size_t distanceFromSource = 0;
            for (const auto& child : BFSRange(op))
            {
                if (auto sourceOp = child.tryGetAs<SourceDescriptorLogicalOperator>())
                {
                    const auto placement = sourceOp->get().getSourceDescriptor().getWorkerId();
                    const auto paths = topology.findPaths(nodeId, placement, NetworkTopology::Upstream);
                    if (paths.empty())
                    {
                        /// Source is unreachable. The distance does not matter as prior constraint would rule out this placement anyways.
                        break;
                    }

                    distanceFromSource += std::ranges::min(paths, {}, [](const auto& path) { return path.path.size(); }).path.size();
                }
            }

            const auto var = operatorPlacementMatrix.at({op.getId(), nodeId});
            checkError(Highs_changeColCost(highs, var, static_cast<double>(distanceFromSource)), highs);
        }
    }


    /// Solve
    checkError(Highs_run(highs), highs);

    HighsInt model_status = Highs_getModelStatus(highs);
    HighsInt primal_status;
    checkError(Highs_getIntInfoValue(highs, "primal_solution_status", &primal_status), highs);

    const auto modelStatus = Highs_getModelStatus(highs);
    if (modelStatus == kHighsModelStatusOptimal || modelStatus == kHighsModelStatusTimeLimit || modelStatus == kHighsModelStatusInterrupt)
    {
        std::vector<double> solution(reverseIndex.size());
        checkError(Highs_getSolution(highs, solution.data(), nullptr, nullptr, nullptr), highs);
        /// extract all chosen placement variables with 1
        auto placement = std::views::zip(reverseIndex, solution)
            | std::views::filter([](const auto& placementAndSolution) { return std::get<1>(placementAndSolution) == 1.0; })
            | std::views::keys | std::ranges::to<std::unordered_map<OperatorId, NetworkTopology::NodeId>>();

        if (modelStatus == kHighsModelStatusTimeLimit || modelStatus == kHighsModelStatusInterrupt)
        {
            NES_WARNING("Found suboptimal solution for bottom-up placement")
        }
        return placement;
    }

    return std::nullopt;
}
}

void BottomUpOperatorPlacer::apply(LogicalPlan& logicalPlan)
{
    const auto topology = workerCatalog->getTopology();
    validatePlan(topology, logicalPlan);

    const auto capacity = topology | std::views::keys
        | std::views::transform(
                              [&](const auto& nodeId) -> std::pair<NetworkTopology::NodeId, size_t>
                              { return {nodeId, workerCatalog->getWorker(nodeId).value().capacity}; })
        | std::ranges::to<std::unordered_map<NetworkTopology::NodeId, size_t>>();

    const auto placement = solvePlacement(logicalPlan, topology, capacity);

    if (!placement)
    {
        throw PlacementFailure("Placement is not possible");
    }
    logicalPlan = LogicalPlan(logicalPlan.getQueryId(), {addPlacementTrait(logicalPlan.getRootOperators().front(), *placement)});
}

}
