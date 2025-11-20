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
#include <string_view>
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
        if (highs != nullptr)
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
        const auto& sinkDescriptorOpt = sinkOperator->getSinkDescriptor();
        if (sinkDescriptorOpt.has_value())
        {
            if (auto placement = sinkDescriptorOpt->getWorkerId(); !topology.contains(placement))
            {
                errors.emplace_back(fmt::format("Sink '{}' was placed on non-existing worker '{}'", sinkOperator.getId(), placement));
            }
        }
    }

    if (!errors.empty())
    {
        throw PlacementFailure(fmt::format("Found errors in query plan:\n{}", fmt::join(errors, "\n")));
    }
}

/// Shared state for the ILP placement model
struct PlacementModel
{
    void* highs;
    std::map<std::pair<OperatorId, NetworkTopology::NodeId>, int> operatorPlacementMatrix;
    std::vector<std::pair<OperatorId, NetworkTopology::NodeId>> reverseIndex;
};

/// Create HiGHS instance with solver options
PlacementModel createPlacementModel()
{
    auto* highs = Highs_create();
    checkError(Highs_setBoolOptionValue(highs, "output_flag", 0), highs);
    checkError(Highs_changeObjectiveSense(highs, kHighsObjSenseMinimize), highs);
    checkError(Highs_setBoolOptionValue(highs, "log_to_console", 0), highs);
    checkError(Highs_setDoubleOptionValue(highs, "time_limit", 5.0), highs); /// NOLINT(readability-magic-numbers) 5 second time limit
    checkError(Highs_setDoubleOptionValue(highs, "mip_rel_gap", 0.01), highs); /// NOLINT(readability-magic-numbers) 1% optimality gap
    return PlacementModel{.highs = highs, .operatorPlacementMatrix = {}, .reverseIndex = {}};
}

///         x₁   x₂   x₃  ... (variables/columns)
///       ┌────┬────┬────┐
/// row 1 │ a₁₁│ a₁₂│ a₁₃│  ≤ b₁  (constraint 1)
/// row 2 │ a₂₁│ a₂₂│ a₂₃│  ≤ b₂  (constraint 2)
/// row 3 │ a₃₁│ a₃₂│ a₃₃│  ≤ b₃  (constraint 3)
///       └────┴────┴────┘
///
/// You have variables: placement[op1][node1], placement[op1][node2], etc.
/// Each of these becomes a COLUMN in the matrix.
/// Each row is a constraint lowerBound <= x1*ar1 + x2*ar2 + ... + <= upperBound
/// Where lowerBound and upperBound for columns restrict the values of individual variables, i.e., placement of a operator on a node.
///
/// Example with 2 operators, 3 nodes = 6 variables = 6 columns:
///   p[0][0] p[0][1] p[0][2] p[1][0] p[1][1] p[1][2]
///      ↓       ↓       ↓       ↓       ↓       ↓
///    col 0   col 1   col 2   col 3   col 4   col 5
///
/// operatorPlacementMatrix maps from (operator, node) to column index.
/// If an operator o is placed on a node n then placementMatrix[o][n] = true
void addPlacementVariables(PlacementModel& model, const LogicalPlan& logicalPlan, const NetworkTopology& topology)
{
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        for (const NetworkTopology::NodeId& node : topology | std::views::keys)
        {
            auto index = static_cast<int>(model.reverseIndex.size());
            model.operatorPlacementMatrix[{op.getId(), node}] = index;
            /// By default we allow placement on every node. we allow values from (0,1) and limit the solution to integers which gives us
            /// exactly {0,1}. Thus if a variable is set to 0 `op` is not placed on `node` and vice versa.
            checkError(Highs_addCol(model.highs, index, 0, 1, 0, nullptr, nullptr), model.highs);
            checkError(Highs_changeColIntegrality(model.highs, index, kHighsVarTypeInteger), model.highs);
            model.reverseIndex.emplace_back(op.getId(), node);
        }
    }
}

/// Constraint: Each operator assigned to exactly one node
void addExactlyOneNodeConstraint(PlacementModel& model, const LogicalPlan& logicalPlan, const NetworkTopology& topology)
{
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        std::vector<int> index;
        std::vector<double> value;
        index.reserve(topology.size());
        value.reserve(topology.size());

        for (const NetworkTopology::NodeId& nodeId : topology | std::views::keys)
        {
            index.push_back(model.operatorPlacementMatrix.at({op.getId(), nodeId}));
            value.push_back(1.0);
        }
        checkError(Highs_addRow(model.highs, 1.0, 1.0, static_cast<HighsInt>(index.size()), index.data(), value.data()), model.highs);
    }
}

/// Constraint: Node capacity limits
void addCapacityConstraints(
    PlacementModel& model,
    const LogicalPlan& logicalPlan,
    const NetworkTopology& topology,
    const std::unordered_map<NetworkTopology::NodeId, size_t>& capacity)
{
    for (const auto& nodeId : topology | std::views::keys)
    {
        std::vector<int> index;
        std::vector<double> value;
        for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
        {
            index.push_back(model.operatorPlacementMatrix.at({op.getId(), nodeId}));
            value.push_back(static_cast<double>(operatorCapacityDemand(op)));
        }
        checkError(
            Highs_addRow(
                model.highs, 0, static_cast<double>(capacity.at(nodeId)), static_cast<HighsInt>(index.size()), index.data(), value.data()),
            model.highs);
    }
}

/// Constraint: Fix source operators to their host nodes
void addSourcePlacementConstraints(PlacementModel& model, const LogicalPlan& logicalPlan)
{
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        if (auto sourceOperator = op.tryGetAs<SourceDescriptorLogicalOperator>())
        {
            auto placement = sourceOperator->get().getSourceDescriptor().getWorkerId();
            const size_t var = model.operatorPlacementMatrix.at({op.getId(), placement});
            checkError(Highs_changeColBounds(model.highs, static_cast<HighsInt>(var), 1.0, 1.0), model.highs);
        }
    }
}

/// Constraint: Fix sink operator to its host node
void addSinkPlacementConstraint(PlacementModel& model, const LogicalPlan& logicalPlan)
{
    auto rootOperatorId = logicalPlan.getRootOperators().front().getId();
    auto sinkOperator = logicalPlan.getRootOperators().front().getAs<SinkLogicalOperator>().get();
    const auto& sinkDescriptorOpt = sinkOperator.getSinkDescriptor();
    INVARIANT(sinkDescriptorOpt, "BUG: sink operator must have a sink descriptor");
    auto sinkPlacement = sinkDescriptorOpt->getWorkerId();

    const auto sinkVar = model.operatorPlacementMatrix.at({rootOperatorId, sinkPlacement});
    checkError(Highs_changeColBounds(model.highs, sinkVar, 1.0, 1.0), model.highs);
}

/// Constraint: Parent-child placement must respect network connectivity
void addConnectivityConstraints(PlacementModel& model, const LogicalPlan& logicalPlan, const NetworkTopology& topology)
{
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
                        std::array<int, 2> index{
                            model.operatorPlacementMatrix.at({op.getId(), nodeId1}),
                            model.operatorPlacementMatrix.at({child.getId(), nodeId2})};
                        std::array values{1.0, 1.0};
                        checkError(Highs_addRow(model.highs, 0, 1.0, index.size(), index.data(), values.data()), model.highs);
                    }
                }
            }
        }
    }
}

/// Objective: minimize the sum of distances for all operator placements to their descendant sources
void addDistanceObjective(PlacementModel& model, const LogicalPlan& logicalPlan, const NetworkTopology& topology)
{
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        for (const auto& nodeId : topology | std::views::keys)
        {
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

            const auto var = model.operatorPlacementMatrix.at({op.getId(), nodeId});
            checkError(Highs_changeColCost(model.highs, var, static_cast<double>(distanceFromSource)), model.highs);
        }
    }
}

/// Run the solver and extract the placement result
std::optional<std::unordered_map<OperatorId, NetworkTopology::NodeId>> extractPlacement(PlacementModel& model)
{
    checkError(Highs_run(model.highs), model.highs);

    HighsInt primalStatus = 0;
    checkError(Highs_getIntInfoValue(model.highs, "primal_solution_status", &primalStatus), model.highs);

    const auto modelStatus = Highs_getModelStatus(model.highs);
    if (modelStatus == kHighsModelStatusOptimal || modelStatus == kHighsModelStatusTimeLimit || modelStatus == kHighsModelStatusInterrupt)
    {
        std::vector<double> solution(model.reverseIndex.size());
        checkError(Highs_getSolution(model.highs, solution.data(), nullptr, nullptr, nullptr), model.highs);
        auto placement = std::views::zip(model.reverseIndex, solution)
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

std::optional<std::unordered_map<OperatorId, NetworkTopology::NodeId>> solvePlacement(
    const LogicalPlan& logicalPlan, const NetworkTopology& topology, const std::unordered_map<NetworkTopology::NodeId, size_t>& capacity)
{
    auto model = createPlacementModel();
    SCOPE_EXIT
    {
        Highs_destroy(model.highs);
    };

    addPlacementVariables(model, logicalPlan, topology);
    addExactlyOneNodeConstraint(model, logicalPlan, topology);
    addCapacityConstraints(model, logicalPlan, topology, capacity);
    addSourcePlacementConstraints(model, logicalPlan);
    addSinkPlacementConstraint(model, logicalPlan);
    addConnectivityConstraints(model, logicalPlan, topology);
    addDistanceObjective(model, logicalPlan, topology);
    return extractPlacement(model);
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
