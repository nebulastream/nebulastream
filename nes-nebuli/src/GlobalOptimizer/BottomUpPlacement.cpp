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

#include <GlobalOptimizer/BottomUpPlacement.hpp>

#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <GlobalOptimizer/OperatorPlacement.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/Trait.hpp>
#include <ortools/sat/cp_model.h>
#include <ortools/sat/cp_model_checker.h>
#include <ortools/sat/cp_model_solver.h>
#include <ortools/sat/model.h>
#include <ortools/sat/sat_parameters.pb.h>
#include <ErrorHandling.hpp>
#include <NetworkTopology.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{
namespace
{
size_t operatorCapacityDemand(const LogicalOperator& op)
{
    if (op.tryGet<SourceDescriptorLogicalOperator>())
    {
        return 0;
    }
    if (op.tryGet<SinkLogicalOperator>())
    {
        return 0;
    }
    return 1;
}

std::optional<std::unordered_map<OperatorId, Topology::NodeId>>
solvePlacement(const Topology& topology, const LogicalPlan& logicalPlan, const std::unordered_map<Topology::NodeId, size_t>& capacity)
{
    PRECONDITION(logicalPlan.getRootOperators().size() == 1, "Bottom up placement only supports single root operators");

    using namespace operations_research::sat;

    CpModelBuilder modelBuilder;
    std::map<std::pair<OperatorId, Topology::NodeId>, BoolVar> operatorPlacementMatrix;

    /// Create placement matrix. The placement matrix has an entry for every operator, node combination.
    /// If an operator o is placed on a node n than placementMatrix[o][n] = true
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        for (const Topology::NodeId& node : topology | std::views::keys)
        {
            operatorPlacementMatrix.try_emplace({op.getId(), node}, modelBuilder.NewBoolVar());
        }
    }

    /// Constraint 1: Each operator assigned to exactly one node. a.k.a the sum of all rows is 1
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        std::vector<BoolVar> placement_vars;
        for (const Topology::NodeId& nodeId : topology | std::views::keys)
        {
            placement_vars.push_back(operatorPlacementMatrix.at({op.getId(), nodeId}));
        }
        modelBuilder.AddEquality(LinearExpr::Sum(placement_vars), 1);
    }

    /// Constraint 2: Capacity constraints
    for (const auto& nodeId : topology | std::views::keys)
    {
        LinearExpr total_demand;
        for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
        {
            total_demand += LinearExpr::Term(operatorPlacementMatrix.at({op.getId(), nodeId}), operatorCapacityDemand(op));
        }
        modelBuilder.AddLessOrEqual(total_demand, capacity.at(nodeId));
    }

    /// Constraint 3: Fixed source placements. Adds constraint to the placement matrix for source operators.
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        if (auto sourceOperator = op.tryGet<SourceDescriptorLogicalOperator>())
        {
            auto placement = sourceOperator->getSourceDescriptor().getWorkerId();
            modelBuilder.AddEquality(operatorPlacementMatrix.at({op.getId(), placement}), true);

            /// Set other placements for this source to 0
            for (const Topology::NodeId& nodeId : topology | std::views::keys)
            {
                if (nodeId != placement)
                {
                    modelBuilder.AddEquality(operatorPlacementMatrix.at({op.getId(), nodeId}), false);
                }
            }
        }
    }

    /// Constraint 4: Sink placement
    auto rootOperatorId = logicalPlan.getRootOperators().front().getId();
    auto sinkOperator = logicalPlan.getRootOperators().front().tryGet<SinkLogicalOperator>().value();
    modelBuilder.AddEquality(operatorPlacementMatrix.at({rootOperatorId, sinkOperator.getSinkDescriptor()->getWorkerId()}), true);

    /// Set other placements for sink to 0
    for (const Topology::NodeId& nodeId : topology | std::views::keys)
    {
        if (nodeId != sinkOperator.getSinkDescriptor()->getWorkerId())
        {
            modelBuilder.AddEquality(operatorPlacementMatrix.at({rootOperatorId, nodeId}), false);
        }
    }

    /// Constraint 5: Parent-child placement must respect network connectivity
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        for (const LogicalOperator& child : op.getChildren())
        {
            for (const Topology::NodeId& nodeId1 : topology | std::views::keys)
            {
                for (const Topology::NodeId& nodeId2 : topology | std::views::keys)
                {
                    if (nodeId1 != nodeId2 && topology.findPaths(nodeId1, nodeId2, Topology::Upstream).empty())
                    {
                        /// Cannot place parent at n1 and child at n2 if no path
                        /// This constraint does not allow op to be placed at node1 and child to be placed at node2, because
                        /// there is no path between the nodes.
                        modelBuilder.AddLessOrEqual(
                            operatorPlacementMatrix.at({op.getId(), nodeId1}) + operatorPlacementMatrix.at({child.getId(), nodeId2}), 1);
                    }
                }
            }
        }
    }

    /// Optmization goal: minimize the sum of distances for all operator placements to the placement of its child sources,
    LinearExpr distanceFromSource;
    for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
    {
        for (const auto& nodeId : topology | std::views::keys)
        {
            for (const auto& child1 : op.getChildren())
            {
                for (const auto& child : BFSRange(child1))
                {
                    if (auto sourceOp = child.tryGet<SourceDescriptorLogicalOperator>())
                    {
                        const auto placement = sourceOp->getSourceDescriptor().getWorkerId();
                        if (const auto paths = topology.findPaths(nodeId, placement, Topology::Upstream); !paths.empty())
                        {
                            auto distance = std::ranges::min(paths, {}, [](const auto& path) { return path.path.size(); }).path.size();
                            distanceFromSource += LinearExpr::Term(operatorPlacementMatrix.at({op.getId(), nodeId}), distance);
                        }
                    }
                }
            }
        }
    }
    modelBuilder.Minimize(distanceFromSource);

    /// Set solver parameters. Nothing special here: We don't want to search longer than one second (which is arguably already alot) and we
    /// don't want to use more than one thread
    Model model;
    SatParameters parameters;
    parameters.set_max_time_in_seconds(1.0);
    parameters.set_num_search_workers(1);
    parameters.set_log_search_progress(false);
    model.Add(NewSatParameters(parameters));

    auto cpModel = modelBuilder.Build();
    /// Solve and extract solution
    const CpSolverResponse response = SolveCpModel(cpModel, &model);

    if (response.status() == MODEL_INVALID)
    {
        NES_ERROR("Placement Model is invalid: {}", ValidateCpModel(cpModel));
        return {};
    }

    if (response.status() == UNKNOWN)
    {
        NES_ERROR("Placement solver did not find a feasible solution within the time limit");
        return {};
    }

    if (response.status() == OPTIMAL || response.status() == FEASIBLE)
    {
        /// Solver find a solution but it was not necessairly optmial. I guess thats fine but we should warn the user.
        if (response.status() == FEASIBLE)
        {
            NES_WARNING("Placement found a non optimal solution");
        }

        std::unordered_map<OperatorId, Topology::NodeId> solution;
        for (const LogicalOperator& op : BFSRange(logicalPlan.getRootOperators().front()))
        {
            for (const Topology::NodeId& nodeId : topology | std::views::keys)
            {
                if (SolutionBooleanValue(response, operatorPlacementMatrix.at({op.getId(), nodeId})))
                {
                    solution[op.getId()] = nodeId;
                    break;
                }
            }
        }
        return solution;
    }

    /// Solver did not find any feasible solution, thus placement has failed.
    return {};
}
}

BottomUpOperatorPlacer::BottomUpOperatorPlacer(QueryPlanningContext& context) : OperatorPlacer{}, context{context}
{
}

BottomUpOperatorPlacer BottomUpOperatorPlacer::with(QueryPlanningContext& context)
{
    return BottomUpOperatorPlacer{context};
}

LogicalOperator addPlacementTrait(const LogicalOperator& op, const std::unordered_map<OperatorId, Topology::NodeId>& placement)
{
    auto updatedTraitSet = op.getTraitSet();
    updatedTraitSet.insert(PlacementTrait{placement.at(op.getId())});
    return op
        .withChildren(
            op.getChildren()
            | std::views::transform(
                [&placement](const LogicalOperator& child) -> LogicalOperator { return addPlacementTrait(child, placement); })
            | std::ranges::to<std::vector>())
        .withTraitSet(updatedTraitSet);
}

PlanStage::PlacedLogicalPlan BottomUpOperatorPlacer::place(PlanStage::OptimizedLogicalPlan&& inputPlan) &&
{
    const auto topology = context.workerCatalog->getTopology();
    const auto capacity = topology | std::views::keys
        | std::views::transform(
                              [&](const auto& nodeId) -> std::pair<Topology::NodeId, size_t>
                              { return {nodeId, context.workerCatalog->getWorker(nodeId).value().capacity}; })
        | std::ranges::to<std::unordered_map>();

    const auto placement = solvePlacement(topology, inputPlan.plan, capacity);

    if (!placement)
    {
        throw PlacementFailure("Placement is not possible");
    }

    return PlanStage::PlacedLogicalPlan(LogicalPlan(addPlacementTrait(inputPlan.plan.getRootOperators().front(), *placement)));
}

}
