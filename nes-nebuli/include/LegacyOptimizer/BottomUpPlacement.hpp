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
#pragma once

#include <optional>
#include <LegacyOptimizer/OperatorPlacement.hpp>
#include <LegacyOptimizer/QueryPlanning.hpp>
#include <NetworkTopology.hpp>

namespace NES
{

/// Placement that traverses the topology from bottom-to-top based on the given capacities of the nodes.
/// This does not change the structure of the plan in any way, it only assigns placement traits with node placements to the given
/// logical operators.
class BottomUpOperatorPlacer final : OperatorPlacer
{
    std::optional<Topology::NodeId> sinkPlacement;
    QueryPlanningContext& context; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is managed by QueryPlanner

    explicit BottomUpOperatorPlacer(QueryPlanningContext& context);

public:
    static BottomUpOperatorPlacer with(QueryPlanningContext& context);

    BottomUpOperatorPlacer() = delete;

    PlanStage::PlacedLogicalPlan place(PlanStage::OptimizedLogicalPlan&& inputPlan) && override;
};
}
