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

#include <NetworkTopology.hpp>
#include <Operators/LogicalOperator.hpp>
#include <QueryPlanning.hpp>

namespace NES
{

/// Obtain the placement for op, return empty optional if placement trait is not present in op
std::optional<Topology::NodeId> getPlacementFor(const LogicalOperator& op);
/// Place operator op on node nodeId by adding the placement trait
LogicalOperator placeOperatorOnNode(const LogicalOperator& op, const Topology::NodeId& nodeId);

class OperatorPlacer
{
public:
    OperatorPlacer() = default;
    virtual ~OperatorPlacer() = default;
    OperatorPlacer(const OperatorPlacer&) = delete;
    OperatorPlacer(OperatorPlacer&&) = delete;
    OperatorPlacer& operator=(const OperatorPlacer&) = delete;
    OperatorPlacer& operator=(OperatorPlacer&&) = delete;

    virtual PlanStage::PlacedLogicalPlan place(PlanStage::OptimizedLogicalPlan&& inputPlan) && = 0;
};
}
