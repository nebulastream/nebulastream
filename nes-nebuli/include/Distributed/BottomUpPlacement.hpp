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
#include <vector>

#include <Distributed/NetworkTopology.hpp>
#include <Distributed/OperatorPlacement.hpp>
#include <Distributed/WorkerCatalog.hpp>
#include <LegacyOptimizer/LegacyOptimizer.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES
{

/// Placement that traverses the topology from bottom-to-top based on the given capacities of the nodes.
/// This does not change the structure of the plan in any way, it only assigns placement traits with node placements to the given
/// logical operators.
class BottomUpOperatorPlacer final : OperatorPlacer
{
    std::optional<TopologyGraph::NodeId> sinkPlacement;

    BottomUpOperatorPlacer(const TopologyGraph& topology, WorkerCatalog& nodeCatalog);

public:
    static BottomUpOperatorPlacer from(const TopologyGraph& topology, WorkerCatalog& nodeCatalog);

    BottomUpOperatorPlacer() = delete;
    ~BottomUpOperatorPlacer() override = default;

    BottomUpOperatorPlacer(const BottomUpOperatorPlacer&) = delete;
    BottomUpOperatorPlacer(BottomUpOperatorPlacer&&) = delete;
    BottomUpOperatorPlacer& operator=(const BottomUpOperatorPlacer&) = delete;
    BottomUpOperatorPlacer& operator=(BottomUpOperatorPlacer&&) = delete;

    PlacedLogicalPlan place(LegacyOptimizer::OptimizedLogicalPlan&& inputPlan) && override;

private:
    LogicalOperator placeOperatorRecursive(const LogicalOperator& op);
    LogicalOperator placeUnaryOperator(const LogicalOperator& op, const LogicalOperator& child);
    LogicalOperator placeBinaryOperator(const LogicalOperator& op, const LogicalOperator& leftChild, const LogicalOperator& rightChild);

    /// Returns the first node with left capacity on any paths between src and dest
    /// @param src start node
    /// @param dest destination node
    /// @throws PlacementFailure, if no such node exists in the topology
    [[nodiscard]] TopologyGraph::NodeId
    findFirstNodeWithCapacityOnPath(const TopologyGraph::NodeId& src, const TopologyGraph::NodeId& dest) const;

    /// Returns the first node with left capacity on any paths between the LCAs of two source nodes
    /// @param candidates paths from the LCA between two start nodes to a destination node
    /// @throws PlacementFailure, if no such node exists in the topology
    [[nodiscard]] TopologyGraph::NodeId
    findFirstNodeWithCapacityFromLCAs(const std::vector<TopologyGraph::LowestCommonAncestor>& candidates) const;
};
}
