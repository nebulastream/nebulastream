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

#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/NESStrongType.hpp>
#include <LegacyOptimizer/QueryPlanning.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <NetworkTopology.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

using ChannelId = NESStrongUUIDType<struct ChannelId_>;

class QueryDecomposer
{
    QueryPlanningContext& context; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is managed by QueryPlanner

    std::unordered_map<Topology::NodeId, std::vector<LogicalPlan>> plansByNode;

    struct NetworkChannel
    {
        ChannelId id{ChannelId::INVALID};
        LogicalOperator upstreamOp;
        Topology::NodeId upstreamNode;
        Topology::NodeId downstreamNode;
    };

    explicit QueryDecomposer(QueryPlanningContext& context);

public:
    static QueryDecomposer with(QueryPlanningContext& context);

    QueryDecomposer() = delete;

    PlanStage::DecomposedLogicalPlan<HostAddr> decompose(PlanStage::PlacedLogicalPlan&& placedPlan) &&;

private:
    LogicalOperator decomposePlanRecursive(const LogicalOperator& op);
    LogicalOperator assignOperator(const LogicalOperator& op, const LogicalOperator& child);

    LogicalOperator createNetworkChannel(const LogicalOperator& op, const Topology::NodeId& startNode, const Topology::NodeId& endNode);

    void addPlanToNode(LogicalOperator&& op, const Topology::NodeId& nodeId);

    using Bridge = std::pair<LogicalOperator, LogicalOperator>;
    Bridge connect(const NetworkChannel& channel);

    [[nodiscard]] auto view() { return plansByNode; }

    [[nodiscard]] auto begin() { return plansByNode.begin(); }

    [[nodiscard]] auto end() { return plansByNode.end(); }
};

}
