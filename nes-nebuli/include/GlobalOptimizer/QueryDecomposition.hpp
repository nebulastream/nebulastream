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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <NetworkTopology.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryPlanning.hpp>

namespace NES
{

class QueryDecomposer
{
    QueryPlanningContext& context;

    std::unordered_map<Topology::NodeId, std::vector<LogicalPlan>> plansByNode;

    struct NetworkChannel
    {
        ChannelId id;
        const LogicalOperator& upstreamOp;
        Topology::NodeId upstreamNode;
        const Topology::NodeId& downstreamNode;
    };

    explicit QueryDecomposer(QueryPlanningContext& context);

public:
    static QueryDecomposer with(QueryPlanningContext& context);

    QueryDecomposer() = delete;
    ~QueryDecomposer() = default;
    QueryDecomposer(const QueryDecomposer&) = delete;
    QueryDecomposer(QueryDecomposer&&) = delete;
    QueryDecomposer& operator=(const QueryDecomposer&) = delete;
    QueryDecomposer& operator=(QueryDecomposer&&) = delete;

    PlanStage::DecomposedLogicalPlan decompose(PlanStage::PlacedLogicalPlan&& placedPlan) &&;

private:
    LogicalOperator decomposePlanRecursive(const LogicalOperator& op);
    LogicalOperator assignOperator(const LogicalOperator& op, const LogicalOperator& child);

    LogicalOperator createNetworkChannel(const LogicalOperator& op, const Topology::NodeId& startNode, const Topology::NodeId& endNode);

    void addPlanToNode(LogicalOperator&& op, const Topology::NodeId& nodeId);

    using Bridge = std::pair<LogicalOperator, LogicalOperator>;
    Bridge connect(NetworkChannel channel);

    [[nodiscard]] auto view() { return plansByNode; }
    [[nodiscard]] auto begin() { return plansByNode.begin(); }
    [[nodiscard]] auto end() { return plansByNode.end(); }
};

}