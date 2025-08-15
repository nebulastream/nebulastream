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

#include <Distributed/NetworkTopology.hpp>
#include <Distributed/OperatorPlacement.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <QueryConfig.hpp>

namespace NES
{

class QueryDecomposer
{
    LogicalPlan inputPlan;
    const TopologyGraph& topology;
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;

    std::unordered_map<TopologyGraph::NodeId, std::vector<LogicalPlan>> plansByNode;
    struct NetworkChannel
    {
        CLI::ChannelId id;
        const LogicalOperator& upstreamOp;
        TopologyGraph::NodeId upstreamNode;
        const TopologyGraph::NodeId& downstreamNode;
    };

    QueryDecomposer(
        OperatorPlacer::PlacedLogicalPlan&& placedPlan,
        const TopologyGraph& topology,
        SharedPtr<SourceCatalog> sourceCatalog,
        SharedPtr<SinkCatalog> sinkCatalog);

public:
    static QueryDecomposer from(
        OperatorPlacer::PlacedLogicalPlan&& placedPlan,
        const TopologyGraph& topology,
        SharedPtr<SourceCatalog> sourceCatalog,
        SharedPtr<SinkCatalog> sinkCatalog);

    QueryDecomposer() = delete;
    ~QueryDecomposer() = default;
    QueryDecomposer(const QueryDecomposer&) = delete;
    QueryDecomposer(QueryDecomposer&&) = delete;
    QueryDecomposer& operator=(const QueryDecomposer&) = delete;
    QueryDecomposer& operator=(QueryDecomposer&&) = delete;

    using DecomposedLogicalPlan = std::unordered_map<CLI::HostAddr, std::vector<LogicalPlan>>;
    DecomposedLogicalPlan decompose() &&;

private:
    LogicalOperator decomposePlanRecursive(const LogicalOperator& op);
    LogicalOperator assignOperator(const LogicalOperator& op, const LogicalOperator& child);

    LogicalOperator createNetworkChannel(
        const LogicalOperator& op,
        const TopologyGraph::NodeId& startNode,
        const TopologyGraph::NodeId& endNode);

    void addPlanToNode(LogicalOperator&& op, const TopologyGraph::NodeId& nodeId);

    using Bridge = std::pair<LogicalOperator, LogicalOperator>;
    Bridge connect(NetworkChannel channel);

    [[nodiscard]] auto view() { return plansByNode; }
    [[nodiscard]] auto begin() { return plansByNode.begin(); }
    [[nodiscard]] auto end() { return plansByNode.end(); }
};
}