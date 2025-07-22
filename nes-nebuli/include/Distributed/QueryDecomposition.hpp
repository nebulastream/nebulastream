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
#include <string>
#include <unordered_map>
#include <vector>

#include <Distributed/NetworkTopology.hpp>
#include <Distributed/OperatorPlacement.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryConfig.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>

namespace NES
{

class QueryDecomposer
{
    LogicalPlan inputPlan;
    const TopologyGraph& topology;
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;

    std::unordered_map<TopologyGraph::NodeId, LogicalPlan> planByNode;
    std::unordered_map<OperatorId, TopologyGraph::NodeId> placement;
    struct NetworkChannel
    {
        CLI::ChannelId id;
        OperatorId upstreamOp;
        OperatorId downstreamOp;
    };
    std::vector<NetworkChannel> channels;

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
    void distributePlanFragments();
    void connectPlanFragments();

    void attachChildToParentOnSameNode(const LogicalOperator& parent, const LogicalOperator& child, const TopologyGraph::NodeId& node);

    void handleCrossNodeConnection(
        const LogicalOperator& parent,
        const LogicalOperator& child,
        const TopologyGraph::NodeId& parentNode,
        const TopologyGraph::NodeId& childNode);

    void addOperatorAsRoot(const LogicalOperator& op);

    LogicalOperator findOperatorById(const OperatorId id) const
    {
        const auto nodeId = placement.at(id);
        return getOperatorById(planByNode.at(nodeId), id).value();
    }

    LogicalOperator createNetworkSink(const NetworkChannel& channel);
    LogicalOperator createNetworkSource(const NetworkChannel& channel);

    [[nodiscard]] auto view() { return planByNode; }
    [[nodiscard]] auto begin() { return planByNode.begin(); }
    [[nodiscard]] auto end() { return planByNode.end(); }
};
}