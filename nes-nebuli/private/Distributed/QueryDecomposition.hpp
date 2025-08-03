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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>

#include <DataTypes/Schema.hpp>
#include <Distributed/NetworkTopology.hpp>
#include <Distributed/OperatorPlacement.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <QueryConfig.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

class BridgeLogicalOperator final : public LogicalOperatorConcept
{
public:
    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override;
    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] TraitSet getTraitSet() const override;
    [[nodiscard]] LogicalOperator withTraitSet(TraitSet traitSet) const override;

    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const override;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override
    {
        if (verbosity == ExplainVerbosity::Debug)
        {
            return fmt::format("BRIDGE(opId: {})", id);
        }
        return fmt::format("BRIDGE");
    }

    [[nodiscard]] std::string_view getName() const noexcept override { return NAME; };

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;

    void setOutputSchema(Schema schema);

private:
    static constexpr std::string_view NAME = "Bridge";

    std::vector<LogicalOperator> children;
    Schema inputSchema, outputSchema;
    TraitSet traitSet;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

class QueryDecomposer
{
    LogicalPlan inputPlan;
    const TopologyGraph& topology;
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;

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
        std::shared_ptr<SourceCatalog> sourceCatalog,
        std::shared_ptr<SinkCatalog> sinkCatalog);

public:
    static QueryDecomposer from(
        OperatorPlacer::PlacedLogicalPlan&& placedPlan,
        const TopologyGraph& topology,
        std::shared_ptr<SourceCatalog> sourceCatalog,
        std::shared_ptr<SinkCatalog> sinkCatalog);

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

    LogicalOperator createNetworkSink(const NetworkChannel& channel) const;
    LogicalOperator createNetworkSource(const NetworkChannel& channel) const;

    [[nodiscard]] auto view() { return planByNode; }
    [[nodiscard]] auto begin() { return planByNode.begin(); }
    [[nodiscard]] auto end() { return planByNode.end(); }
};
}