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

#include <Placement/QueryDecomposition.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigResolution.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/NetworkSink.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/NetworkSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <NetworkTopology.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerCatalogEntry.hpp>

#include <Configurations/ConfigField.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/SourceCatalog.hpp>

namespace NES
{

namespace
{
struct DecompositionContext
{
    std::unordered_map<NetworkTopology::NodeId, std::vector<LogicalPlan>> plansByNode;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) deliberate const-ref in local helper struct
    const QueryOptimizerNetworkConfiguration& config;
    std::shared_ptr<const SourceCatalog> sourceCatalog;
    std::shared_ptr<const SinkCatalog> sinkCatalog;
    std::shared_ptr<const WorkerCatalog> workerCatalog;

    void addPlanToNode(LogicalOperator op, const NetworkTopology::NodeId& nodeId)
    {
        plansByNode[nodeId].emplace_back(INVALID_QUERY_ID, std::vector{std::move(op)});
    }
};

struct NetworkChannel
{
    ChannelId id{ChannelId::INVALID};
    LogicalOperator upstreamOp;
    NetworkTopology::NodeId upstreamNode;
    NetworkTopology::NodeId downstreamNode;
};

using Bridge = std::pair<LogicalOperator, LogicalOperator>;

Bridge connect(const DecompositionContext& context, const NetworkChannel& channel)
{
    /// Look up connection (data-plane) addresses for the upstream and downstream nodes.
    /// Network sources/sinks use the data-plane address for actual data transfer,
    /// while the Host (gRPC address) is only used for management.
    const auto downstreamWorker = context.workerCatalog->getWorker(channel.downstreamNode);
    INVARIANT(downstreamWorker.has_value(), "Downstream worker {} not found in catalog", channel.downstreamNode);
    const auto upstreamWorker = context.workerCatalog->getWorker(channel.upstreamNode);
    INVARIANT(upstreamWorker.has_value(), "Upstream worker {} not found in catalog", channel.upstreamNode);

    const auto& downstreamData = downstreamWorker->dataAddress;
    const auto& upstreamData = upstreamWorker->dataAddress;

    auto sourceConfigValues = std::vector<LiteralConfigValue>{
        {QualifiedIdentifier::create(Identifier::parse("channel")), channel.id.getRawValue()},
        {QualifiedIdentifier::create(Identifier::parse("bind")), downstreamData}};
    if (context.config.receiverQueueSize.has_value())
    {
        sourceConfigValues.emplace_back(
            QualifiedIdentifier::create(Identifier::parse("receiver_queue_size")), static_cast<int64_t>(*context.config.receiverQueueSize));
    }
    const auto sourceConfig = Schema<LiteralConfigValue, Ordered>{std::move(sourceConfigValues)};
    auto networkSourceConfig = unwrapOrAbort(NetworkSourceConfig::fromConfig(
        InstantiatedConfig{unwrapOrAbort(toExpected(resolveConfig(sourceConfig, NetworkSource::getConfigSchema())))}));
    auto inputFormatterConfig = InputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}};

    auto sinkConfigValues = std::vector<LiteralConfigValue>{
        {QualifiedIdentifier::create(Identifier::parse("channel")), channel.id.getRawValue()},
        {QualifiedIdentifier::create(Identifier::parse("bind")), upstreamData},
        {QualifiedIdentifier::create(Identifier::parse("data_endpoint")), downstreamData}};

    if (context.config.maxPendingAcks.has_value())
    {
        sinkConfigValues.emplace_back(
            QualifiedIdentifier::create(Identifier::parse("max_pending_acks")), static_cast<int64_t>(*context.config.maxPendingAcks));
    }
    if (context.config.senderQueueSize.has_value())
    {
        sinkConfigValues.emplace_back(
            QualifiedIdentifier::create(Identifier::parse("sender_queue_size")), static_cast<int64_t>(*context.config.senderQueueSize));
    }
    const auto sinkConfig = Schema<LiteralConfigValue, Ordered>{std::move(sinkConfigValues)};

    /// The backpressure thresholds are general sink settings, applied directly to the descriptor.
    GeneralSinkConfig generalSinkConfig{.host = Host(channel.upstreamNode.getRawValue())};
    if (context.config.backpressureUpperThreshold.has_value())
    {
        generalSinkConfig.backpressureUpperThreshold = *context.config.backpressureUpperThreshold;
    }
    if (context.config.backpressureLowerThreshold.has_value())
    {
        generalSinkConfig.backpressureLowerThreshold = *context.config.backpressureLowerThreshold;
    }
    auto networkSinkConfig = unwrapOrAbort(NetworkSinkConfig::fromConfig(
        InstantiatedConfig{unwrapOrAbort(toExpected(resolveConfig(sinkConfig, NetworkSink::getConfigSchema())))}));

    auto orderedUpstreamSchema = channel.upstreamOp->getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
    const auto networkSourceDescriptorExp = PhysicalSourceBuilder{
        GeneralSourceConfig{.host = Host(channel.downstreamNode.getRawValue()), .maxInflightBuffers = std::nullopt},
        PluginSourceConfiguration{Identifier::parse("Network"), ExplicitAny{std::any{std::move(networkSourceConfig)}}},
        std::move(inputFormatterConfig),
        context.sourceCatalog}.build(orderedUpstreamSchema);

    INVARIANT(networkSourceDescriptorExp.has_value(), "Failed to add physical source for network channel");
    const auto& networkSourceDescriptor = networkSourceDescriptorExp.value();

    auto networkSinkDescriptor = context.sinkCatalog->createAnonymousSinkDescriptor(
        std::make_shared<const Schema<UnqualifiedUnboundField, Ordered>>(orderedUpstreamSchema),
        generalSinkConfig,
        PluginSinkConfiguration{Identifier::parse("Network"), ExplicitAny{std::any{std::move(networkSinkConfig)}}},
        OutputFormatterDescriptor::native());

    auto outputOriginIds = channel.upstreamOp.getTraitSet().get<OutputOriginIdsTrait>();
    auto memoryLayout = channel.upstreamOp.getTraitSet().get<MemoryLayoutTypeTrait>();
    const auto ts = channel.upstreamOp.getTraitSet()
        | std::views::filter([](const auto& trait) { return trait.getTypeInfo() != typeid(PlacementTrait); }) | std::ranges::to<TraitSet>();
    auto upstreamTs = ts;
    auto downstreamTs = ts;

    upstreamTs.insert(PlacementTrait{channel.upstreamNode});
    downstreamTs.insert(PlacementTrait{channel.downstreamNode});

    return Bridge{
        SourceDescriptorLogicalOperator::create(networkSourceDescriptor)->withTraitSet(downstreamTs),
        SinkLogicalOperator::create(channel.upstreamOp, networkSinkDescriptor)->withTraitSet(upstreamTs).withInferredSchema()};
}

LogicalOperator createNetworkChannel(
    DecompositionContext& context,
    const LogicalOperator& op,
    const NetworkTopology::NodeId& startNode,
    const NetworkTopology::NodeId& endNode)
{
    /// Ask the topology for a path of nodes that connects upstream and downstream, currently we use any of them
    const auto paths = context.workerCatalog->getTopology().findPaths(startNode, endNode, NetworkTopology::Direction::Downstream);
    if (paths.empty())
    {
        throw PlacementFailure("No path from {} to {} found", startNode, endNode);
    }
    const auto path = paths.front().path;
    INVARIANT(path.size() >= 2, "Path from {} to {} must contain at least 2 nodes", startNode, endNode);

    LogicalOperator currentOp = op;
    for (size_t i = 0; i < path.size() - 1; ++i)
    {
        const auto& upstreamNode = path.at(i);
        const auto& downstreamNode = path.at(i + 1);

        auto [networkSource, networkSink] = connect(
            context,
            NetworkChannel{
                .id = ChannelId(generateUUID()), .upstreamOp = currentOp, .upstreamNode = upstreamNode, .downstreamNode = downstreamNode});

        context.addPlanToNode(std::move(networkSink), upstreamNode);
        currentOp = networkSource;
    }

    return currentOp;
}

LogicalOperator decomposePlanRecursive(DecompositionContext& context, const LogicalOperator& op);

NetworkTopology::NodeId getPlacementFor(const LogicalOperator& op)
{
    auto placementTrait = op.getTraitSet().get<PlacementTrait>();
    return placementTrait->onNode;
}

LogicalOperator assignOperator(DecompositionContext& context, const LogicalOperator& op, const LogicalOperator& child)
{
    auto assignedChild = decomposePlanRecursive(context, child);

    const auto opNode = getPlacementFor(op);
    const auto childNode = getPlacementFor(child);

    if (opNode == childNode)
    {
        return assignedChild;
    }
    return createNetworkChannel(context, assignedChild, childNode, opNode);
}

LogicalOperator decomposePlanRecursive(DecompositionContext& context, const LogicalOperator& op)
{
    std::vector<LogicalOperator> assignedChildren;
    assignedChildren.reserve(op.getChildren().size());

    for (const auto& child : op.getChildren())
    {
        assignedChildren.emplace_back(assignOperator(context, op, child));
    }

    return op.withChildren({std::move(assignedChildren)});
}
}

QueryDecomposer::QueryDecomposer(
    SharedPtr<const WorkerCatalog> workerCatalog, SharedPtr<const SourceCatalog> sourceCatalog, SharedPtr<const SinkCatalog> sinkCatalog)
    : workerCatalog(std::move(workerCatalog)), sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
{
}

DistributedLogicalPlan QueryDecomposer::decompose(const LogicalPlan& placedPlan, const QueryOptimizerNetworkConfiguration& configuration)
{
    PRECONDITION(placedPlan.getRootOperators().size() == 1, "BUG: query decomposition requires a single root operator");
    PRECONDITION(
        std::ranges::all_of(
            BFSRange(placedPlan.getRootOperators().front()), [](const auto& op) { return hasTrait<PlacementTrait>(op.getTraitSet()); }),
        "BUG: query decomposition requires placement of all operators");

    DecompositionContext context{
        .plansByNode = {},
        .config = configuration,
        .sourceCatalog = copyPtr(sourceCatalog),
        .sinkCatalog = copyPtr(sinkCatalog),
        .workerCatalog = copyPtr(workerCatalog)};

    auto root = decomposePlanRecursive(context, placedPlan.getRootOperators().front()).withInferredSchema();
    context.addPlanToNode(root, getPlacementFor(root));

    for (const auto& [node, plans] : context.plansByNode)
    {
        for (const auto& plan : plans)
        {
            NES_DEBUG("Plan fragment on node [{}]: {}", node, plan);
        }
    }

    return {std::move(context.plansByNode), placedPlan};
}

}
