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
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
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
#include <WorkerConfig.hpp>

namespace NES
{

namespace
{
struct DecompositionContext
{
    std::unordered_map<NetworkTopology::NodeId, std::vector<LogicalPlan>> plansByNode;
    /// The operators of the placed plan that each local plan hosts, in the same order as `plansByNode`.
    std::unordered_map<NetworkTopology::NodeId, std::vector<std::vector<OperatorId>>> hostedOperatorsByNode;
    std::vector<NES::NetworkChannel> channels;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) deliberate const-ref in local helper struct
    const QueryOptimizerNetworkConfiguration& config;
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;
    SharedPtr<const WorkerCatalog> workerCatalog;

    /// Adds a local plan and returns its index among the local plans of the node. The channels in `incomingChannels` deliver to network
    /// sources of the plan.
    size_t addPlanToNode(
        LogicalOperator op,
        const NetworkTopology::NodeId& nodeId,
        std::vector<OperatorId> hostedOperators,
        const std::vector<size_t>& incomingChannels)
    {
        auto& plans = plansByNode[nodeId];
        plans.emplace_back(INVALID_QUERY_ID, std::vector{std::move(op)});
        hostedOperatorsByNode[nodeId].emplace_back(std::move(hostedOperators));
        const auto planIndex = plans.size() - 1;
        for (const auto channel : incomingChannels)
        {
            channels.at(channel).downstreamPlanIndex = planIndex;
        }
        return planIndex;
    }
};

/// The operators of the placed plan that end up in one local plan, and the channels that deliver to its network sources.
struct Fragment
{
    std::vector<OperatorId> operators;
    std::vector<size_t> incomingChannels;
};

struct ChannelHop
{
    ChannelId id{ChannelId::INVALID};
    LogicalOperator upstreamOp;
    NetworkTopology::NodeId upstreamNode;
    NetworkTopology::NodeId downstreamNode;
};

using Bridge = std::pair<LogicalOperator, LogicalOperator>;

Bridge createBridge(
    const WorkerCatalog& workerCatalog,
    const SourceCatalog& sourceCatalog,
    const SinkCatalog& sinkCatalog,
    const QueryOptimizerNetworkConfiguration& config,
    const ChannelHop& channel)
{
    /// Look up connection (data-plane) addresses for the upstream and downstream nodes.
    /// Network sources/sinks use the data-plane address for actual data transfer,
    /// while the Host (gRPC address) is only used for management.
    const auto downstreamWorker = workerCatalog.getWorker(channel.downstreamNode);
    INVARIANT(downstreamWorker.has_value(), "Downstream worker {} not found in catalog", channel.downstreamNode);
    const auto upstreamWorker = workerCatalog.getWorker(channel.upstreamNode);
    INVARIANT(upstreamWorker.has_value(), "Upstream worker {} not found in catalog", channel.upstreamNode);

    const auto& downstreamData = downstreamWorker->dataAddress;
    const auto& upstreamData = upstreamWorker->dataAddress;

    auto sourceConfig = std::unordered_map<Identifier, std::string>{
        {Identifier::parse("channel"), channel.id.getRawValue()}, {Identifier::parse("bind"), downstreamData}};
    if (config.receiverQueueSize.has_value())
    {
        sourceConfig.emplace(Identifier::parse("receiver_queue_size"), std::to_string((*config.receiverQueueSize)));
    }

    auto sinkConfig = std::unordered_map<Identifier, std::string>{
        {Identifier::parse("channel"), channel.id.getRawValue()},
        {Identifier::parse("bind"), upstreamData},
        {Identifier::parse("data_endpoint"), downstreamData},
        {Identifier::parse("output_format"), "NATIVE"}};

    if (config.maxPendingAcks.has_value())
    {
        sinkConfig.emplace(Identifier::parse("max_pending_acks"), std::to_string((*config.maxPendingAcks)));
    }
    if (config.senderQueueSize.has_value())
    {
        sinkConfig.emplace(Identifier::parse("sender_queue_size"), std::to_string((*config.senderQueueSize)));
    }
    if (config.backpressureUpperThreshold.has_value())
    {
        sinkConfig.emplace(Identifier::parse("backpressure_upper_threshold"), std::to_string((*config.backpressureUpperThreshold)));
    }
    if (config.backpressureLowerThreshold.has_value())
    {
        sinkConfig.emplace(Identifier::parse("backpressure_lower_threshold"), std::to_string((*config.backpressureLowerThreshold)));
    }

    auto orderedUpstreamSchema = channel.upstreamOp->getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
    const auto networkSourceDescriptorOpt = sourceCatalog.getAnonymousSource(
        Identifier::parse("Network"),
        orderedUpstreamSchema,
        Host(channel.downstreamNode.getRawValue()),
        {{InputFormatterDescriptor::TYPE_FIELD.getName(), "NATIVE"}},
        sourceConfig);
    INVARIANT(networkSourceDescriptorOpt.has_value(), "Failed to add physical source for network channel");
    const auto& networkSourceDescriptor = networkSourceDescriptorOpt.value();

    auto networkSinkDescriptor = sinkCatalog.getAnonymousSink(
        orderedUpstreamSchema, Identifier::parse("Network"), Host(channel.upstreamNode.getRawValue()), sinkConfig, {});
    INVARIANT(networkSinkDescriptor.has_value(), "Invalid sink descriptor config for network sink");

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
        SinkLogicalOperator::create(channel.upstreamOp, networkSinkDescriptor.value())->withTraitSet(upstreamTs).withInferredSchema()};
}

Bridge connect(const DecompositionContext& context, const ChannelHop& channel)
{
    return createBridge(*context.workerCatalog, *context.sourceCatalog, *context.sinkCatalog, context.config, channel);
}

/// Connects `op`, the root of `fragment` on `startNode`, with its downstream operator on `endNode`. Returns the network source on
/// `endNode` and adds the channel of the last hop to `downstreamFragment`.
LogicalOperator createNetworkChannel(
    DecompositionContext& context,
    const LogicalOperator& op,
    Fragment fragment,
    Fragment& downstreamFragment,
    const OperatorId upstreamOperatorId,
    const OperatorId downstreamOperatorId,
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

        const auto channelId = ChannelId(generateUUID());
        auto [networkSource, networkSink] = connect(
            context, ChannelHop{.id = channelId, .upstreamOp = currentOp, .upstreamNode = upstreamNode, .downstreamNode = downstreamNode});

        /// Only the first hop hosts operators of the placed plan, the other hops are forwarding plans that receive over the previous hop.
        const auto planIndex = context.addPlanToNode(
            std::move(networkSink),
            upstreamNode,
            i == 0 ? std::move(fragment.operators) : std::vector<OperatorId>{},
            i == 0 ? fragment.incomingChannels : std::vector{context.channels.size() - 1});
        context.channels.emplace_back(
            NES::NetworkChannel{
                .id = channelId.getRawValue(),
                .upstreamWorker = upstreamNode,
                .downstreamWorker = downstreamNode,
                .upstreamOperatorId = upstreamOperatorId,
                .downstreamOperatorId = downstreamOperatorId,
                .upstreamPlanIndex = planIndex,
                .downstreamPlanIndex = 0});
        currentOp = networkSource;
    }
    downstreamFragment.incomingChannels.emplace_back(context.channels.size() - 1);
    return currentOp;
}

LogicalOperator decomposePlanRecursive(DecompositionContext& context, const LogicalOperator& op, Fragment& fragment);

NetworkTopology::NodeId getPlacementFor(const LogicalOperator& op)
{
    auto placementTrait = op.getTraitSet().get<PlacementTrait>();
    return placementTrait->onNode;
}

/// `fragment` collects the operators of the placed plan that end up in the same local plan as `op`.
LogicalOperator assignOperator(DecompositionContext& context, const LogicalOperator& op, const LogicalOperator& child, Fragment& fragment)
{
    const auto opNode = getPlacementFor(op);
    const auto childNode = getPlacementFor(child);

    if (opNode == childNode)
    {
        return decomposePlanRecursive(context, child, fragment);
    }
    Fragment childFragment;
    auto assignedChild = decomposePlanRecursive(context, child, childFragment);
    return createNetworkChannel(context, assignedChild, std::move(childFragment), fragment, child.getId(), op.getId(), childNode, opNode);
}

LogicalOperator decomposePlanRecursive(DecompositionContext& context, const LogicalOperator& op, Fragment& fragment)
{
    fragment.operators.emplace_back(op.getId());
    std::vector<LogicalOperator> assignedChildren;
    assignedChildren.reserve(op.getChildren().size());

    for (const auto& child : op.getChildren())
    {
        assignedChildren.emplace_back(assignOperator(context, op, child, fragment));
    }

    return op.withChildren({std::move(assignedChildren)});
}
}

QueryDecomposer::QueryDecomposer(
    SharedPtr<const WorkerCatalog> workerCatalog, SharedPtr<const SourceCatalog> sourceCatalog, SharedPtr<const SinkCatalog> sinkCatalog)
    : workerCatalog(std::move(workerCatalog)), sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
{
}

NetworkOperators QueryDecomposer::createNetworkOperators(
    const std::string& channelId,
    const LogicalOperator& upstreamOperator,
    const Host& upstreamWorker,
    const Host& downstreamWorker,
    const QueryOptimizerNetworkConfiguration& configuration) const
{
    auto [source, sink] = createBridge(
        *workerCatalog,
        *sourceCatalog,
        *sinkCatalog,
        configuration,
        ChannelHop{
            .id = ChannelId(channelId),
            .upstreamOp = upstreamOperator,
            .upstreamNode = upstreamWorker,
            .downstreamNode = downstreamWorker});
    return {.source = std::move(source), .sink = std::move(sink)};
}

DistributedLogicalPlan
QueryDecomposer::decompose(const LogicalPlan& placedPlan, const QueryOptimizerNetworkConfiguration& configuration) const
{
    PRECONDITION(placedPlan.getRootOperators().size() == 1, "BUG: query decomposition requires a single root operator");
    PRECONDITION(
        std::ranges::all_of(
            BFSRange(placedPlan.getRootOperators().front()), [](const auto& op) { return hasTrait<PlacementTrait>(op.getTraitSet()); }),
        "BUG: query decomposition requires placement of all operators");

    DecompositionContext context{
        .plansByNode = {},
        .hostedOperatorsByNode = {},
        .channels = {},
        .config = configuration,
        .sourceCatalog = copyPtr(sourceCatalog),
        .sinkCatalog = copyPtr(sinkCatalog),
        .workerCatalog = copyPtr(workerCatalog)};

    Fragment rootFragment;
    auto root = decomposePlanRecursive(context, placedPlan.getRootOperators().front(), rootFragment).withInferredSchema();
    context.addPlanToNode(root, getPlacementFor(root), std::move(rootFragment.operators), rootFragment.incomingChannels);

    for (const auto& [node, plans] : context.plansByNode)
    {
        for (const auto& plan : plans)
        {
            NES_DEBUG("Plan fragment on node [{}]: {}", node, plan);
        }
    }

    return {std::move(context.plansByNode), placedPlan, std::move(context.channels), std::move(context.hostedOperatorsByNode)};
}

}
