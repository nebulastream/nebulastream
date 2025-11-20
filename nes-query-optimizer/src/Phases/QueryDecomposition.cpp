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

#include <Phases/QueryDecomposition.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/OperatorPlacement.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
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
    const QueryOptimizerNetworkConfiguration& config;
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;
    SharedPtr<const WorkerCatalog> workerCatalog;

    void addPlanToNode(LogicalOperator&& op, const NetworkTopology::NodeId& nodeId)
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

    const auto& downstreamData = downstreamWorker->data;
    const auto& upstreamData = upstreamWorker->data;

    auto sourceConfig = std::unordered_map<std::string, std::string>{
        {"channel", channel.id.getRawValue()}, {"bind", downstreamData}, {"host", channel.downstreamNode.getRawValue()}};
    if (context.config.receiverQueueSize.isExplicitlySet())
    {
        sourceConfig.emplace("receiver_queue_size", std::to_string(context.config.receiverQueueSize.getValue()));
    }

    auto sinkConfig = std::unordered_map<std::string, std::string>{
        {"channel", channel.id.getRawValue()},
        {"bind", upstreamData},
        {"host", channel.upstreamNode.getRawValue()},
        {"data_endpoint", downstreamData},
        {"output_format", "NATIVE"}};

    if (context.config.maxPendingAcks.isExplicitlySet())
    {
        sinkConfig.emplace("max_pending_acks", std::to_string(context.config.maxPendingAcks.getValue()));
    }
    if (context.config.senderQueueSize.isExplicitlySet())
    {
        sinkConfig.emplace("sender_queue_size", std::to_string(context.config.senderQueueSize.getValue()));
    }
    if (context.config.backpressureUpperThreshold.isExplicitlySet())
    {
        sinkConfig.emplace("backpressure_upper_threshold", std::to_string(context.config.backpressureUpperThreshold.getValue()));
    }
    if (context.config.backpressureLowerThreshold.isExplicitlySet())
    {
        sinkConfig.emplace("backpressure_lower_threshold", std::to_string(context.config.backpressureLowerThreshold.getValue()));
    }

    const auto networkSourceDescriptorOpt
        = context.sourceCatalog->getInlineSource("Network", channel.upstreamOp.getOutputSchema(), {{"type", "Native"}}, sourceConfig);
    INVARIANT(networkSourceDescriptorOpt.has_value(), "Failed to add physical source for network channel");
    const auto& networkSourceDescriptor = networkSourceDescriptorOpt.value();

    auto networkSinkDescriptor = context.sinkCatalog->getInlineSink(channel.upstreamOp.getOutputSchema(), "Network", sinkConfig, {});
    INVARIANT(networkSinkDescriptor.has_value(), "Invalid sink descriptor config for network sink");

    auto outputOriginIds = channel.upstreamOp.getTraitSet().get<OutputOriginIdsTrait>();
    auto memoryLayout = channel.upstreamOp.getTraitSet().get<MemoryLayoutTypeTrait>();
    TraitSet ts;

    USED_IN_DEBUG auto traitInserted = ts.tryInsert(outputOriginIds.get());
    INVARIANT(traitInserted, "Failed to add output origin");
    traitInserted = ts.tryInsert(memoryLayout.get());
    INVARIANT(traitInserted, "Failed to add memory layout");

    return Bridge{
        SourceDescriptorLogicalOperator{networkSourceDescriptor}.withTraitSet(ts),
        SinkLogicalOperator{networkSinkDescriptor.value()}.withTraitSet(ts).withInferredSchema({channel.upstreamOp.getOutputSchema()})};
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
                .id = ChannelId(generateUUID()), .upstreamOp = op, .upstreamNode = upstreamNode, .downstreamNode = downstreamNode});

        context.addPlanToNode(networkSink.withChildren({std::move(currentOp)}), upstreamNode);
        currentOp = networkSource;
    }

    return currentOp;
}

LogicalOperator decomposePlanRecursive(DecompositionContext& context, const LogicalOperator& op);

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

    auto root = decomposePlanRecursive(context, placedPlan.getRootOperators().front());
    context.addPlanToNode(std::move(root), getPlacementFor(root));

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
