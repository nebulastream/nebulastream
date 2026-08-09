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
#include <map>
#include <numeric>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
#include <PlanRewriteUtils.hpp>
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
    /// Roots collected per node while decomposing, merged into plans once decomposition is done.
    std::unordered_map<NetworkTopology::NodeId, std::vector<LogicalOperator>> rootsByNode;
    /// Network source per (operator, consuming node): an operator shared by sinks is sent to a node once, not once per consumer. It
    /// may still feed several nodes, one channel each — the subtree below it is decomposed once and deployed once all the same.
    std::map<std::pair<OperatorId, NetworkTopology::NodeId>, LogicalOperator> networkChannels;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) deliberate const-ref in local helper struct
    const QueryOptimizerNetworkConfiguration& config;
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;
    SharedPtr<const WorkerCatalog> workerCatalog;

    void addRootToNode(LogicalOperator op, const NetworkTopology::NodeId& nodeId) { rootsByNode[nodeId].emplace_back(std::move(op)); }
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

    auto sourceConfig = std::unordered_map<Identifier, std::string>{
        {Identifier::parse("channel"), channel.id.getRawValue()}, {Identifier::parse("bind"), downstreamData}};
    if (context.config.receiverQueueSize.isExplicitlySet())
    {
        sourceConfig.emplace(Identifier::parse("receiver_queue_size"), std::to_string(context.config.receiverQueueSize.getValue()));
    }

    auto sinkConfig = std::unordered_map<Identifier, std::string>{
        {Identifier::parse("channel"), channel.id.getRawValue()},
        {Identifier::parse("bind"), upstreamData},
        {Identifier::parse("data_endpoint"), downstreamData},
        {Identifier::parse("output_format"), "NATIVE"}};

    if (context.config.maxPendingAcks.isExplicitlySet())
    {
        sinkConfig.emplace(Identifier::parse("max_pending_acks"), std::to_string(context.config.maxPendingAcks.getValue()));
    }
    if (context.config.senderQueueSize.isExplicitlySet())
    {
        sinkConfig.emplace(Identifier::parse("sender_queue_size"), std::to_string(context.config.senderQueueSize.getValue()));
    }
    if (context.config.backpressureUpperThreshold.isExplicitlySet())
    {
        sinkConfig.emplace(
            Identifier::parse("backpressure_upper_threshold"), std::to_string(context.config.backpressureUpperThreshold.getValue()));
    }
    if (context.config.backpressureLowerThreshold.isExplicitlySet())
    {
        sinkConfig.emplace(
            Identifier::parse("backpressure_lower_threshold"), std::to_string(context.config.backpressureLowerThreshold.getValue()));
    }

    auto orderedUpstreamSchema = channel.upstreamOp->getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
    const auto networkSourceDescriptorOpt = context.sourceCatalog->getAnonymousSource(
        Identifier::parse("Network"),
        orderedUpstreamSchema,
        Host(channel.downstreamNode.getRawValue()),
        {{Identifier::parse(InputFormatterDescriptor::getTypeString()), "NATIVE"}},
        sourceConfig);
    INVARIANT(networkSourceDescriptorOpt.has_value(), "Failed to add physical source for network channel");
    const auto& networkSourceDescriptor = networkSourceDescriptorOpt.value();

    auto networkSinkDescriptor = context.sinkCatalog->getAnonymousSink(
        orderedUpstreamSchema, Identifier::parse("Network"), Host(channel.upstreamNode.getRawValue()), sinkConfig, {});
    INVARIANT(networkSinkDescriptor.has_value(), "Invalid sink descriptor config for network sink");

    auto memoryLayout = channel.upstreamOp.getTraitSet().get<MemoryLayoutTypeTrait>();
    const auto ts = channel.upstreamOp.getTraitSet()
        | std::views::filter([](const auto& trait) { return trait.getTypeInfo() != typeid(PlacementTrait); }) | std::ranges::to<TraitSet>();
    auto upstreamTs = ts;
    auto downstreamTs = ts;

    upstreamTs.insert(PlacementTrait{channel.upstreamNode});
    downstreamTs.insert(PlacementTrait{channel.downstreamNode});

    return Bridge{
        SourceDescriptorLogicalOperator::create(networkSourceDescriptor)->withTraitSet(downstreamTs),
        /// withChildren re-infers only this sink's own schema; the recursive withInferredSchema() would deep-copy the subtree below
        /// it, so two network sinks fed by one shared subplan would each end up with a private copy of it, sources included.
        SinkLogicalOperator::create(channel.upstreamOp, networkSinkDescriptor.value())
            ->withTraitSet(upstreamTs)
            .withChildren({channel.upstreamOp})};
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

        context.addRootToNode(std::move(networkSink), upstreamNode);
        currentOp = networkSource;
    }

    return currentOp;
}

LogicalOperator decomposePlanRecursive(
    DecompositionContext& context, const LogicalOperator& op, std::unordered_map<OperatorId, LogicalOperator>& decomposed);

NetworkTopology::NodeId getPlacementFor(const LogicalOperator& op)
{
    auto placementTrait = op.getTraitSet().get<PlacementTrait>();
    return placementTrait->onNode;
}

LogicalOperator assignOperator(
    DecompositionContext& context,
    const LogicalOperator& op,
    const LogicalOperator& child,
    std::unordered_map<OperatorId, LogicalOperator>& decomposed)
{
    auto assignedChild = decomposePlanRecursive(context, child, decomposed);

    const auto opNode = getPlacementFor(op);
    const auto childNode = getPlacementFor(child);

    if (opNode == childNode)
    {
        return assignedChild;
    }

    const auto channelKey = std::pair{child.getId(), opNode};
    if (const auto existingChannel = context.networkChannels.find(channelKey); existingChannel != context.networkChannels.end())
    {
        return existingChannel->second;
    }
    auto networkSource = createNetworkChannel(context, assignedChild, childNode, opNode);
    context.networkChannels.emplace(channelKey, networkSource);
    return networkSource;
}

/// @param decomposed operators that were already decomposed, keyed by their id before the decomposition. An operator shared between
/// sinks is reached through more than one parent, but must be decomposed only once so that all parents keep reading one instance of it.
LogicalOperator decomposePlanRecursive(
    DecompositionContext& context, const LogicalOperator& op, std::unordered_map<OperatorId, LogicalOperator>& decomposed)
{
    if (const auto alreadyDecomposed = decomposed.find(op.getId()); alreadyDecomposed != decomposed.end())
    {
        return alreadyDecomposed->second;
    }

    std::vector<LogicalOperator> assignedChildren;
    assignedChildren.reserve(op.getChildren().size());

    for (const auto& child : op.getChildren())
    {
        assignedChildren.emplace_back(assignOperator(context, op, child, decomposed));
    }

    auto decomposedOperator = op.withChildren({std::move(assignedChildren)});
    decomposed.emplace(op.getId(), decomposedOperator);
    return decomposedOperator;
}

/// Roots on one node whose subtrees overlap have to end up in the same plan, so that what they share — and the sources below it —
/// is deployed once rather than once per root. Roots that share nothing stay in plans of their own: a node hosting several
/// independent physical sources keeps one plan per source.
/// Compares every pair of roots on a node, of which there is a handful; worth revisiting only if a node ever carries many.
std::vector<std::vector<LogicalOperator>> groupRootsBySharedOperators(const std::vector<LogicalOperator>& roots)
{
    std::vector<std::unordered_set<OperatorId>> reachable;
    reachable.reserve(roots.size());
    for (const auto& root : roots)
    {
        reachable.emplace_back(
            planOperators(LogicalPlan{INVALID_QUERY_ID, {root}})
            | std::views::transform([](const LogicalOperator& op) { return op.getId(); }) | std::ranges::to<std::unordered_set>());
    }

    std::vector<size_t> groupOfRoot(roots.size());
    std::iota(groupOfRoot.begin(), groupOfRoot.end(), 0);
    for (size_t i = 0; i < roots.size(); ++i)
    {
        for (size_t j = i + 1; j < roots.size(); ++j)
        {
            if (groupOfRoot.at(i) == groupOfRoot.at(j)
                or std::ranges::none_of(reachable.at(j), [&](const auto& id) { return reachable.at(i).contains(id); }))
            {
                continue;
            }
            const auto target = groupOfRoot.at(i);
            const auto merged = groupOfRoot.at(j);
            std::ranges::replace(groupOfRoot, merged, target);
        }
    }

    std::map<size_t, std::vector<LogicalOperator>> grouped;
    for (size_t i = 0; i < roots.size(); ++i)
    {
        grouped[groupOfRoot.at(i)].push_back(roots.at(i));
    }
    return grouped | std::views::values | std::ranges::to<std::vector>();
}

}

QueryDecomposer::QueryDecomposer(
    SharedPtr<const WorkerCatalog> workerCatalog, SharedPtr<const SourceCatalog> sourceCatalog, SharedPtr<const SinkCatalog> sinkCatalog)
    : workerCatalog(std::move(workerCatalog)), sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
{
}

DistributedLogicalPlan QueryDecomposer::decompose(const LogicalPlan& placedPlan, const QueryOptimizerNetworkConfiguration& configuration)
{
    PRECONDITION(not placedPlan.getRootOperators().empty(), "BUG: query decomposition requires at least one root operator");
    PRECONDITION(
        std::ranges::all_of(planOperators(placedPlan), [](const auto& op) { return hasTrait<PlacementTrait>(op.getTraitSet()); }),
        "BUG: query decomposition requires placement of all operators");

    DecompositionContext context{
        .plansByNode = {},
        .rootsByNode = {},
        .networkChannels = {},
        .config = configuration,
        .sourceCatalog = copyPtr(sourceCatalog),
        .sinkCatalog = copyPtr(sinkCatalog),
        .workerCatalog = copyPtr(workerCatalog)};

    /// An operator shared between sinks lives on exactly one node. Sending it to a second node means duplicating it there, sources
    /// included, so a plan that would require that is rejected instead of silently reading its sources twice.
    for (const auto& op : planOperators(placedPlan))
    {
        const auto parents = getParents(placedPlan, op);
        if (parents.size() > 1
            and not std::ranges::all_of(
                parents, [&](const auto& parent) { return getPlacementFor(parent) == getPlacementFor(parents.front()); }))
        {
            throw PlacementFailure(
                "Operator {} is shared by sinks that were placed on different workers", op.explain(ExplainVerbosity::Short));
        }
    }

    std::unordered_map<OperatorId, LogicalOperator> decomposed;
    for (const auto& rootOperator : placedPlan.getRootOperators())
    {
        auto root = decomposePlanRecursive(context, rootOperator, decomposed);
        context.addRootToNode(root, getPlacementFor(root));
    }
    for (auto& [node, roots] : context.rootsByNode)
    {
        for (auto& group : groupRootsBySharedOperators(roots))
        {
            /// Decomposition inserts network sources and sinks, so the operators above them need their schema inferred again and
            /// have to pick up the origin ids of the network sources below them. Setting the children re-infers the schema locally,
            /// which keeps the part the roots share shared.
            context.plansByNode[node].emplace_back(rewritePlanBottomUp(
                LogicalPlan{INVALID_QUERY_ID, std::move(group)},
                [](const LogicalOperator& original, std::vector<LogicalOperator> children)
                { return original.withChildren(std::move(children)); }));
        }
    }

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
