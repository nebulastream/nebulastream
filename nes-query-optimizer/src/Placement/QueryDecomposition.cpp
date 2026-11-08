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
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UDFLogicalOperator.hpp>
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
#include <magic_enum/magic_enum.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <NetworkTopology.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <UdfDescriptor.hpp>
#include <UdfProcessCall.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

namespace
{
struct DecompositionContext
{
    std::unordered_map<NetworkTopology::NodeId, std::vector<LogicalPlan>> plansByNode;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) deliberate const-ref in local helper struct
    const QueryOptimizerNetworkConfiguration& config;
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;
    SharedPtr<const WorkerCatalog> workerCatalog;

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

    auto sourceConfig = std::unordered_map<Identifier, std::string>{
        {Identifier::parse("channel"), channel.id.getRawValue()}, {Identifier::parse("bind"), downstreamData}};
    if (context.config.receiverQueueSize.has_value())
    {
        sourceConfig.emplace(Identifier::parse("receiver_queue_size"), std::to_string((*context.config.receiverQueueSize)));
    }

    auto sinkConfig = std::unordered_map<Identifier, std::string>{
        {Identifier::parse("channel"), channel.id.getRawValue()},
        {Identifier::parse("bind"), upstreamData},
        {Identifier::parse("data_endpoint"), downstreamData},
        {Identifier::parse("output_format"), "NATIVE"}};

    if (context.config.maxPendingAcks.has_value())
    {
        sinkConfig.emplace(Identifier::parse("max_pending_acks"), std::to_string((*context.config.maxPendingAcks)));
    }
    if (context.config.senderQueueSize.has_value())
    {
        sinkConfig.emplace(Identifier::parse("sender_queue_size"), std::to_string((*context.config.senderQueueSize)));
    }
    if (context.config.backpressureUpperThreshold.has_value())
    {
        sinkConfig.emplace(Identifier::parse("backpressure_upper_threshold"), std::to_string((*context.config.backpressureUpperThreshold)));
    }
    if (context.config.backpressureLowerThreshold.has_value())
    {
        sinkConfig.emplace(Identifier::parse("backpressure_lower_threshold"), std::to_string((*context.config.backpressureLowerThreshold)));
    }

    auto orderedUpstreamSchema = channel.upstreamOp->getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
    const auto networkSourceDescriptorOpt = context.sourceCatalog->getAnonymousSource(
        Identifier::parse("Network"),
        orderedUpstreamSchema,
        Host(channel.downstreamNode.getRawValue()),
        {{InputFormatterDescriptor::TYPE_FIELD.getName(), "NATIVE"}},
        sourceConfig);
    INVARIANT(networkSourceDescriptorOpt.has_value(), "Failed to add physical source for network channel");
    const auto& networkSourceDescriptor = networkSourceDescriptorOpt.value();

    auto networkSinkDescriptor = context.sinkCatalog->getAnonymousSink(
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

/// Position of a field among the fields of the batches.
uint64_t fieldPosition(const Schema<UnqualifiedUnboundField, Ordered>& fields, const Identifier& name)
{
    const auto found = std::ranges::find_if(fields, [&](const auto& field) { return *field.getFullyQualifiedName().begin() == name; });
    INVARIANT(found != std::ranges::end(fields), "UDF operator reads or writes field {}, which its batches do not hold", name);
    return static_cast<uint64_t>(std::ranges::distance(std::ranges::begin(fields), found));
}

/// The batches the source of a UDF process emits are already allocated, as they are the ones its sink got. With the default limit, the
/// source waits for the worker threads to take them more often than for the processes.
constexpr size_t UDF_PROCESS_SOURCE_INFLIGHT_BUFFERS = 256;

/// Replaces an out-of-process UDF operator by a sink and a source on its node, which exchange the batches with the process. The sink
/// becomes a plan of its own; the source takes the operator's place in the current plan.
LogicalOperator splitOutOfProcessUdfOperator(DecompositionContext& context, const TypedLogicalOperator<UDFLogicalOperator>& udf)
{
    const auto child = udf->getChild();
    const auto node = getPlacementFor(udf);
    /// A batch holds the fields of the child and the results, so its rows fit into one buffer on the way back from the process.
    const auto fields = udf.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();

    std::vector<UdfProcessCall> calls;
    for (const auto& [name, function] : udf->getCalls())
    {
        const auto call = function.tryGetAs<UDFCallLogicalFunction>();
        INVARIANT(call.has_value() && call.value()->getDescriptor().has_value(), "A UDF operator only holds resolved UDF calls");
        const auto& descriptor = *call.value()->getDescriptor();
        UdfProcessCall description{
            .resultField = fieldPosition(fields, name),
            .name = descriptor.getName(),
            .entrypoint = descriptor.getEntrypoint(),
            .body = std::nullopt,
            .parameterNames = {},
            .arguments = {}};
        if (const auto& source = descriptor.getSource())
        {
            description.body = source->body;
            description.parameterNames = source->parameterNames;
        }
        for (const auto& argument : function.getChildren())
        {
            if (const auto access = argument.tryGetAs<FieldAccessLogicalFunction>())
            {
                description.arguments.push_back(
                    {.field = fieldPosition(fields, access.value()->getField().getLastName()),
                     .constant = std::nullopt,
                     .constantType = {}});
            }
            else
            {
                const auto constant = argument.tryGetAs<ConstantValueLogicalFunction>();
                INVARIANT(constant.has_value(), "UDF argument {} is neither a field nor a constant", argument);
                description.arguments.push_back(
                    {.field = std::nullopt,
                     .constant = constant.value()->getConstantValue(),
                     .constantType = std::string{magic_enum::enum_name(constant.value()->getDataType().type)}});
            }
        }
        calls.push_back(std::move(description));
    }

    static const auto ProcessType = Identifier::parse("PythonProcess");
    const auto channel = ChannelId(generateUUID()).getRawValue();
    const auto sinkDescriptor = context.sinkCatalog->getAnonymousSink(
        child.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields(),
        ProcessType,
        Host(node.getRawValue()),
        {{Identifier::parse("channel"), channel}, {Identifier::parse("output_format"), "NATIVE"}},
        {});
    INVARIANT(sinkDescriptor.has_value(), "Invalid sink descriptor config for a UDF process");
    /// The sink takes the fields of its child. Its field ordering is the one of the batches, which also hold the columns of the results.
    auto sinkTraitSet = child.getTraitSet()
        | std::views::filter([](const auto& trait) { return trait.getTypeInfo() != typeid(FieldOrderingTrait); })
        | std::ranges::to<TraitSet>();
    sinkTraitSet.insert(FieldOrderingTrait{fields});
    context.addPlanToNode(
        SinkLogicalOperator::create(child, sinkDescriptor.value())->withTraitSet(std::move(sinkTraitSet)).withInferredSchema(), node);

    const auto sourceDescriptor = context.sourceCatalog->getAnonymousSource(
        ProcessType,
        fields,
        Host(node.getRawValue()),
        {{InputFormatterDescriptor::TYPE_FIELD.getName(), "NATIVE"}},
        {{Identifier::parse("channel"), channel},
         {Identifier::parse("calls"), serializeUdfProcessCalls(calls)},
         {Identifier::parse("max_inflight_buffers"), std::to_string(UDF_PROCESS_SOURCE_INFLIGHT_BUFFERS)}});
    INVARIANT(sourceDescriptor.has_value(), "Invalid source descriptor config for a UDF process");
    return SourceDescriptorLogicalOperator::create(sourceDescriptor.value())->withTraitSet(udf.getTraitSet());
}

LogicalOperator decomposePlanRecursive(DecompositionContext& context, const LogicalOperator& op)
{
    std::vector<LogicalOperator> assignedChildren;
    assignedChildren.reserve(op.getChildren().size());

    for (const auto& child : op.getChildren())
    {
        assignedChildren.emplace_back(assignOperator(context, op, child));
    }

    auto assigned = op.withChildren({std::move(assignedChildren)});
    if (const auto udf = assigned.tryGetAs<UDFLogicalOperator>();
        udf.has_value() && udf.value()->getExecution() == UdfExecution::OutOfProcess)
    {
        return splitOutOfProcessUdfOperator(context, udf.value());
    }
    return assigned;
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
