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
#include <iterator>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Functions/AsyncLogicalFunction.hpp>
#include <Functions/AsyncOutputLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rfl/json/write.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <NetworkTopology.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerAsyncConfiguration.hpp>
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
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) deliberate const-ref in local helper struct
    const QueryOptimizerNetworkConfiguration& config;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) deliberate const-ref in local helper struct
    const QueryOptimizerAsyncConfiguration& asyncConfig;
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

/// Flattens the calls of an async stage into the list the Async source runs. Lifting leaves fields, constants
/// and directly nested calls as arguments; a nested call is listed before the call reading it, and
/// structurally equal calls are listed once.
class AsyncCallList
{
public:
    /// `ASYNC_OUTPUT(call, i)` writes the i-th value of its call, any other call its first.
    void addResult(const Identifier& output, const LogicalFunction& function)
    {
        const auto [call, index] = resolve(function);
        calls[call].outputs.push_back({.index = index, .field = output.asCanonicalString()});
    }

    [[nodiscard]] const std::vector<AsyncCall>& get() const { return calls; }

private:
    std::vector<LogicalFunction> described;
    std::vector<AsyncCall> calls;

    /// The listed call a function reads and which of its values.
    std::pair<size_t, size_t> resolve(const LogicalFunction& function)
    {
        if (const auto output = function.tryGetAs<AsyncOutputLogicalFunction>())
        {
            return {describe(output.value()->getCall()), output.value()->getIndex()};
        }
        return {describe(function), 0};
    }

    size_t describe(const LogicalFunction& call)
    {
        if (const auto known = std::ranges::find(described, call); known != described.end())
        {
            return static_cast<size_t>(std::distance(described.begin(), known));
        }
        const auto async = call.tryGetAs<AsyncLogicalFunction>();
        INVARIANT(async.has_value(), "async stage computes {}, which is not an async call", call);
        AsyncCall description{
            .outputs = {},
            .function = std::string{call.getType()},
            .configuration = async.value()->get().getAsyncConfiguration(),
            .arguments = {}};
        for (const auto& argument : call.getChildren())
        {
            if (const auto field = argument.tryGetAs<FieldAccessLogicalFunction>())
            {
                description.arguments.push_back(
                    {.field = field.value()->getField().getLastName().asCanonicalString(),
                     .constant = std::nullopt,
                     .call = std::nullopt,
                     .output = 0});
            }
            else if (const auto constant = argument.tryGetAs<ConstantValueLogicalFunction>())
            {
                description.arguments.push_back(
                    {.field = std::nullopt, .constant = constant.value()->getConstantValue(), .call = std::nullopt, .output = 0});
            }
            else
            {
                INVARIANT(
                    argument.tryGetAs<AsyncLogicalFunction>().has_value(),
                    "async call {} has an argument that is neither a field, a constant nor an async call: {}",
                    call,
                    argument);
                const auto [earlierCall, index] = resolve(argument);
                description.arguments.push_back({.field = std::nullopt, .constant = std::nullopt, .call = earlierCall, .output = index});
            }
        }
        described.push_back(call);
        calls.push_back(std::move(description));
        return calls.size() - 1;
    }
};

/// Replaces an operator executed asynchronously by a Handoff sink / Async source pair on its own node.
/// The sink becomes a plan of its own; the source takes the operator's place in the current plan.
LogicalOperator splitAsyncOperator(DecompositionContext& context, const LogicalOperator& asyncOp)
{
    const auto projection = asyncOp.tryGetAs<ProjectionLogicalOperator>();
    INVARIANT(projection.has_value(), "only projections are executed asynchronously, got {}", asyncOp);
    INVARIANT(asyncOp.getChildren().size() == 1, "an asynchronously executed operator has exactly one child");
    const auto child = asyncOp.getChildren().front();
    const auto node = getPlacementFor(asyncOp);
    const auto channel = ChannelId(generateUUID()).getRawValue();

    /// Field accesses only forward an input field; everything else is a call.
    AsyncCallList calls;
    for (const auto& [field, function] : projection.value()->getProjections())
    {
        if (!function.tryGetAs<FieldAccessLogicalFunction>().has_value())
        {
            calls.addResult(field.getLastName(), function);
        }
    }

    /// Both halves create the channel, whichever starts first, so both need its capacity.
    std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("channel"), channel}, {Identifier::parse("output_format"), "NATIVE"}};
    std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("channel"), channel}};
    if (context.asyncConfig.channelCapacity.has_value())
    {
        const auto capacity = std::to_string(*context.asyncConfig.channelCapacity);
        sinkConfig.emplace(Identifier::parse("channel_capacity"), capacity);
        sourceConfig.emplace(Identifier::parse("channel_capacity"), capacity);
    }
    if (context.asyncConfig.maxConcurrency.has_value())
    {
        sourceConfig.emplace(Identifier::parse("max_concurrency"), std::to_string(*context.asyncConfig.maxConcurrency));
    }
    if (context.asyncConfig.maxRetries.has_value())
    {
        sourceConfig.emplace(Identifier::parse("max_retries"), std::to_string(*context.asyncConfig.maxRetries));
    }

    const auto inputFields = child.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
    const auto handoffSinkDescriptor
        = context.sinkCatalog->getAnonymousSink(inputFields, Identifier::parse("Handoff"), Host(node.getRawValue()), sinkConfig, {});
    INVARIANT(handoffSinkDescriptor.has_value(), "Invalid sink descriptor config for handoff sink");
    sourceConfig.emplace(Identifier::parse("calls"), rfl::json::write(calls.get()));
    sourceConfig.emplace(
        Identifier::parse("input_fields"),
        fmt::format(
            "{}",
            fmt::join(
                inputFields | std::views::transform([](const auto& field) { return fmt::format("{}", field.getFullyQualifiedName()); }),
                ",")));
    context.addPlanToNode(
        SinkLogicalOperator::create(child, handoffSinkDescriptor.value())->withTraitSet(child.getTraitSet()).withInferredSchema(), node);

    const auto asyncSourceDescriptor = context.sourceCatalog->getAnonymousSource(
        Identifier::parse("Async"),
        asyncOp.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields(),
        Host(node.getRawValue()),
        {{InputFormatterDescriptor::TYPE_FIELD.getName(), "NATIVE"}},
        sourceConfig);
    INVARIANT(asyncSourceDescriptor.has_value(), "Failed to add physical source for async operator");
    const auto sourceTraits = asyncOp.getTraitSet()
        | std::views::filter([](const auto& trait) { return trait.getTypeInfo() != typeid(AsyncExecutionTrait); })
        | std::ranges::to<TraitSet>();
    return SourceDescriptorLogicalOperator::create(asyncSourceDescriptor.value())->withTraitSet(sourceTraits);
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
    if (hasTrait<AsyncExecutionTrait>(op.getTraitSet()))
    {
        return splitAsyncOperator(context, assigned);
    }
    return assigned;
}
}

QueryDecomposer::QueryDecomposer(
    SharedPtr<const WorkerCatalog> workerCatalog, SharedPtr<const SourceCatalog> sourceCatalog, SharedPtr<const SinkCatalog> sinkCatalog)
    : workerCatalog(std::move(workerCatalog)), sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
{
}

DistributedLogicalPlan QueryDecomposer::decompose(
    const LogicalPlan& placedPlan,
    const QueryOptimizerNetworkConfiguration& configuration,
    const QueryOptimizerAsyncConfiguration& asyncConfiguration)
{
    PRECONDITION(placedPlan.getRootOperators().size() == 1, "BUG: query decomposition requires a single root operator");
    PRECONDITION(
        std::ranges::all_of(
            BFSRange(placedPlan.getRootOperators().front()), [](const auto& op) { return hasTrait<PlacementTrait>(op.getTraitSet()); }),
        "BUG: query decomposition requires placement of all operators");

    DecompositionContext context{
        .plansByNode = {},
        .config = configuration,
        .asyncConfig = asyncConfiguration,
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
