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

#include <Distributed/QueryDecomposition.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <queue>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>

#include <uuid/uuid.h>

#include <Configurations/Descriptor.hpp>
#include <Distributed/NetworkTopology.hpp>
#include <Distributed/OperatorPlacement.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

static std::string generateUUID()
{
    uuid_t bytes;
    std::array<char, 36 + 1> parsed{};
    uuid_generate(bytes);
    uuid_unparse(bytes, parsed.data());
    return {parsed.data(), 36};
}

bool BridgeLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    return getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas()
        && getInputOriginIds() == rhs.getInputOriginIds() && getOutputOriginIds() == rhs.getOutputOriginIds();
}

LogicalOperator BridgeLogicalOperator::withInferredSchema(std::vector<Schema> /*inputSchemas*/) const
{
    throw std::logic_error("Method should not be called");
}

LogicalOperator BridgeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet BridgeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator BridgeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> BridgeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema BridgeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

void BridgeLogicalOperator::setOutputSchema(Schema schema)
{
    outputSchema = schema;
}

std::vector<std::vector<OriginId>> BridgeLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> BridgeLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator BridgeLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Bridge should have one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator BridgeLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> BridgeLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator BridgeLogicalOperator::serialize() const
{
    throw std::logic_error("BridgeLogicalOperator should be part of the global query plan and should not be serialized");
}


static LogicalOperator addChild(const LogicalOperator& op, LogicalOperator&& child)
{
    auto children = op.getChildren();
    children.push_back(std::move(child));
    return op.withChildren(std::move(children));
}


LogicalOperator QueryDecomposer::createNetworkSource(const NetworkChannel& channel) const
{
    const auto upstreamOp = findOperatorById(channel.upstreamOp);
    const auto downstreamOp = findOperatorById(channel.downstreamOp);
    const auto downstreamNode = *getPlacementFor(downstreamOp);
    const auto upstreamNode = *getPlacementFor(upstreamOp);

    const auto logicalSource
        = sourceCatalog->addLogicalSource(fmt::format("Net:{}-{}", channel.upstreamOp, channel.downstreamOp), upstreamOp.getOutputSchema())
              .value();

    auto descriptor = sourceCatalog
                          ->addPhysicalSource(
                              logicalSource,
                              "Network",
                              upstreamNode,
                              {
                                  {"channel", channel.id},
                                  {"bind", downstreamNode},
                              },
                              ParserConfig{.parserType = "Native", .tupleDelimiter = "", .fieldDelimiter = ""})
                          .value();

    return SourceDescriptorLogicalOperator{std::move(descriptor)}
        .withInputOriginIds({upstreamOp.getOutputOriginIds()})
        .withOutputOriginIds(upstreamOp.getOutputOriginIds())
        .withTraitSet({PlacementTrait{downstreamNode}});
}

LogicalOperator QueryDecomposer::createNetworkSink(const NetworkChannel& channel) const
{
    const auto upstreamOp = findOperatorById(channel.upstreamOp);
    const auto upstreamNode = *getPlacementFor(upstreamOp);
    const auto downstreamOp = findOperatorById(channel.downstreamOp);
    const auto downstreamNode = *getPlacementFor(downstreamOp);

    auto networkSinkDescriptor = sinkCatalog->addSinkDescriptor(
        fmt::format("Net:{}-{}", channel.upstreamOp, channel.downstreamOp),
        upstreamOp.getOutputSchema(),
        "Network",
        upstreamNode,
        {{"channel", channel.id}, {"connection", downstreamNode}, {"bind", upstreamNode}});
    INVARIANT(networkSinkDescriptor.has_value(), "Invalid sink descriptor config for network sink");

    return SinkLogicalOperator{std::move(*networkSinkDescriptor)}
        .withInputOriginIds({upstreamOp.getOutputOriginIds()})
        .withOutputOriginIds(upstreamOp.getOutputOriginIds())
        .withTraitSet({PlacementTrait{upstreamNode}})
        .withInferredSchema({upstreamOp.getOutputSchema()});
}

QueryDecomposer::QueryDecomposer(
    OperatorPlacer::PlacedLogicalPlan&& placedPlan,
    const TopologyGraph& topology,
    std::shared_ptr<SourceCatalog> sourceCatalog,
    std::shared_ptr<SinkCatalog> sinkCatalog)
    : inputPlan{std::move(placedPlan.plan)}, topology{topology}, sourceCatalog{sourceCatalog}, sinkCatalog{sinkCatalog}
{
    for (const auto& rootOp : this->inputPlan.getRootOperators())
    {
        for (const auto& op : BFSRange(rootOp))
        {
            placement[op.getId()] = getPlacementFor(op).value();
        }
    }
}

QueryDecomposer QueryDecomposer::from(
    OperatorPlacer::PlacedLogicalPlan&& placedPlan,
    const TopologyGraph& topology,
    std::shared_ptr<SourceCatalog> sourceCatalog,
    std::shared_ptr<SinkCatalog> sinkCatalog)
{
    return QueryDecomposer(std::move(placedPlan), topology, sourceCatalog, sinkCatalog);
}

QueryDecomposer::DecomposedLogicalPlan QueryDecomposer::decompose() &&
{
    PRECONDITION(inputPlan.getRootOperators().size() == 1, "BUG: query decomposition requires single root operator");
    PRECONDITION(
        std::ranges::all_of(
            BFSRange(inputPlan.getRootOperators().front()), [](const auto& op) { return hasTrait<PlacementTrait>(op.getTraitSet()); }),
        "BUG: query decomposition requires placement of all operators");

    /// Step 1: distribute query plan among workers based on placement traits of operators
    distributePlanFragments();
    for (const auto& [node, plan] : planByNode)
    {
        NES_DEBUG("Plan fragment for node [{}] after plan distribution: \n{}", node, plan);
    }

    /// Step 2: connect plan fragments on nodes with network sources/sinks to enable sending of intermediate results
    connectPlanFragments();

    DecomposedLogicalPlan decomposed_logical_plan;
    for (const auto& [node, plan] : planByNode)
    {
        for (const auto& root_operator : plan.getRootOperators())
        {
            decomposed_logical_plan[node].emplace_back(root_operator);
        }
    }

    return decomposed_logical_plan;
}

/// Takes the input logical plan and assumes that all operators have been assigned to a node by attaching a placement trait to them.
/// Operators are assigned to plan fragments based on their placement traits.
/// We do a BFS over the plan and operate on pairs of (parent,-child) operators until all pairs have been processed.
/// This means we do a level-order traversal in top-down direction, meaning that parents are processed before their children.
/// At all times, we maintain the invariant that the parent has been assigned to a plan fragment when assigning its child.
void QueryDecomposer::distributePlanFragments()
{
    std::queue<std::pair<LogicalOperator, LogicalOperator>> bfsQueue;
    /// Initialize with root operator placed on its node (this will be the sink of the plan)
    const auto rootOp = inputPlan.getRootOperators().front();
    addOperatorAsRoot(rootOp);
    for (const auto& childOp : rootOp.getChildren())
    {
        bfsQueue.emplace(rootOp, childOp);
    }

    /// INVARIANT for each pair of (parent, child):
    /// 1) Parent HAS already been assigned to a plan fragment (either as root above, or when it was a child in an earlier iteration)
    /// 2) Child has NOT been assigned to a plan fragment
    /// This holds true because BFS guarantees that parents are processed before their children.
    while (not bfsQueue.empty())
    {
        const auto [parent, child] = bfsQueue.front();
        bfsQueue.pop();

        for (const auto& grandChild : child.getChildren())
        {
            bfsQueue.emplace(child, grandChild);
        }

        const auto parentPlacement = getPlacementFor(parent).value();
        const auto childPlacement = getPlacementFor(child).value();
        INVARIANT(planByNode.contains(parentPlacement), "BUG: uninitialized plan fragment on node [{}]", parentPlacement);

        if (parentPlacement == childPlacement)
        {
            /// Simple case: just add child to the parent on the assigned node
            attachChildToParentOnSameNode(parent, child, parentPlacement);
        }
        else
        {
            handleCrossNodeConnection(parent, child, parentPlacement, childPlacement);
        }
    }
}

void QueryDecomposer::attachChildToParentOnSameNode(
    const LogicalOperator& parent, const LogicalOperator& child, const TopologyGraph::NodeId& node)
{
    auto& planFragment = planByNode.at(node);
    const auto parentInFragment = getOperatorById(planFragment, parent.getId()).value();

    auto updatedChildren = parentInFragment.getChildren();
    updatedChildren.push_back(child.withChildren({}));

    const auto updatedParent = parentInFragment.withChildren(std::move(updatedChildren));
    planFragment = replaceSubtree(planFragment, parent.getId(), updatedParent).value();
}

void QueryDecomposer::handleCrossNodeConnection(
    const LogicalOperator& parent,
    const LogicalOperator& child,
    const TopologyGraph::NodeId& parentNode,
    const TopologyGraph::NodeId& childNode)
{
    /// Ask the topology for a path of nodes that connects parent and child
    const auto [path] = topology.findPaths(parentNode, childNode, TopologyGraph::Upstream).front();
    INVARIANT(path.size() >= 2, "Path from {} to {} must contain at least 2 nodes", parentNode, childNode);

    // Process intermediate nodes (skip first and last)
    OperatorId downstreamOp = parent.getId();
    for (size_t i = 1; i < path.size() - 1; ++i)
    {
        NES_DEBUG("Adding bridge on {}", path.at(i));
        const auto bridgeOp
            = BridgeLogicalOperator{}.withTraitSet({PlacementTrait{path.at(i)}}).withOutputOriginIds(parent.getOutputOriginIds());
        placement[bridgeOp.getId()] = path.at(i);
        bridgeOp.get<BridgeLogicalOperator>().setOutputSchema(parent.getOutputSchema());

        addOperatorAsRoot(bridgeOp);
        channels.emplace_back(generateUUID(), bridgeOp.getId(), downstreamOp);
        downstreamOp = bridgeOp.getId();
    }

    addOperatorAsRoot(child);
    channels.emplace_back(generateUUID(), child.getId(), downstreamOp);
}

void QueryDecomposer::addOperatorAsRoot(const LogicalOperator& op)
{
    /// In both cases, we only operate on the roots:
    /// a) op is a
    /// b)
    /// Here, we give up on the constraint that each plan only has a single root (sink),
    /// because we need to slice the plan arbitrarily depending on placement
    auto opWithoutChildren = op.withChildren({});
    const auto opPlacement = *getPlacementFor(op);

    if (const auto [_, wasInserted]
        = planByNode.try_emplace(opPlacement, LogicalPlan{inputPlan.getQueryId(), {opWithoutChildren}, inputPlan.getOriginalSql()});
        not wasInserted)
    {
        planByNode.at(opPlacement) = addRootOperators(planByNode.at(opPlacement), {std::move(opWithoutChildren)});
    }
}

void QueryDecomposer::connectPlanFragments()
{
    for (const auto& channel : channels)
    {
        auto& upstreamPlan = planByNode.at(placement.at(channel.upstreamOp));
        auto upstreamOp = findOperatorById(channel.upstreamOp);
        auto networkSink = createNetworkSink(channel);
        upstreamPlan = replaceSubtree(upstreamPlan, channel.upstreamOp, networkSink.withChildren({std::move(upstreamOp)})).value();

        auto& downstreamPlan = planByNode.at(placement.at(channel.downstreamOp));
        auto downstreamOp = findOperatorById(channel.downstreamOp);
        auto networkSource = createNetworkSource(channel);
        downstreamPlan = replaceSubtree(downstreamPlan, channel.downstreamOp, addChild(downstreamOp, std::move(networkSource))).value();
    }

    for (auto& [node, plan] : planByNode)
    {
        LogicalPlan updatedPlan = plan;
        for (const auto sinks = getOperatorByType<SinkLogicalOperator>(plan); const auto& sink : sinks)
        {
            if (const auto bridge = sink.getChildren().front().tryGet<BridgeLogicalOperator>(); bridge.has_value())
            {
                updatedPlan = replaceSubtree(updatedPlan, sink.id, sink.withChildren(bridge.value().getChildren())).value();
            }
        }
        plan = updatedPlan;
        NES_DEBUG("Plan fragment for node [{}] after addition of network sources/sinks: \n{}", node, plan);
    }
}
}
