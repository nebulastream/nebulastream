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

#include <Distributed/Placement.hpp>

#include <any>
#include <functional>
#include <map>
#include <memory>
#include <ranges>
#include <stack>
#include <string>
#include <unordered_set>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <uuid/uuid.h>
#include <ErrorHandling.hpp>

constexpr static size_t NumberOfBuffersInNetworkSource = 64;

void NES::Distributed::BottomUpPlacement::doPlacement(const Topology& topology, QueryPlan& queryPlan) const
{
    auto capacity = this->capacity;
    auto getNode = [](const std::shared_ptr<Operator>& op) -> Topology::Node
    { return std::any_cast<Topology::Node>(op->getProperty(std::string(PINNED_NODE))); };
    auto firstNodeWithCapacity = [&](Topology::Node start, Topology::Node nextPlacedOperatorNode)
    {
        const auto paths = topology.findPaths(start, nextPlacedOperatorNode, Topology::Downstream);
        if (paths.empty())
        {
            throw NoPath();
        }

        auto path = paths.at(0);
        for (auto node : path.path)
        {
            if (capacity[node] > 0)
            {
                return node;
            }
        }
        return INVALID<Topology::Node>;
    };

    std::function<std::shared_ptr<Operator>(const std::shared_ptr<Operator>&)> findNextPlacedOperator
        = [&findNextPlacedOperator](const std::shared_ptr<Operator>& op) -> std::shared_ptr<Operator>
    {
        if (op->hasProperty(std::string(PINNED_NODE)))
        {
            return op;
        }
        for (const auto& parent : op->getParents())
        {
            auto node = findNextPlacedOperator(Util::as<Operator>(parent));
            if (node)
            {
                return node;
            }
        }

        return nullptr;
    };

    std::queue<std::shared_ptr<Operator>> operators;
    for (const auto& source_name_logical_operator : queryPlan.getSourceOperators<SourceDescriptorLogicalOperator>())
    {
        for (const auto& parent : source_name_logical_operator->getParents())
        {
            operators.push(Util::as<Operator>(parent));
        }
    }
    INVARIANT(!operators.empty(), "The Plan does nto contain any source operators");

    std::shared_ptr<Operator> previousOperator = nullptr;
    while (!operators.empty())
    {
        auto currentOperator = std::move(operators.front());
        operators.pop();

        previousOperator = currentOperator;

        if (currentOperator->hasProperty(std::string(PINNED_NODE)))
        {
            continue;
        }


        for (const auto& parent : currentOperator->getParents())
        {
            operators.push(Util::as<Operator>(parent));
        }

        if (!std::ranges::all_of(
                currentOperator->getChildren(),
                [](const auto& child) { return Util::as<Operator>(child)->hasProperty(std::string(PINNED_NODE)); }))
        {
            operators.push(currentOperator);
            continue;
        }

        auto childrenPlacement = currentOperator->getChildren()
            | std::views::transform([&](const auto& op) { return getNode(Util::as<Operator>(op)); })
            | std::ranges::to<std::unordered_set>();

        if (childrenPlacement.size() == 1)
        {
            auto node = *childrenPlacement.begin();
            auto targetNode = firstNodeWithCapacity(node, getNode(findNextPlacedOperator(currentOperator)));
            if (targetNode == INVALID<Topology::Node>)
            {
                throw PlacementFailed("Not enough capacities");
            }
            capacity.at(targetNode)--;
            currentOperator->addProperty(std::string(PINNED_NODE), targetNode);
        }
        else
        {
            INVARIANT(
                childrenPlacement.size() == 2,
                "BottomUp Placement cannot handle placement where children are distributed across 3 distinct nodes");
            auto nextPlacedOperator = findNextPlacedOperator(currentOperator);
            INVARIANT(nextPlacedOperator != nullptr, "BottomUp Placement cannot find an downstream operator that has been placed");

            auto candidates = topology.findCommonNode(
                *childrenPlacement.begin(), *(++childrenPlacement.begin()), getNode(nextPlacedOperator), Topology::Downstream);
            const auto candidate = candidates | std::views::transform([&](const auto& candidate) { return std::get<0>(candidate); })
                | std::views::filter([&](const Topology::Node& node) { return capacity.at(node) > 0; }) | std::ranges::to<std::vector>();
            INVARIANT(!candidate.empty(), "Could not find a path from child operators to parent operator");

            --capacity.at(candidate[0]);
            currentOperator->addProperty(std::string(PINNED_NODE), candidate[0]);
        }
    }
}

class BridgeLogicalOperator final : public NES::LogicalOperator
{
    std::shared_ptr<LogicalOperator> parent;

public:
    explicit BridgeLogicalOperator(NES::OperatorId id, std::shared_ptr<LogicalOperator> parent)
        : Operator(id), LogicalOperator(id), parent(std::move(parent))
    {
    }
    std::shared_ptr<Operator> copy() override { throw std::runtime_error("Not implemented"); }
    std::shared_ptr<NES::Schema> getOutputSchema() const override { return parent->getOutputSchema(); }
    void setOutputSchema(std::shared_ptr<NES::Schema> schema) override { parent->setOutputSchema(std::move(schema)); }
    std::vector<NES::OriginId> getOutputOriginIds() const override { return parent->getOutputOriginIds(); }
    void inferInputOrigins() override { throw std::runtime_error("Not implemented"); }
    void inferStringSignature() override { throw std::runtime_error("Not implemented"); }
    bool inferSchema() override { throw std::runtime_error("Not implemented"); }
};

NES::Distributed::DecomposedPlanDAG NES::Distributed::decompose(const Topology& topology, const QueryPlan& queryPlan)
{
    auto getPlacement
        = [&](const auto& node) { return std::any_cast<Topology::Node>(node->getProperty(std::string(Placement::PINNED_NODE))); };

    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Requires Single Root Operator");
    PRECONDITION(
        std::ranges::all_of(
            queryPlan.getAllOperators(), [](const auto& op) { return op->hasProperty(std::string(Placement::PINNED_NODE)); }),
        "Requires placement for all operators");

    auto parent = queryPlan.getRootOperators().at(0);

    auto copiedParent = parent->copy();
    copiedParent->removeChildren();

    std::stack<std::pair<std::shared_ptr<Operator>, std::shared_ptr<Operator>>> dfs;
    for (const auto& child : parent->getChildren())
    {
        dfs.emplace(copiedParent, Util::as<Operator>(child));
    }


    std::map<Topology::Node, DecomposedPlanDAG::Node> dag;
    dag.try_emplace(getPlacement(parent), queryPlan.getQueryId(), getPlacement(parent), INVALID<Topology::Node>);
    dag.at(getPlacement(parent)).plan->addRootOperator(copiedParent);

    while (!dfs.empty())
    {
        auto [parent, child] = dfs.top();
        dfs.pop();
        auto copiedChild = child->copy();
        copiedChild->removeChildren();

        for (const auto& children : child->getChildren())
        {
            dfs.emplace(copiedChild, Util::as<Operator>(children));
        }

        auto [parentNode, childNode] = std::make_pair(getPlacement(parent), getPlacement(child));
        if (parentNode == childNode)
        {
            parent->addChild(copiedChild);
            continue;
        }

        auto path = topology.findPaths(parentNode, childNode, Topology::Upstream).at(0);
        INVARIANT(path.path.size() >= 2, "Path from {} to {} must contain at least 2 nodes", childNode, parentNode);
        auto parentOperatorId = parent->getId();

        /// Path would be: [1, ..., N]. Where 1 is the node where the next pinned operator resides and N is the node where the most recent
        /// operator has been assigned to.
        /// For any other node in the path we place one bridge operator.
        /// {ParentOperator, SUCESSOR: TBD, PINNED: Node 1} {Bridge, SUCCESSOR: Parent} {Bridge1, SUCCESSOR: Bridge2} {Child, SUCCESSOR: Bridge1, PINNED: Node N}
        /// If libc++ would support std::views::slide(2) this is exactly whats happening here.
        auto nodesInPath = path.path | std::views::drop(1) | std::views::reverse;
        auto childNodeIt = std::ranges::begin(nodesInPath);
        auto successorNodeIt = childNodeIt + 1;
        while (successorNodeIt != std::ranges::end(nodesInPath))
        {
            NES_DEBUG("Adding bridge between {} and {}", *childNodeIt, *successorNodeIt);
            auto bridge = std::make_shared<BridgeLogicalOperator>(getNextOperatorId(), Util::as<LogicalOperator>(parent));
            bridge->addProperty(std::string("SUCCESSOR_OPERATOR"), parentOperatorId);
            bridge->addProperty(std::string(Placement::PINNED_NODE), *successorNodeIt);
            parentOperatorId = bridge->getId();
            dag.try_emplace(*successorNodeIt, queryPlan.getQueryId(), *successorNodeIt, *childNodeIt)
                .first->second.plan->addRootOperator(std::move(bridge));
            ++childNodeIt;
            ++successorNodeIt;
        }

        dag.try_emplace(path.path.back(), queryPlan.getQueryId(), path.path.back(), path.path.at(path.path.size() - 2))
            .first->second.plan->addRootOperator(copiedChild);
        copiedChild->addProperty(std::string("SUCCESSOR_OPERATOR"), parentOperatorId);
    }

    return DecomposedPlanDAG{std::move(dag)};
}

std::string generateUUID()
{
    uuid_t bytes;
    std::array<char, 36 + 1> parsed;
    uuid_generate(bytes);
    uuid_unparse(bytes, parsed.data());
    return std::string(parsed.data(), 36);
}

std::vector<std::shared_ptr<NES::DecomposedQueryPlan>>
NES::Distributed::connect(DecomposedPlanDAG dag, std::unordered_map<Topology::Node, PhysicalNodeConfig> physicalTopology)
{
    std::vector<std::shared_ptr<NES::DecomposedQueryPlan>> plans;
    std::unordered_map<OperatorId, std::vector<std::string>> channels;
    std::unordered_map<OperatorId, std::vector<OriginId>> originIds;
    std::unordered_map<OperatorId, std::vector<std::shared_ptr<Schema>>> schemas;

    for (const auto& node : dag)
    {
        NES_DEBUG("{}", node.plan->toString())
        if (node.plan->getOperatorByType<SinkLogicalOperator>().empty())
        {
            for (auto rootOperator : node.plan->getRootOperators())
            {
                auto networkSink = std::make_shared<SinkLogicalOperator>(getNextOperatorId());
                auto successorOperatorId = std::any_cast<OperatorId>(rootOperator->getProperty(std::string("SUCCESSOR_OPERATOR")));
                auto channelName = generateUUID();

                networkSink->sinkName = fmt::format("Net:{}-{}", successorOperatorId, rootOperator->getOutputOriginIds().at(0));
                channels[successorOperatorId].emplace_back(channelName);

                networkSink->sinkDescriptor = std::make_shared<Sinks::SinkDescriptor>(
                    "Network",
                    Configurations::DescriptorConfig::Config{
                        {"type", "Network"}, {"channel", channelName}, {"connection", physicalTopology.at(node.successorNode).connection}},
                    false);

                networkSink->setInputOriginIds(rootOperator->getOutputOriginIds());
                networkSink->setInputSchema(rootOperator->getOutputSchema());
                networkSink->setOutputSchema(rootOperator->getOutputSchema());
                INVARIANT(networkSink->getInputOriginIds().size() == 1, "NetworkSink should only have one output originId");

                originIds[successorOperatorId].emplace_back(networkSink->getInputOriginIds()[0]);
                schemas[successorOperatorId].emplace_back(networkSink->getOutputSchema());

                rootOperator->addParent(networkSink);
                node.plan->replaceRootOperator(rootOperator, networkSink);
            }
        }
    }

    for (auto node : dag)
    {
        for (const auto& leafOperator : node.plan->getLeafOperators())
        {
            if (!Util::instanceOf<SourceDescriptorLogicalOperator>(leafOperator))
            {
                for (const auto& [channelName, inputOrigin, inputSchema] :
                     std::views::zip(channels[leafOperator->getId()], originIds[leafOperator->getId()], schemas[leafOperator->getId()]))
                {
                    auto descriptor = std::make_shared<Sources::SourceDescriptor>(
                        leafOperator->getOutputSchema(),
                        fmt::format("Net:{}-{}", leafOperator->getId(), inputOrigin),
                        "Network",
                        NumberOfBuffersInNetworkSource,
                        Sources::ParserConfig{.parserType = "Raw", .tupleDelimiter = "", .fieldDelimiter = ""},
                        Configurations::DescriptorConfig::Config{
                            {"type", "Network"},
                            {"channel", channelName},
                        });

                    auto networkSource = std::make_shared<SourceDescriptorLogicalOperator>(std::move(descriptor), getNextOperatorId());
                    networkSource->setOriginId(inputOrigin);
                    networkSource->setInputSchema(inputSchema);
                    networkSource->setOutputSchema(inputSchema);
                    networkSource->addParent(leafOperator);
                }
            }
        }

        plans.emplace_back(std::make_shared<DecomposedQueryPlan>(
            node.plan->getQueryId(), physicalTopology[node.nodeId].grpc, node.plan->getRootOperators()));
    }

    for (auto node : dag)
    {
        for (const auto& bridgeOperators : node.plan->getOperatorByType<BridgeLogicalOperator>())
        {
            bridgeOperators->removeAndJoinParentAndChildren();
        }
    }

    return plans;
}

void NES::Distributed::pinSourcesAndSinks(
    QueryPlan& queryPlan, Catalogs::Source::SourceCatalog&, std::unordered_map<std::string, Topology::Node> sinks)
{
    for (const auto& sink : queryPlan.getSinkOperators())
    {
        auto sinkPlacement = sinks.at(sink->sinkName);
        sink->addProperty(std::string(Placement::PINNED_NODE), sinkPlacement);
    }

    for (auto& source : queryPlan.getOperatorByType<SourceDescriptorLogicalOperator>())
    {
        PRECONDITION(
            source->hasProperty(std::string(Distributed::Placement::PINNED_NODE)),
            "Sources should have been placed during source expansion");
    }
}
