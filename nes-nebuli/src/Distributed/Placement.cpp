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

#include "Placement.hpp"

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
#include <Util/Ranges.hpp>
#include <uuid/uuid.h>
#include "ErrorHandling.hpp"
#include "Sources/SourceDescriptor.hpp"


void NES::Distributed::BottomUpPlacement::doPlacement(const Topology& topology, QueryPlan& queryPlan) const
{
    auto capacity = this->capacity;
    std::function getNode = [](const std::shared_ptr<Operator>& op) -> Topology::Node
    { return std::any_cast<Topology::Node>(op->getProperty(std::string(PINNED_NODE))); };
    std::function firstNodeWithCapacity = [&](Topology::Node start, Topology::Node nextPlacedOperatorNode)
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

        INVARIANT(currentOperator->hasProperty(std::string(PINNED_NODE)) || currentOperator != previousOperator, "Cycle");
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
            | std::views::transform([&](const auto& op) { return getNode(Util::as<Operator>(op)); }) | ranges::to<std::unordered_set>();

        if (childrenPlacement.size() == 1)
        {
            auto node = *childrenPlacement.begin();
            auto targetNode = firstNodeWithCapacity(node, getNode(findNextPlacedOperator(currentOperator)));
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
        for (auto node : path.path | std::views::chunk(2))
        {
            dag.try_emplace(node[1], queryPlan.getQueryId(), node[1], node[0]);
        }
        dag.at(path.path.back()).plan->addRootOperator(copiedChild);
        copiedChild->addProperty(std::string("SUCCESSOR_OPERATOR"), parent->getId());
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
    std::unordered_map<OperatorId, std::string> channels;
    std::unordered_map<OperatorId, OriginId> originIds;
    std::unordered_map<OperatorId, std::shared_ptr<Schema>> schemas;

    for (auto node : dag)
    {
        if (node.plan->getOperatorByType<SinkLogicalOperator>().empty())
        {
            for (auto rootOperator : node.plan->getRootOperators())
            {
                auto networkSink = std::make_shared<SinkLogicalOperator>(getNextOperatorId());
                auto successorOperatorId = std::any_cast<OperatorId>(rootOperator->getProperty(std::string("SUCCESSOR_OPERATOR")));

                networkSink->sinkName = fmt::format("NS:{}", successorOperatorId);
                channels[successorOperatorId] = generateUUID();

                networkSink->sinkDescriptor = std::make_shared<Sinks::SinkDescriptor>(
                    "Network",
                    Configurations::DescriptorConfig::Config{
                        {"type", "Network"},
                        {"channel", std::string(std::begin(channels[successorOperatorId]), std::end(channels[successorOperatorId]))},
                        {"connection", physicalTopology.at(node.successorNode).connection}},
                    false);
                networkSink->setInputOriginIds(rootOperator->getOutputOriginIds());
                networkSink->setInputSchema(rootOperator->getOutputSchema());
                networkSink->setOutputSchema(rootOperator->getOutputSchema());
                INVARIANT(networkSink->getInputOriginIds().size() == 1, "NetworkSink should only have one output originId");
                originIds.try_emplace(successorOperatorId, networkSink->getInputOriginIds()[0]);
                schemas.try_emplace(successorOperatorId, networkSink->getOutputSchema());

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
                auto descriptor = std::make_shared<Sources::SourceDescriptor>(
                    leafOperator->getOutputSchema(),
                    fmt::format("NetworkSource-{}-{}", node.plan->getQueryId(), leafOperator->getId()),
                    fmt::format("NetworkSource-{}-{}", node.plan->getQueryId(), leafOperator->getId()),
                    "Network",
                    Sources::ParserConfig{.parserType = "Raw", .tupleDelimiter = "", .fieldDelimiter = ""},
                    Configurations::DescriptorConfig::Config{
                        {"type", "Network"},
                        {"channel", std::string(std::begin(channels[leafOperator->getId()]), std::end(channels[leafOperator->getId()]))},
                    });

                auto networkSource = std::make_shared<SourceDescriptorLogicalOperator>(std::move(descriptor), getNextOperatorId());
                networkSource->setOriginId(originIds.at(leafOperator->getId()));
                networkSource->setInputSchema(schemas.at(leafOperator->getId()));
                networkSource->setOutputSchema(schemas.at(leafOperator->getId()));
                networkSource->addParent(leafOperator);
            }
        }

        plans.emplace_back(std::make_shared<DecomposedQueryPlan>(
            node.plan->getQueryId(), physicalTopology[node.nodeId].grpc, node.plan->getRootOperators()));
    }
    return plans;
}
void NES::Distributed::pinSourcesAndSinks(
    QueryPlan& queryPlan, Catalogs::Source::SourceCatalog& catalog, std::unordered_map<std::string, Topology::Node> sinks)
{
    for (const auto& sink : queryPlan.getSinkOperators())
    {
        auto sinkPlacement = sinks.at(sink->sinkName);
        sink->addProperty(std::string(Distributed::Placement::PINNED_NODE), sinkPlacement);
    }

    for (auto& source : queryPlan.getOperatorByType<SourceDescriptorLogicalOperator>())
    {
        auto entriesForLogicalSource = catalog.getPhysicalSources(source->getSourceDescriptorRef().logicalSourceName);
        auto it = std::ranges::find(
            entriesForLogicalSource,
            source->getSourceDescriptorRef().physicalSourceName,
            [](const std::shared_ptr<Catalogs::Source::SourceCatalogEntry>& entry) { return entry->getPhysicalSource()->getName(); });

        INVARIANT(
            it != entriesForLogicalSource.end(),
            "Physical source {} is not in the catalog",
            source->getSourceDescriptorRef().physicalSourceName);

        source->addProperty(std::string(Distributed::BottomUpPlacement::PINNED_NODE), (*it)->getTopologyNodeId());
    }
}
