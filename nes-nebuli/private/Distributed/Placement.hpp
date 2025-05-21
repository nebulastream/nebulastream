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

#include <Distributed/Topology.hpp>

#include <memory>
#include <ranges>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>


namespace NES::Distributed
{
class Placement
{
public:
    constexpr static std::string_view PINNED_NODE = "PINNED_NODE";
    virtual ~Placement() = default;

    virtual void doPlacement(const Topology&, QueryPlan& queryPlan) const = 0;
};

class BottomUpPlacement : public Placement
{
public:
    std::unordered_map<Topology::Node, size_t> capacity;
    void doPlacement(const Topology&, QueryPlan& queryPlan) const override;
};

struct DecomposedPlanDAG
{
    struct Node
    {
        Node(QueryId queryId, Topology::Node nodeId, Topology::Node successorNode)
            : plan(std::make_shared<DecomposedQueryPlan>(queryId, "")), successorNode(successorNode), nodeId(nodeId)
        {
        }
        std::shared_ptr<DecomposedQueryPlan> plan;
        Topology::Node successorNode;
        Topology::Node nodeId;
    };

    std::map<Topology::Node, Node> dag; /// sorted on key

    auto view() const { return std::views::values(dag); }
    auto begin() const { return view().begin(); }
    auto end() const { return view().end(); }
};

DecomposedPlanDAG decompose(const Topology& topology, const QueryPlan& queryPlan);

struct PhysicalNodeConfig
{
    std::string connection;
    std::string grpc;
};

std::vector<std::shared_ptr<DecomposedQueryPlan>>
connect(DecomposedPlanDAG dag, std::unordered_map<Topology::Node, PhysicalNodeConfig> physicalTopology);

void pinSourcesAndSinks(
    QueryPlan& queryPlan, Catalogs::Source::SourceCatalog& catalog, std::unordered_map<std::string, Topology::Node> sinks);

}
