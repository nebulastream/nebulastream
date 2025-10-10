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

#include <cstddef>
#include <ostream>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

/// Implements a directed acyclic graph (DAG).
/// For each node, stores upstream and downstream nodes, enabling traversal in both directions.
/// Provides utility methods, such as retrieving paths between two nodes, or finding the LCA.
class Topology
{
    struct Node
    {
        using Id = HostAddr;

        std::vector<Id> upstreamNodes;
        std::vector<Id> downstreamNodes;

        friend std::ostream& operator<<(std::ostream& os, const Node& node)
        {
            return os << fmt::format(
                       "Edges(downstream: [{}], upstream: [{}])",
                       fmt::join(node.downstreamNodes, ", "),
                       fmt::join(node.upstreamNodes, ", "));
        }
    };

    std::unordered_map<Node::Id, Node> dag;

public:
    using NodeId = Node::Id;

    /// Represents a path of directly connected nodes in the topology
    struct Path
    {
        std::vector<NodeId> path;

        auto operator<=>(const Path& other) const = default;
        friend std::ostream& operator<<(std::ostream& os, const Path& path);

        [[nodiscard]] auto begin() const { return path.begin(); }

        [[nodiscard]] auto end() const { return path.end(); }
    };

    void addNode(const Node::Id& id, const std::vector<Node::Id>& downstreamNodes);
    void removeNode(const Node::Id& id);

    std::vector<NodeId> getUpstreamNodesOf(const NodeId& node) const;
    std::vector<NodeId> getDownstreamNodesOf(const NodeId& node) const;

    std::vector<NodeId> getSourceNodes() const
    {
        return dag | std::views::keys | std::views::filter([this](const auto& id) { return getInDegreeOf(id) == 0; })
            | std::ranges::to<std::vector>();
    }

    std::vector<NodeId> getSinkNodes() const
    {
        return dag | std::views::keys | std::views::filter([this](const auto& id) { return getOutDegreeOf(id) == 0; })
            | std::ranges::to<std::vector>();
    }

    /// Upstream --> from the DAG sink to the DAG source
    /// Downstream --> from the DAG source to the DAG sink
    enum Direction
    {
        Upstream,
        Downstream
    };

    [[nodiscard]] static Direction reverse(Direction direction) { return direction == Upstream ? Downstream : Upstream; }

    /// Return the set of all possible paths from source to dest in the given direction
    std::vector<Path> findPaths(const NodeId& src, const NodeId& dest, Direction direction) const;

    struct LowestCommonAncestor
    {
        NodeId lca;
        std::pair<Path, Path> paths;

        bool operator==(const LowestCommonAncestor& other) const { return lca == other.lca && paths == other.paths; }
    };

    /// Return the two paths to the lowest/first common ancestor (LCA) to a dest node
    /// Returns a set of results (there may be multiple LCAs with the same distance)
    /// Each result is a tuple containing the optional LCA, the path src2 --> dest, and src2 --> dest
    std::vector<LowestCommonAncestor> findLCA(const NodeId& src1, const NodeId& src2, const NodeId& dest, Direction direction) const;

    size_t size() const { return dag.size(); }

    auto view() const { return dag; }

    auto begin() const { return dag.cbegin(); }

    auto end() const { return dag.cend(); }

    /// Validates if topology forms a valid DAG with at least one source, one sink, and no cycles
    bool isValidDAG() const;

private:
    size_t getInDegreeOf(const NodeId& node) const
    {
        PRECONDITION(dag.contains(node), "Node {} does not exist in topology", node);
        return dag.at(node).upstreamNodes.size();
    }

    size_t getOutDegreeOf(const NodeId& node) const
    {
        PRECONDITION(dag.contains(node), "Node {} does not exist in topology", node);
        return dag.at(node).downstreamNodes.size();
    }
};

void renderTopology(const Topology& graph, std::ostream& os);
}
