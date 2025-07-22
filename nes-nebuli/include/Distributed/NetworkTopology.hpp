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

#include <ErrorHandling.hpp>
#include <QueryConfig.hpp>

namespace NES
{

/// Implements a directed acyclic graph (DAG).
/// For each node, stores upstream and downstream nodes, enabling traversal in both directions.
/// Provides utility methods, such as retrieving paths between two nodes, or finding the LCA.
/// Implemented as a read-only structure that is created once and cannot be changed afterwards.
class TopologyGraph
{
    struct Node
    {
        using Id = CLI::HostAddr;

        std::vector<Id> upstreamNodes;
        std::vector<Id> downstreamNodes;
    };
    std::unordered_map<Node::Id, Node> nodes{};

public:
    using NodeId = Node::Id;

    static TopologyGraph from(const std::vector<std::pair<NodeId, std::vector<NodeId>>>& dag);

    /// Represents a path of directly connected nodes in the topology
    struct Path
    {
        std::vector<NodeId> path;

        auto operator<=>(const Path& other) const = default;
        friend std::ostream& operator<<(std::ostream& os, const Path& path);
        [[nodiscard]] auto begin() const { return path.begin(); }
        [[nodiscard]] auto end() const { return path.end(); }
    };

    std::vector<NodeId> getUpstreamNodesOf(const NodeId& node) const;
    std::vector<NodeId> getDownstreamNodesOf(const NodeId& node) const;

    std::vector<NodeId> getSourceNodes() const
    {
        return nodes | std::views::keys | std::views::filter([this](const auto& id) { return getInDegreeOf(id) == 0; })
            | std::ranges::to<std::vector>();
    }
    std::vector<NodeId> getSinkNodes() const
    {
        return nodes | std::views::keys | std::views::filter([this](const auto& id) { return getOutDegreeOf(id) == 0; })
            | std::ranges::to<std::vector>();
    }
    /// Upstream --> from the DAG sink to the DAG source
    /// Downstream --> from the DAG source to the DAG sink
    enum Direction
    {
        Upstream,
        Downstream
    };

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

    size_t size() const { return nodes.size(); }
    auto view() const { return nodes; }
    auto begin() const { return nodes.cbegin(); }
    auto end() const { return nodes.cend(); }

private:
    TopologyGraph() = default;

    void addNode(const Node::Id& id);
    void addDownstreamNode(const NodeId& node, const NodeId& downstreamNode);

    size_t getInDegreeOf(const NodeId& node) const
    {
        PRECONDITION(nodes.contains(node), "Node {} does not exist in topology", node);
        return nodes.at(node).upstreamNodes.size();
    }
    size_t getOutDegreeOf(const NodeId& node) const
    {
        PRECONDITION(nodes.contains(node), "Node {} does not exist in topology", node);
        return nodes.at(node).downstreamNodes.size();
    }
};

}
