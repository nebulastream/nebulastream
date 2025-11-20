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
#include <cstdint>
#include <ostream>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp> ///NOLINT(misc-include-cleaner)
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{

/// Implements a directed acyclic graph (DAG).
/// For each node, stores upstream and downstream nodes, enabling traversal in both directions.
/// Provides utility methods, such as retrieving paths between two nodes, or finding the LCA.
class NetworkTopology
{
    struct Node
    {
        using Id = WorkerId;

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

    [[nodiscard]] std::vector<NodeId> getUpstreamNodesOf(const NodeId& node) const;
    [[nodiscard]] std::vector<NodeId> getDownstreamNodesOf(const NodeId& node) const;

    /// Upstream --> from the DAG sink to the DAG source
    /// Downstream --> from the DAG source to the DAG sink
    enum Direction : uint8_t
    {
        Upstream,
        Downstream
    };

    [[nodiscard]] static constexpr Direction reverse(Direction direction) { return direction == Upstream ? Downstream : Upstream; }

    /// Return the set of all possible paths from source to dest in the given direction
    [[nodiscard]] std::vector<Path> findPaths(const NodeId& src, const NodeId& dest, Direction direction) const;

    [[nodiscard]] size_t size() const { return dag.size(); }

    [[nodiscard]] auto view() const { return dag; }

    [[nodiscard]] auto begin() const { return dag.cbegin(); }

    [[nodiscard]] auto end() const { return dag.cend(); }

    [[nodiscard]] bool contains(const NodeId& id) const { return dag.contains(id); }
};

}
