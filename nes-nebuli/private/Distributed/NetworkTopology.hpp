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

#include <optional>
#include <ostream>
#include <ranges>
#include <unordered_map>
#include <vector>

#include <ErrorHandling.hpp>
#include <QueryConfig.hpp>

namespace NES
{

class TopologyGraph
{
    struct Node
    {
        using Id = CLI::HostAddr;

        std::vector<Id> upstreamNodes;
        std::vector<Id> downstreamNodes;
    };
    std::unordered_map<Node::Id, Node> nodes;

public:
    using NodeId = Node::Id;

    static TopologyGraph from(const std::vector<CLI::NodeConfig>& nodeConfigs)
    {
        const auto dag
            = std::views::transform(
                  nodeConfigs, [](const auto& workerNode) { return std::make_pair(workerNode.host, workerNode.downstreamNodes); })
            | std::ranges::to<std::vector>();

        TopologyGraph graph;
        for (const auto& node : dag | std::views::keys)
        {
            graph.addNode(node);
        }
        for (const auto& [node, downstreamNodes] : dag)
        {
            for (const auto& downstreamNode : downstreamNodes)
            {
                graph.addDownstreamNode(node, downstreamNode);
            }
        }
        return graph;
    }

    /// Represents a path of directly connected nodes in the topology
    struct Path
    {
        std::vector<NodeId> path;
        auto operator<=>(const Path& other) const = default;
        friend std::ostream& operator<<(std::ostream& os, const Path& path);

        auto begin() const { return path.begin(); }
        auto end() const { return path.end(); }
    };

    /// Access information about the topology
    bool empty() const { return nodes.empty(); }
    size_t size() const { return nodes.size(); }
    bool contains(const NodeId& id) const { return nodes.contains(id); }

    const std::vector<NodeId>& getUpstreamNodesOf(const NodeId& node) const { return nodes.at(node).upstreamNodes; }
    const std::vector<NodeId>& getDownstreamNodesOf(const NodeId& node) const { return nodes.at(node).downstreamNodes; }

    enum Direction
    {
        Upstream,
        Downstream
    };
    // Return the set of all possible paths from source to dest in the given direction
    std::vector<Path> findPaths(const NodeId& src, const NodeId& dest, Direction direction) const;
    std::vector<std::tuple<std::optional<NodeId>, Path, Path>>
    findLCA(const NodeId& src1, const NodeId& src2, const NodeId& dest, Direction direction) const;

    auto view() const { return nodes; }
    auto begin() const { return nodes.cbegin(); }
    auto end() const { return nodes.cend(); }

private:
    TopologyGraph() = default;

    void addNode(const Node::Id& id);
    void addDownstreamNode(const NodeId& node, const NodeId& downstream);
};

inline void TopologyGraph::addDownstreamNode(const NodeId& node, const NodeId& downstream)
{
    if (not contains(node))
    {
        throw InvalidTopology("Tried to insert edge {} -> {}, but {} does not exist in the topology", node, downstream, node);
    }
    nodes[node].downstreamNodes.emplace_back(downstream);
    nodes[downstream].upstreamNodes.emplace_back(node);
}

}
