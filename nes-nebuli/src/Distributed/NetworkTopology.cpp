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

#include <Distributed/NetworkTopology.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iostream>
#include <iterator>
#include <optional>
#include <ostream>
#include <ranges>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ErrorHandling.hpp>

namespace NES
{


TopologyGraph TopologyGraph::from(const std::vector<std::pair<NodeId, std::vector<NodeId>>>& dag)
{
    TopologyGraph graph{};
    /// Add all the nodes before inserting downstream edges
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

std::ostream& operator<<(std::ostream& os, const TopologyGraph::Path& path)
{
    return os << fmt::format("Path [{}]", fmt::join(path.path, " -> "));
}

void TopologyGraph::addNode(const NodeId& id)
{
    if (not nodes.try_emplace(id).second)
    {
        throw InvalidTopology("Attempt to insert node [{}] into topology that was already present", id);
    }
}

void TopologyGraph::addDownstreamNode(const NodeId& node, const NodeId& downstreamNode)
{
    if (not nodes.contains(node))
    {
        throw InvalidTopology("Tried to insert edge ({}, {}), but node [{}] does not exist in the topology", node, downstreamNode, node);
    }
    nodes[node].downstreamNodes.emplace_back(downstreamNode);
    nodes[downstreamNode].upstreamNodes.emplace_back(node);
}


std::vector<TopologyGraph::NodeId> TopologyGraph::getUpstreamNodesOf(const NodeId& node) const
{
    if (nodes.contains(node))
    {
        return nodes.at(node).upstreamNodes;
    }
    return {};
}

std::vector<TopologyGraph::NodeId> TopologyGraph::getDownstreamNodesOf(const NodeId& node) const
{
    if (nodes.contains(node))
    {
        return nodes.at(node).downstreamNodes;
    }
    return {};
}

std::vector<TopologyGraph::Path> TopologyGraph::findPaths(const NodeId& src, const NodeId& dest, const Direction direction) const
{
    PRECONDITION(nodes.contains(src) && nodes.contains(dest), "Both source [{}] and dest [{}] must be part of the topology", src, dest);

    auto getNeighbors = [this, direction](const NodeId& node) -> std::vector<NodeId>
    {
        if (direction == Upstream)
        {
            return getUpstreamNodesOf(node);
        }
        return getDownstreamNodesOf(node);
    };

    std::vector<Path> paths;
    std::unordered_set<NodeId> visited;

    std::function<void(const NodeId&, Path&)> dfs = [&](const NodeId& currentNode, Path& currentPath) -> void
    {
        if (currentNode == dest)
        {
            paths.push_back(currentPath);
            return;
        }

        visited.insert(currentNode);

        for (const auto& children : getNeighbors(currentNode))
        {
            if (not visited.contains(children))
            {
                currentPath.path.push_back(children);
                dfs(children, currentPath);
                currentPath.path.pop_back();
            }
        }

        visited.erase(currentNode);
    };

    Path initialPath;
    initialPath.path.push_back(src);
    dfs(src, initialPath);
    return paths;
}

std::vector<TopologyGraph::LowestCommonAncestor>
TopologyGraph::findLCA(const NodeId& src1, const NodeId& src2, const NodeId& dest, const Direction direction) const
{
    const auto paths1 = findPaths(dest, src1, direction == Downstream ? Upstream : Downstream); // NOLINT(*-suspicious-call-argument)
    const auto paths2 = findPaths(dest, src2, direction == Downstream ? Upstream : Downstream); // NOLINT(*-suspicious-call-argument)

    const auto commonSuffix = [](const Path& path1, const Path& path2) -> std::pair<size_t, std::optional<NodeId>>
    {
        const auto [in1, in2] = std::ranges::mismatch(path1.path, path2.path);
        auto distance = std::ranges::distance(path1.path.begin(), in1);
        if (distance > 0)
        {
            return {distance, *std::prev(in1)};
        }
        return {distance, std::nullopt};
    };

    std::vector<LowestCommonAncestor> lcas;
    size_t longestSuffix = 0;
    /// Iterate over all combinations of paths and keep the ones with the longest common suffix, i.e., those that start with the LCA
    for (const auto& path1 : paths1)
    {
        for (const auto& path2 : paths2)
        {
            if (auto [suffixLength, node] = commonSuffix(path1, path2); node.has_value() && suffixLength >= longestSuffix)
            {
                if (suffixLength > longestSuffix)
                {
                    lcas.clear();
                    longestSuffix = suffixLength;
                }
                lcas.emplace_back(*node, std::make_pair(path1, path2));
            }
        }
    }

    for (auto& [_, paths] : lcas)
    {
        std::ranges::reverse(paths.first.path);
        std::ranges::reverse(paths.second.path);
    }

    return lcas;
}
}
