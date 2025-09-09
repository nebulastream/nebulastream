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

#include <NetworkTopology.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <optional>
#include <ostream>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ErrorHandling.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const Topology::Path& path)
{
    return os << fmt::format("Path [{}]", fmt::join(path.path, " -> "));
}

void Topology::addNode(const NodeId& id, const std::vector<NodeId>& downstreamNodes)
{
    /// Check for self-reference
    for (const auto& downstreamNode : downstreamNodes)
    {
        if (downstreamNode == id)
        {
            throw InvalidTopology("Cannot have a self-referential topology: [{}] was given as a downstream node of itself", id);
        }
    }

    /// Insert node with downstream connections
    dag.emplace(id, Node{.upstreamNodes = {}, .downstreamNodes = downstreamNodes});

    /// Update existing downstream nodes to include this node as upstream
    for (const auto& downstreamNode : downstreamNodes)
    {
        if (dag.contains(downstreamNode))
        {
            dag[downstreamNode].upstreamNodes.emplace_back(id);
        }
    }

    /// Check if any existing nodes have this new node as downstream and update upstream links
    for (auto& [nodeId, node] : dag)
    {
        if (nodeId != id) /// Skip current node
        {
            for (const auto& downstreamNode : node.downstreamNodes)
            {
                if (downstreamNode == id)
                {
                    dag[id].upstreamNodes.emplace_back(nodeId);
                }
            }
        }
    }
}

void Topology::removeNode(const NodeId& id)
{
    PRECONDITION(dag.contains(id), "Can not remove node [{}] that is not present in the DAG", id);

    const auto& [upstreamNodes, downstreamNodes] = dag[id];

    /// Remove this node from upstream nodes' downstream lists
    for (const auto& upstream : upstreamNodes)
    {
        if (dag.contains(upstream))
        {
            std::erase_if(dag[upstream].downstreamNodes, [&id](const auto& node) { return node == id; });
        }
    }

    /// Remove this node from downstream nodes' upstream lists
    for (const auto& downstream : downstreamNodes)
    {
        if (dag.contains(downstream))
        {
            std::erase_if(dag[downstream].upstreamNodes, [&id](const auto& node) { return node == id; });
        }
    }

    dag.erase(id);
}

std::vector<Topology::NodeId> Topology::getUpstreamNodesOf(const NodeId& node) const
{
    if (dag.contains(node))
    {
        return dag.at(node).upstreamNodes;
    }
    return {};
}

std::vector<Topology::NodeId> Topology::getDownstreamNodesOf(const NodeId& node) const
{
    if (dag.contains(node))
    {
        return dag.at(node).downstreamNodes;
    }
    return {};
}

std::vector<Topology::Path> Topology::findPaths(const NodeId& src, const NodeId& dest, const Direction direction) const
{
    PRECONDITION(dag.contains(src) && dag.contains(dest), "Both source [{}] and dest [{}] must be part of the topology", src, dest);

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

std::vector<Topology::LowestCommonAncestor>
Topology::findLCA(const NodeId& src1, const NodeId& src2, const NodeId& dest, const Direction direction) const
{
    const auto paths1 = findPaths(dest, src1, reverse(direction));
    const auto paths2 = findPaths(dest, src2, reverse(direction));

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

bool Topology::isValidDAG() const
{
    /// For a finite graph, if it's acyclic it must have at least one source and one sink
    /// Check for cycles using DFS with state-based cycle detection
    enum class State
    {
        Unvisited,
        Visiting,
        Visited
    };
    std::unordered_map<NodeId, State> nodeStates;

    /// Initialize all nodes as unvisited
    for (const auto& nodeId : dag | std::views::keys)
    {
        nodeStates[nodeId] = State::Unvisited;
    }

    std::function<bool(const NodeId&)> hasCycleDFS = [&](const NodeId& node) -> bool
    {
        nodeStates[node] = State::Visiting;

        for (const auto& downstream : dag.at(node).downstreamNodes)
        {
            if (dag.contains(downstream))
            {
                if (nodeStates[downstream] == State::Visiting) /// Back edge found - cycle detected
                {
                    return true;
                }
                if (nodeStates[downstream] == State::Unvisited && hasCycleDFS(downstream))
                {
                    return true;
                }
            }
        }

        nodeStates[node] = State::Visited;
        return false;
    };

    /// Check for cycles starting from each unvisited node
    for (const auto& nodeId : dag | std::views::keys)
    {
        if (nodeStates[nodeId] == State::Unvisited && hasCycleDFS(nodeId))
        {
            return false;
        }
    }

    return true;
}
}
