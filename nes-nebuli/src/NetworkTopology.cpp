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

#include <functional>
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

std::ostream& operator<<(std::ostream& os, const NetworkTopology::Path& path)
{
    return os << fmt::format("Path [{}]", fmt::join(path.path, " -> "));
}

void NetworkTopology::addNode(const NodeId& id, const std::vector<NodeId>& downstreamNodes)
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

    /// Set upstream nodes of added node, by finding all nodes that have the new node as a downstream node
    for (auto& [nodeId, node] : dag)
    {
        for (const auto& downstreamNode : node.downstreamNodes)
        {
            if (downstreamNode == id)
            {
                dag[id].upstreamNodes.emplace_back(nodeId);
            }
        }
    }

    /// Cycle detection: DFS from the new node following downstream edges. If we reach the new node again, there is a cycle.
    std::unordered_set<NodeId> visited;
    std::vector<NodeId> stack(dag.at(id).downstreamNodes.begin(), dag.at(id).downstreamNodes.end());
    while (not stack.empty())
    {
        auto current = stack.back();
        stack.pop_back();
        if (current == id)
        {
            /// Undo: remove this node from downstream nodes' upstream lists and erase the node
            for (const auto& dn : downstreamNodes)
            {
                if (dag.contains(dn))
                {
                    std::erase(dag[dn].upstreamNodes, id);
                }
            }
            dag.erase(id);
            throw InvalidTopology("Adding node [{}] would create a cycle in the topology", id);
        }
        if (visited.insert(current).second && dag.contains(current))
        {
            const auto& dns = dag.at(current).downstreamNodes;
            stack.insert(stack.end(), dns.begin(), dns.end());
        }
    }
}

void NetworkTopology::removeNode(const NodeId& id)
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

std::vector<NetworkTopology::NodeId> NetworkTopology::getUpstreamNodesOf(const NodeId& node) const
{
    if (dag.contains(node))
    {
        return dag.at(node).upstreamNodes;
    }
    return {};
}

std::vector<NetworkTopology::NodeId> NetworkTopology::getDownstreamNodesOf(const NodeId& node) const
{
    if (dag.contains(node))
    {
        return dag.at(node).downstreamNodes;
    }
    return {};
}

std::vector<NetworkTopology::Path> NetworkTopology::findPaths(const NodeId& src, const NodeId& dest, const Direction direction) const
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

}
