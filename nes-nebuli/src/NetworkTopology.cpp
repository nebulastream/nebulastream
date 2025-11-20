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

}
