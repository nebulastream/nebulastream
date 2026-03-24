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
#include <unordered_set>
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

NetworkTopology NetworkTopology::fromEdges(
    const std::vector<NodeId>& nodes,
    const std::vector<std::pair<NodeId, NodeId>>& edges)
{
    NetworkTopology topology;
    for (const auto& node : nodes)
    {
        topology.dag.emplace(node, Node{.upstreamNodes = {}, .downstreamNodes = {}});
    }
    for (const auto& [upstream, downstream] : edges)
    {
        if (topology.dag.contains(upstream))
        {
            topology.dag[upstream].downstreamNodes.emplace_back(downstream);
        }
        if (topology.dag.contains(downstream))
        {
            topology.dag[downstream].upstreamNodes.emplace_back(upstream);
        }
    }
    return topology;
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
