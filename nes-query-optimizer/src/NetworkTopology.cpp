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
#include <unordered_set>
#include <vector>

#include <Util/PlanRenderer.hpp>
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

    /// DAG invariant: white/gray/black DFS coloring detects back edges (= cycles) in O(V+E).
    enum class Color : uint8_t
    {
        White,
        Gray,
        Black
    };
    std::unordered_map<NodeId, Color> color;
    color.reserve(topology.dag.size());
    for (const auto& [id, _] : topology.dag)
    {
        color.emplace(id, Color::White);
    }
    std::function<void(const NodeId&)> visit = [&](const NodeId& node) -> void
    {
        color[node] = Color::Gray;
        for (const auto& downstream : topology.dag.at(node).downstreamNodes)
        {
            if (not topology.dag.contains(downstream))
            {
                continue;
            }
            if (color.at(downstream) == Color::Gray)
            {
                throw InvalidTopology("cycle detected involving node [{}] -> [{}]", node, downstream);
            }
            if (color.at(downstream) == Color::White)
            {
                visit(downstream);
            }
        }
        color[node] = Color::Black;
    };
    for (const auto& [id, _] : topology.dag)
    {
        if (color.at(id) == Color::White)
        {
            visit(id);
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

struct TopologyNodeWrapper
{
    /// Non-owning view into NetworkTopology for PlanRenderer.
    /// Uses pointer instead of reference to allow copy assignment (required by std::deque in PlanRenderer).
    /// Pointer is safe because TopologyNodeWrapper only exists during renderTopology()
    /// where the NetworkTopology is guaranteed to outlive the wrapper.
    const NetworkTopology* graph;
    NetworkTopology::NodeId id;
};

template <>
struct GetRootOperator<NetworkTopology>
{
    auto operator()(const NetworkTopology& op) const
    {
        return std::views::filter(op, [&op](const auto& node) { return op.getDownstreamNodesOf(node.first).empty(); })
            | std::views::transform([&op](const auto& node) { return TopologyNodeWrapper{&op, node.first}; })
            | std::ranges::to<std::vector>();
    }
};

template <>
struct Explain<TopologyNodeWrapper>
{
    auto operator()(const TopologyNodeWrapper& op, const ExplainVerbosity) const { return op.id.getRawValue(); }
};

template <>
struct GetId<TopologyNodeWrapper>
{
    auto operator()(const TopologyNodeWrapper& op) const { return op.id.getRawValue(); }
};

template <>
struct GetChildren<TopologyNodeWrapper>
{
    auto operator()(const TopologyNodeWrapper& op) const
    {
        return op.graph->getUpstreamNodesOf(op.id)
            | std::views::transform([&op](const auto& child) { return TopologyNodeWrapper{op.graph, child}; })
            | std::ranges::to<std::vector>();
    }
};

void renderTopology(const NetworkTopology& graph, std::ostream& os)
{
    PlanRenderer<NetworkTopology, TopologyNodeWrapper>(os, ExplainVerbosity::Short).dump(graph);
}
}
