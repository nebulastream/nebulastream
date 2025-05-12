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

std::ostream& operator<<(std::ostream& os, const TopologyGraph::Path& path)
{
    return os << fmt::format("Path [{}]", fmt::join(path.path, " -> "));
}

void TopologyGraph::addNode(const NodeId& id)
{
    if (not nodes.try_emplace(id).second)
    {
        throw InvalidTopology();
    }
}

std::vector<TopologyGraph::Path> TopologyGraph::findPaths(const NodeId& src, const NodeId& dest, const Direction direction) const
{
    PRECONDITION(contains(src) && contains(dest), "Both source and dest must be part of the topology");

    std::vector<Path> result;
    std::unordered_set<NodeId> visited;
    auto getNeighbors = [&](const NodeId& node) -> const std::vector<NodeId>&
    {
        if (direction == Upstream)
        {
            return nodes.at(node).upstreamNodes;
        }
        return nodes.at(node).downstreamNodes;
    };

    std::function<void(NodeId, Path&)> dfs = [&](const NodeId& current, Path& currentPath)
    {
        if (current == dest)
        {
            result.push_back(currentPath);
            return;
        }

        visited.insert(current);

        for (const auto& children : getNeighbors(current))
        {
            if (!visited.contains(children))
            {
                currentPath.path.push_back(children);
                dfs(children, currentPath);
                currentPath.path.pop_back();
            }
        }

        visited.erase(current);
    };

    Path initialPath;
    initialPath.path.emplace_back(src);
    dfs(src, initialPath);
    return result;
}

std::vector<std::tuple<std::optional<TopologyGraph::NodeId>, TopologyGraph::Path, TopologyGraph::Path>>
TopologyGraph::findLCA(const NodeId& src1, const NodeId& src2, const NodeId& dest, const Direction direction) const
{
    /// The problem of finding the common node reduces to finding the lowest common ancestor (LCA) of the dest
    auto paths1 = findPaths(dest, src1, direction == Downstream ? Upstream : Downstream);
    auto paths2 = findPaths(dest, src2, direction == Downstream ? Upstream : Downstream);

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

    std::vector<std::tuple<std::optional<NodeId>, Path, Path>> result;
    size_t longestSuffix = 0;
    /// brute force
    for (const auto& path1 : paths1)
    {
        for (const auto& path2 : paths2)
        {
            if (auto [suffix, node] = commonSuffix(path1, path2); suffix >= longestSuffix)
            {
                if (suffix > longestSuffix)
                {
                    result.clear();
                    longestSuffix = suffix;
                }
                result.emplace_back(node, path1, path2);
            }
        }
    }

    if (longestSuffix == 0)
    {
        return {};
    }

    for (auto& [_, path1, path2] : result)
    {
        std::ranges::reverse(path1.path);
        std::ranges::reverse(path2.path);
    }

    return result;
}
}
