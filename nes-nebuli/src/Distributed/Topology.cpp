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

#include <Distributed/Topology.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <ostream>
#include <unordered_set>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>


std::atomic<uint64_t> NES::Distributed::Topology::Node_::counter = Id::INITIAL;
std::ostream& NES::Distributed::operator<<(std::ostream& os, const Topology::Path& path)
{
    return os << fmt::format("Path [{}]", fmt::join(path.path, " -> "));
}
NES::Distributed::Topology::Node_::Id NES::Distributed::Topology::addNode()
{
    Node_::Id id(Node_::counter++);
    INVARIANT(nodes.try_emplace(id).second, "Node Id should be unique");
    return id;
}
void NES::Distributed::Topology::removeNode(Node_::Id target)
{
    if (auto it = nodes.find(target); it != nodes.end())
    {
        std::ranges::for_each(it->second.downstreams, [&](auto node) { std::erase(nodes.at(node).upstreams, target); });
        std::ranges::for_each(it->second.upstreams, [&](auto node) { std::erase(nodes.at(node).downstreams, target); });
        nodes.erase(it);
    }
}
std::vector<NES::Distributed::Topology::Path>
NES::Distributed::Topology::findPaths(Node_::Id source, Node_::Id dest, Direction direction) const
{
    PRECONDITION(contains(source) && contains(dest), "Both source and dest must be part of the topology");

    std::vector<Path> result;
    std::unordered_set<Node_::Id> visited;
    auto getChildren = [&](Node_::Id node) -> const std::vector<Node_::Id>&
    {
        if (direction == Upstream)
        {
            return nodes.at(node).upstreams;
        }
        return nodes.at(node).downstreams;
    };

    std::function<void(Node_::Id, Path&)> dfs = [&](Node_::Id current, Path& currentPath)
    {
        if (current == dest)
        {
            result.push_back(currentPath);
            return;
        }

        visited.insert(current);

        for (const auto& children : getChildren(current))
        {
            if (visited.find(children) == visited.end())
            {
                currentPath.path.push_back(children);
                dfs(children, currentPath);
                currentPath.path.pop_back();
            }
        }

        visited.erase(current);
    };

    Path initialPath;
    initialPath.path.emplace_back(source);
    dfs(source, initialPath);
    return result;
}
std::vector<std::tuple<NES::Distributed::Topology::Node_::Id, NES::Distributed::Topology::Path, NES::Distributed::Topology::Path>>
NES::Distributed::Topology::findCommonNode(Node_::Id source1, Node_::Id source2, Node_::Id destination, Direction direction) const
{
    /// The problem of finding the common node reduces to finding the longest common suffix between a path from source1 to destination and source2 to destination
    auto paths1 = findPaths(destination, source1, direction == Downstream ? Upstream : Downstream);
    auto paths2 = findPaths(destination, source2, direction == Downstream ? Upstream : Downstream);

    const auto commonSuffix = [](const Path& path1, const Path& path2) -> std::pair<size_t, Node_::Id>
    {
        auto missmatch = std::ranges::mismatch(path1.path, path2.path);
        auto distance = std::ranges::distance(path1.path.begin(), missmatch.in1);
        if (distance > 0)
        {
            return {distance, *std::prev(missmatch.in1)};
        }
        return {distance, INVALID<Node_::Id>};
    };


    std::vector<std::tuple<Node_::Id, Path, Path>> result;
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
