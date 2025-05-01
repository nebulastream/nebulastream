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

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <Identifiers/NESStrongType.hpp>

namespace NES::Distributed
{
class Topology
{
    struct Node_
    {
        using Id = NES::NESStrongType<uint64_t, struct NodeId_, 0, 1>;
        Id id = INVALID<Id>;
        static std::atomic<Id::Underlying> counter;

        std::vector<Id> upstreams;
        std::vector<Id> downstreams;
    };
    std::unordered_map<Node_::Id, Node_> nodes;

public:
    using Node = Node_::Id;

    struct Path
    {
        std::vector<Node> path;
        auto operator<=>(const Path& other) const = default;
        friend std::ostream& operator<<(std::ostream& os, const Path& path);
    };

    bool empty() const { return nodes.empty(); }
    size_t size() const { return nodes.size(); }
    bool contains(Node id) const { return nodes.contains(id); }

    /// Modify graph
    Node addNode();
    void addDownstreams(Node target, std::same_as<Node> auto... newNodes);
    void addUpstreams(Node target, std::same_as<Node> auto... newNodes);
    void removeNode(Node target);


    enum Direction
    {
        Upstream,
        Downstream
    };

    const std::vector<Node>& getUpstreams(Node node) const { return nodes.at(node).upstreams; }
    const std::vector<Node>& getDownstreams(Node node) const { return nodes.at(node).downstreams; }

    std::vector<Path> findPaths(Node source, Node dest, Direction direction) const;
    std::vector<std::tuple<Node, Path, Path>> findCommonNode(Node source1, Node source2, Node destination, Direction direction) const;
};

void Topology::addDownstreams(Node target, std::same_as<NESStrongType<unsigned long, NodeId_, 0, 1>> auto... newNodes)
{
    (nodes[target].downstreams.emplace_back(newNodes), ...);
    (nodes[newNodes].upstreams.emplace_back(target), ...);
}

void Topology::addUpstreams(Node target, std::same_as<NESStrongType<unsigned long, NodeId_, 0, 1>> auto... newNodes)
{
    (nodes[target].upstreams.emplace_back(newNodes), ...);
    (nodes[newNodes].downstreams.emplace_back(target), ...);
}
}
