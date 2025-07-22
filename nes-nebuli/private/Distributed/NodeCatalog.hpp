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

#include <cstddef>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>

#include <QueryConfig.hpp>

namespace NES
{

class NodeCatalog
{
    std::unordered_map<CLI::HostAddr, CLI::NodeConfig> nodes;

    explicit NodeCatalog(const std::vector<CLI::NodeConfig>& nodeConfigs)
    {
        nodes = std::views::transform(nodeConfigs, [](const auto& nodeConfig) { return std::make_pair(nodeConfig.host, nodeConfig); })
            | std::ranges::to<std::unordered_map<CLI::HostAddr, CLI::NodeConfig>>();
    }

public:
    static NodeCatalog from(const std::vector<CLI::NodeConfig>& nodeConfigs) { return NodeCatalog{nodeConfigs}; }

    [[nodiscard]] CLI::GrpcAddr getGrpcAddressFor(const CLI::HostAddr& hostAddr) const { return nodes.at(hostAddr).grpc; }

    [[nodiscard]] size_t getCapacityFor(const CLI::HostAddr& hostAddr) const { return nodes.at(hostAddr).capacity; }

    [[nodiscard]] bool hasCapacity(const CLI::HostAddr& hostAddr) const { return getCapacityFor(hostAddr) > 0; }

    void consumeCapacity(const CLI::HostAddr& hostAddr, const size_t reduction) { nodes.at(hostAddr).capacity -= reduction; }

    [[nodiscard]] size_t size() const { return nodes.size(); }

    auto values() const { return std::views::values(nodes); }
    auto begin() const { return nodes.cbegin(); }
    auto end() const { return nodes.cend(); }
};
}
