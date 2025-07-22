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

class WorkerCatalog
{
    std::unordered_map<CLI::HostAddr, CLI::WorkerConfig> workers;

    explicit WorkerCatalog(const std::vector<CLI::WorkerConfig>& nodeConfigs)
    {
        workers = std::views::transform(nodeConfigs, [](const auto& workerConfig) { return std::make_pair(workerConfig.host, workerConfig); })
            | std::ranges::to<std::unordered_map<CLI::HostAddr, CLI::WorkerConfig>>();
    }

public:
    static WorkerCatalog from(const std::vector<CLI::WorkerConfig>& workerConfigs) { return WorkerCatalog{workerConfigs}; }

    [[nodiscard]] CLI::GrpcAddr getGrpcAddressFor(const CLI::HostAddr& hostAddr) const { return workers.at(hostAddr).grpc; }

    [[nodiscard]] size_t getCapacityFor(const CLI::HostAddr& hostAddr) const { return workers.at(hostAddr).capacity; }

    [[nodiscard]] bool hasCapacity(const CLI::HostAddr& hostAddr) const { return getCapacityFor(hostAddr) > 0; }

    void consumeCapacity(const CLI::HostAddr& hostAddr, const size_t reduction) { workers.at(hostAddr).capacity -= reduction; }
    void setCapacityForAll(const size_t capacity)
    {
        for (auto& config : workers | std::views::values)
        {
            config.capacity = capacity;
        }
    }

    [[nodiscard]] size_t size() const { return workers.size(); }

    auto values() const { return std::views::values(workers); }
    auto begin() const { return workers.cbegin(); }
    auto end() const { return workers.cend(); }
};
}
