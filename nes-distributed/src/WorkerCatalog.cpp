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

#include <WorkerCatalog.hpp>

#include <cstddef>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <WorkerConfig.hpp>
#include <NetworkTopology.hpp>

namespace NES
{
WorkerCatalog::WorkerCatalog(const std::vector<WorkerConfig>& workerConfigs)
    : workers{
        std::views::transform(workerConfigs, [](const auto& worker) { return std::make_pair(worker.host, worker); }) | std::ranges::to<
            std::unordered_map>()
    }
    , topology{
        Topology::from(
            std::views::transform(
                workerConfigs,
                [](const auto& worker) { return std::make_pair(worker.host, worker.downstream); }) | std::ranges::to<std::vector>())}
{
}

WorkerCatalog WorkerCatalog::from(const std::vector<WorkerConfig>& workers)
{
    return WorkerCatalog{workers};
}

[[nodiscard]] std::optional<WorkerConfig> WorkerCatalog::getWorker(const HostAddr& hostAddr) const
{
    if (workers.contains(hostAddr))
    {
        return workers.at(hostAddr);
    }
    return std::nullopt;
}

bool WorkerCatalog::tryConsumeCapacity(const HostAddr& hostAddr, const size_t capacity)
{
    if (not workers.contains(hostAddr))
    {
        NES_WARNING("Worker with addr {} not found in the catalog", hostAddr);
        return false;
    }

    auto& worker = workers.at(hostAddr);
    if (worker.capacity - capacity < 0)
    {
        return false;
    }
    worker.capacity -= capacity;
    return true;
}

void WorkerCatalog::returnCapacity(const HostAddr& hostAddr, const size_t capacity)
{
    if (not workers.contains(hostAddr))
    {
        NES_WARNING("Worker with addr {} not found in the catalog", hostAddr);
        return;
    }
    workers.at(hostAddr).capacity += capacity;
}

size_t WorkerCatalog::getNumWorkers() const
{
    return workers.size();
}

const Topology& WorkerCatalog::getTopology() const
{
    return topology; /// Copy constructor creates a snapshot
}

std::vector<WorkerConfig> WorkerCatalog::getAllWorkers() const
{
    return workers | std::views::values | std::ranges::to<std::vector<WorkerConfig> >();
}
}