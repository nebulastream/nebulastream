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
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>

#include <Util/Logger/Logger.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

bool WorkerCatalog::addWorker(const HostAddr& host, const GrpcAddr& grpc, size_t capacity, const std::vector<HostAddr>& downstream)
{
    const bool added = workers.try_emplace(host, host, grpc, capacity, downstream).second;
    if (added)
    {
        topology.addNode(host, downstream);
    }
    return added;
}

std::optional<WorkerConfig> WorkerCatalog::removeWorker(const HostAddr& hostAddr)
{
    const auto it = std::ranges::find_if(
        workers,
        [&](const auto& pair)
        {
            const auto& [addr, _] = pair;
            return hostAddr == addr;
        });

    if (it == workers.end())
    {
        return std::nullopt;
    }

    topology.removeNode(hostAddr);
    return {workers.extract(it).mapped()};
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
    return workers | std::views::values | std::ranges::to<std::vector<WorkerConfig>>();
}

}
