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
#include <cstdint>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

WorkerCatalog::WorkerCatalog(const std::vector<WorkerConfig>& workers)
{
    for (const auto& [host, data, maxOperators, downstream, config] : workers)
    {
        this->addWorker(host, data, maxOperators, downstream, config);
    }
}

bool WorkerCatalog::addWorker(
    const Host& host,
    std::string dataAddress,
    Capacity maxOperators,
    const std::vector<Host>& downstream,
    Schema<LiteralConfigValue, Ordered> config)
{
    const bool added = workers
                           .try_emplace(
                               host,
                               WorkerConfig{
                                   .host = host,
                                   .dataAddress = std::move(dataAddress),
                                   .maxOperators = maxOperators,
                                   .downstream = downstream,
                                   .config = std::move(config)})
                           .second;
    if (added)
    {
        topology.addNode(host, downstream);
        ++version;
    }
    return added;
}

std::optional<WorkerConfig> WorkerCatalog::removeWorker(const Host& hostAddr)
{
    if (const auto node = workers.extract(hostAddr); not node.empty())
    {
        topology.removeNode(hostAddr);
        const std::scoped_lock lock(slotMutex);
        occupiedSlots.erase(hostAddr);
        ++version;
        return std::move(node.mapped());
    }
    return std::nullopt;
}

void WorkerCatalog::addLink(const Host& upstream, const Host& downstream)
{
    topology.addLink(upstream, downstream);
    workers.at(upstream).downstream.emplace_back(downstream);
    ++version;
}

bool WorkerCatalog::removeLink(const Host& upstream, const Host& downstream)
{
    if (not topology.removeLink(upstream, downstream))
    {
        return false;
    }
    std::erase(workers.at(upstream).downstream, downstream);
    ++version;
    return true;
}

WorkerCatalog::WorkerCatalog(const WorkerCatalog& other) : workers(other.workers), topology(other.topology), version(other.version)
{
    const std::scoped_lock lock(other.slotMutex);
    occupiedSlots = other.occupiedSlots;
}

bool WorkerCatalog::tryOccupySlots(const Host& host, const size_t slots)
{
    PRECONDITION(workers.contains(host), "Cannot occupy slots of unknown worker {}", host);
    const std::scoped_lock lock(slotMutex);
    const auto available = getAvailableSlotsLocked(host);
    if (const auto* limited = std::get_if<CapacityKind::Limited>(&available); limited != nullptr && limited->value < slots)
    {
        return false;
    }
    occupiedSlots[host] += slots;
    return true;
}

void WorkerCatalog::occupySlots(const Host& host, const size_t slots)
{
    PRECONDITION(workers.contains(host), "Cannot occupy slots of unknown worker {}", host);
    const std::scoped_lock lock(slotMutex);
    occupiedSlots[host] += slots;
}

bool WorkerCatalog::releaseSlots(const Host& host, const size_t slots)
{
    if (not workers.contains(host))
    {
        /// The worker was removed from the topology, so are its slots.
        return true;
    }
    const std::scoped_lock lock(slotMutex);
    const auto it = occupiedSlots.find(host);
    const size_t occupied = it == occupiedSlots.end() ? 0 : it->second;
    if (occupied < slots)
    {
        return false;
    }
    if (slots > 0)
    {
        it->second -= slots;
    }
    return true;
}

size_t WorkerCatalog::getOccupiedSlots(const Host& host) const
{
    const std::scoped_lock lock(slotMutex);
    const auto it = occupiedSlots.find(host);
    return it == occupiedSlots.end() ? 0 : it->second;
}

Capacity WorkerCatalog::getAvailableSlots(const Host& host) const
{
    const std::scoped_lock lock(slotMutex);
    return getAvailableSlotsLocked(host);
}

bool WorkerCatalog::tryLockWorker(const Host& host)
{
    const std::scoped_lock lock(slotMutex);
    return lockedWorkers.insert(host).second;
}

void WorkerCatalog::unlockWorker(const Host& host)
{
    const std::scoped_lock lock(slotMutex);
    lockedWorkers.erase(host);
}

Capacity WorkerCatalog::getAvailableSlotsLocked(const Host& host) const
{
    PRECONDITION(workers.contains(host), "Cannot compute the available slots of unknown worker {}", host);
    const auto it = occupiedSlots.find(host);
    const size_t occupied = it == occupiedSlots.end() ? 0 : it->second;
    return std::visit(
        Overloaded{
            [](const CapacityKind::Unlimited& unlimited) -> Capacity { return unlimited; },
            [occupied](const CapacityKind::Limited& limited) -> Capacity
            { return CapacityKind::Limited{limited.value > occupied ? limited.value - occupied : 0}; }},
        workers.at(host).maxOperators);
}

[[nodiscard]] std::optional<WorkerConfig> WorkerCatalog::getWorker(const Host& hostAddr) const
{
    if (workers.contains(hostAddr))
    {
        return workers.at(hostAddr);
    }
    return std::nullopt;
}

size_t WorkerCatalog::size() const
{
    return workers.size();
}

NetworkTopology WorkerCatalog::getTopology() const
{
    return topology; /// Copy constructor creates a snapshot
}

std::vector<WorkerConfig> WorkerCatalog::getAllWorkers() const
{
    return workers | std::views::values | std::ranges::to<std::vector<WorkerConfig>>();
}

uint64_t WorkerCatalog::getVersion() const
{
    return version;
}

}
