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
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <NetworkTopology.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

inline void addWorkers()
{
}

/// Catalog of worker nodes in a distributed NebulaStream cluster.
/// Maintains the network topology to enable path-finding for distributed
/// query decomposition and placement validation.
class WorkerCatalog
{
    std::unordered_map<Host, WorkerConfig> workers;
    /// Number of operator slots occupied by the operators placed on a worker (the original's TopologyNode::occupySlots).
    std::unordered_map<Host, size_t> occupiedSlots;
    NetworkTopology topology;
    uint64_t version = 0;

public:
    WorkerCatalog() = default;
    explicit WorkerCatalog(const std::vector<WorkerConfig>& workers);

    bool addWorker(
        const Host& host,
        std::string dataAddress,
        Capacity maxOperators,
        const std::vector<Host>& downstream,
        Schema<LiteralConfigValue, Ordered> config = {}); /// NOLINT(fuchsia-default-arguments-declarations)
    std::optional<WorkerConfig> removeWorker(const Host& hostAddr);

    /// Adds the topology link upstream -> downstream. Throws InvalidTopology if the link is invalid (see NetworkTopology::addLink).
    void addLink(const Host& upstream, const Host& downstream);

    /// Removes the topology link upstream -> downstream. Returns false if the link does not exist.
    bool removeLink(const Host& upstream, const Host& downstream);
    [[nodiscard]] std::optional<WorkerConfig> getWorker(const Host& hostAddr) const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] std::vector<WorkerConfig> getAllWorkers() const;
    [[nodiscard]] NetworkTopology getTopology() const;

    /// Occupies `slots` operator slots of the worker if its capacity allows it. Returns false and occupies nothing otherwise.
    /// Used by the placement amendment of ISQP to validate a computed placement.
    [[nodiscard]] bool tryOccupySlots(const Host& host, size_t slots);

    /// Occupies `slots` operator slots of the worker without checking its capacity. Used to account for the initial (holistic)
    /// placement, which keeps its own capacity semantics.
    void occupySlots(const Host& host, size_t slots);

    /// Releases `slots` operator slots of the worker. Returns false and releases nothing if fewer slots are occupied.
    [[nodiscard]] bool releaseSlots(const Host& host, size_t slots);

    [[nodiscard]] size_t getOccupiedSlots(const Host& host) const;

    /// Returns the remaining capacity of the worker, i.e., its capacity minus the occupied slots.
    [[nodiscard]] Capacity getAvailableSlots(const Host& host) const;

    /// Every change to the workerCatalog increments the version. This allows other components to check if the catalog has changed.
    [[nodiscard]] uint64_t getVersion() const;
};

}
