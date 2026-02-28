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
#include <Identifiers/Identifiers.hpp>
#include <NetworkTopology.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
constexpr size_t INFINITE_CAPACITY = 100'000'000;

/// Catalog of worker nodes in a distributed NebulaStream cluster.
/// Maintains the network topology to enable path-finding for distributed
/// query decomposition and placement validation.
class WorkerCatalog
{
    std::unordered_map<WorkerId, WorkerConfig> workers;
    NetworkTopology topology;
    uint64_t version = 0;

public:
    bool addWorker(
        const WorkerId& host,
        std::string connection,
        size_t capacity,
        const std::vector<WorkerId>& downstream,
        SingleNodeWorkerConfiguration config = {}); /// NOLINT(fuchsia-default-arguments-declarations)
    std::optional<WorkerConfig> removeWorker(const WorkerId& hostAddr);
    [[nodiscard]] std::optional<WorkerConfig> getWorker(const WorkerId& hostAddr) const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] std::vector<WorkerConfig> getAllWorkers() const;
    [[nodiscard]] NetworkTopology getTopology() const;

    /// Every change to the workerCatalog increments the version. This allows other components to check if the catalog has changed.
    [[nodiscard]] uint64_t getVersion() const;
};

}
