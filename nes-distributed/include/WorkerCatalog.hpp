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
#include <optional>
#include <shared_mutex>
#include <unordered_map>

#include <NetworkTopology.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

class WorkerCatalog
{
    std::unordered_map<HostAddr, WorkerConfig> workers;
    Topology topology;
    uint64_t version = 0;

public:
    bool addWorker(const HostAddr& host, const GrpcAddr& grpc, size_t capacity, const std::vector<HostAddr>& downstream);
    std::optional<WorkerConfig> removeWorker(const HostAddr& hostAddr);
    [[nodiscard]] std::optional<WorkerConfig> getWorker(const HostAddr& hostAddr) const;
    bool tryConsumeCapacity(const HostAddr& hostAddr, size_t capacity);
    [[nodiscard]] size_t getNumWorkers() const;
    std::vector<WorkerConfig> getAllWorkers() const;
    [[nodiscard]] Topology getTopology() const;
    [[nodiscard]] uint64_t getVersion() const;
};

}
