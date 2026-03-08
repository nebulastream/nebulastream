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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <NetworkTopology.hpp>
#include <Replay/ReplayExecutionStatistics.hpp>
#include <Replay/RecordingRuntimeStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
constexpr size_t INFINITE_CAPACITY = 100'000'000;

struct WorkerRuntimeMetrics
{
    std::chrono::system_clock::time_point observedAt{};
    size_t recordingStorageBytes = 0;
    size_t recordingFileCount = 0;
    size_t activeQueryCount = 0;
    size_t recordingWriteBytesPerSecond = 0;
    uint64_t replayReadBytes = 0;
    size_t replayReadBytesPerSecond = 0;
    std::unordered_map<std::string, ReplayOperatorStatistics> replayOperatorStatistics;
    std::vector<Replay::RecordingRuntimeStatus> recordingStatuses;

    [[nodiscard]] bool operator==(const WorkerRuntimeMetrics& other) const = default;
};

/// Catalog of worker nodes in a distributed NebulaStream cluster.
/// Maintains the network topology to enable path-finding for distributed
/// query decomposition and placement validation.
class WorkerCatalog
{
    std::unordered_map<Host, WorkerConfig> workers;
    std::unordered_map<Host, WorkerRuntimeMetrics> runtimeMetrics;
    NetworkTopology topology;
    uint64_t version = 0;

public:
    bool addWorker(
        const Host& host,
        std::string data,
        size_t capacity,
        const std::vector<Host>& downstream,
        SingleNodeWorkerConfiguration config = {},
        size_t recordingStorageBudget = INFINITE_CAPACITY); /// NOLINT(fuchsia-default-arguments-declarations)
    std::optional<WorkerConfig> removeWorker(const Host& hostAddr);
    [[nodiscard]] std::optional<WorkerConfig> getWorker(const Host& hostAddr) const;
    [[nodiscard]] std::optional<WorkerRuntimeMetrics> getWorkerRuntimeMetrics(const Host& hostAddr) const;
    [[nodiscard]] std::optional<ReplayOperatorStatistics> getReplayOperatorStatistics(const Host& hostAddr, std::string_view nodeFingerprint) const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] std::vector<WorkerConfig> getAllWorkers() const;
    [[nodiscard]] NetworkTopology getTopology() const;
    bool setRecordingStorageBudget(const Host& hostAddr, size_t recordingStorageBudget);
    bool updateWorkerRuntimeMetrics(const Host& hostAddr, WorkerRuntimeMetrics metrics);

    /// Every change to the workerCatalog increments the version. This allows other components to check if the catalog has changed.
    [[nodiscard]] uint64_t getVersion() const;
};

}
