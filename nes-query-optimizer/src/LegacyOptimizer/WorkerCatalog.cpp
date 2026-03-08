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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

bool WorkerCatalog::addWorker(
    const Host& host,
    std::string data,
    size_t capacity,
    const std::vector<Host>& downstream,
    SingleNodeWorkerConfiguration config,
    size_t recordingStorageBudget)
{
    const bool added
        = workers
              .try_emplace(
                  host,
                  WorkerConfig{
                      .host = host,
                      .data = std::move(data),
                      .capacity = capacity,
                      .recordingStorageBudget = recordingStorageBudget,
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
        runtimeMetrics.erase(hostAddr);
        topology.removeNode(hostAddr);
        ++version;
        return std::move(node.mapped());
    }
    return std::nullopt;
}

[[nodiscard]] std::optional<WorkerConfig> WorkerCatalog::getWorker(const Host& hostAddr) const
{
    if (workers.contains(hostAddr))
    {
        return workers.at(hostAddr);
    }
    return std::nullopt;
}

std::optional<WorkerRuntimeMetrics> WorkerCatalog::getWorkerRuntimeMetrics(const Host& hostAddr) const
{
    if (runtimeMetrics.contains(hostAddr))
    {
        return runtimeMetrics.at(hostAddr);
    }
    return std::nullopt;
}

std::optional<ReplayOperatorStatistics> WorkerCatalog::getReplayOperatorStatistics(const Host& hostAddr, std::string_view nodeFingerprint) const
{
    const auto metrics = getWorkerRuntimeMetrics(hostAddr);
    if (!metrics.has_value())
    {
        return std::nullopt;
    }

    if (const auto it = metrics->replayOperatorStatistics.find(std::string(nodeFingerprint)); it != metrics->replayOperatorStatistics.end())
    {
        return it->second;
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

bool WorkerCatalog::setRecordingStorageBudget(const Host& hostAddr, const size_t recordingStorageBudget)
{
    if (!workers.contains(hostAddr))
    {
        return false;
    }

    workers.at(hostAddr).recordingStorageBudget = recordingStorageBudget;
    return true;
}

std::vector<WorkerConfig> WorkerCatalog::getAllWorkers() const
{
    return workers | std::views::values | std::ranges::to<std::vector<WorkerConfig>>();
}

bool WorkerCatalog::updateWorkerRuntimeMetrics(const Host& hostAddr, WorkerRuntimeMetrics metrics)
{
    if (!workers.contains(hostAddr))
    {
        return false;
    }

    if (const auto existingMetrics = getWorkerRuntimeMetrics(hostAddr); existingMetrics.has_value())
    {
        const auto elapsed = metrics.observedAt - existingMetrics->observedAt;
        const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
        if (elapsedMs > 0 && metrics.recordingStorageBytes >= existingMetrics->recordingStorageBytes)
        {
            const auto deltaBytes = metrics.recordingStorageBytes - existingMetrics->recordingStorageBytes;
            metrics.recordingWriteBytesPerSecond = deltaBytes == 0 ? 0 : deltaBytes * 1000 / static_cast<size_t>(elapsedMs);
        }
        else
        {
            metrics.recordingWriteBytesPerSecond = 0;
        }
    }

    runtimeMetrics.insert_or_assign(hostAddr, std::move(metrics));
    return true;
}

uint64_t WorkerCatalog::getVersion() const
{
    return version;
}

}
