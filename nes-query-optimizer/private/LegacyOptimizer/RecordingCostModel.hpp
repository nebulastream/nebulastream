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
#include <optional>

#include <Operators/LogicalOperator.hpp>
#include <Replay/ReplaySpecification.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

struct RecordingCostEstimate
{
    size_t estimatedStorageBytes = 0;
    size_t operatorCount = 0;
    size_t estimatedReplayBandwidthBytesPerSecond = 0;
    std::chrono::milliseconds estimatedReplayLatency{0};
    double maintenanceCost = 0.0;
    double replayCost = 0.0;
    double recomputeCost = 0.0;
    bool fitsBudget = false;
    bool satisfiesReplayLatency = true;

    [[nodiscard]] double totalCost() const { return maintenanceCost + replayCost; }
};

class RecordingCostModel
{
public:
    [[nodiscard]] RecordingCostEstimate estimateNewRecording(
        const LogicalOperator& recordedSubplanRoot,
        const WorkerConfig& worker,
        const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
        const std::optional<ReplaySpecification>& replaySpecification) const;

    [[nodiscard]] RecordingCostEstimate estimateReplayReuse(
        const LogicalOperator& recordedSubplanRoot,
        const WorkerConfig& worker,
        const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
        const std::optional<ReplaySpecification>& replaySpecification,
        std::optional<uint64_t> retainedWindowMs) const;

    [[nodiscard]] RecordingCostEstimate estimateRecordingUpgrade(
        const LogicalOperator& recordedSubplanRoot,
        const WorkerConfig& worker,
        const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
        const std::optional<ReplaySpecification>& replaySpecification,
        std::optional<uint64_t> existingRetentionWindowMs) const;
};

}
