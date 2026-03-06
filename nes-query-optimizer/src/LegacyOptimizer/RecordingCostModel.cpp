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

#include <LegacyOptimizer/RecordingCostModel.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>

#include <Replay/ReplayStorage.hpp>

namespace NES
{
namespace
{
constexpr double DEFAULT_RETENTION_WINDOW_MS = 60'000.0;
constexpr double DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND = 64.0 * 1024.0 * 1024.0;
constexpr double MIN_REPLAY_BANDWIDTH_BYTES_PER_SECOND = 4.0 * 1024.0 * 1024.0;
constexpr double COST_NORMALIZATION_BYTES = 4096.0;
constexpr double COST_NORMALIZATION_COMPLEXITY = 256.0;

std::optional<ReplaySpecification> withRetentionCoverage(
    const std::optional<ReplaySpecification>& replaySpecification, const std::optional<uint64_t> retentionWindowMs)
{
    if (!replaySpecification.has_value() && !retentionWindowMs.has_value())
    {
        return std::nullopt;
    }

    auto effectiveSpecification = replaySpecification.value_or(ReplaySpecification{});
    effectiveSpecification.retentionWindowMs = retentionWindowMs;
    return effectiveSpecification;
}

size_t estimateRecordingStorageBytes(const Schema& schema, const std::optional<ReplaySpecification>& replaySpecification)
{
    const auto schemaBytes = std::max(schema.getSizeOfSchemaInBytes(), size_t{1});
    const auto retentionWindowMs = replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; })
                                       .value_or(static_cast<uint64_t>(DEFAULT_RETENTION_WINDOW_MS));
    const auto retentionFactor = std::max(1.0, static_cast<double>(retentionWindowMs) / DEFAULT_RETENTION_WINDOW_MS);
    return std::max(
        static_cast<size_t>(std::ceil(schemaBytes * Replay::DEFAULT_ESTIMATED_RECORDING_ROWS * retentionFactor)),
        Replay::MIN_RECORDING_SIZE_BYTES);
}

size_t countOperators(const LogicalOperator& root)
{
    size_t count = 1;
    for (const auto& child : root.getChildren())
    {
        count += countOperators(child);
    }
    return count;
}

size_t availableRecordingStorageBytes(const WorkerConfig& worker, const std::optional<WorkerRuntimeMetrics>& runtimeMetrics)
{
    const auto usedRecordingStorageBytes = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0);
    return usedRecordingStorageBytes >= worker.recordingStorageBudget ? 0 : worker.recordingStorageBudget - usedRecordingStorageBytes;
}

double retentionFactor(const std::optional<ReplaySpecification>& replaySpecification)
{
    const auto retentionWindowMs = replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; })
                                       .value_or(static_cast<uint64_t>(DEFAULT_RETENTION_WINDOW_MS));
    return std::max(1.0, static_cast<double>(retentionWindowMs) / DEFAULT_RETENTION_WINDOW_MS);
}

size_t estimatedIngressWriteBytesPerSecond(size_t estimatedStorageBytes, const std::optional<ReplaySpecification>& replaySpecification)
{
    const auto retentionWindowMs = replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; })
                                       .value_or(static_cast<uint64_t>(DEFAULT_RETENTION_WINDOW_MS));
    return std::max<size_t>(1, estimatedStorageBytes * 1000 / std::max<uint64_t>(retentionWindowMs, 1));
}

size_t estimatedReplayBandwidthBytesPerSecond(
    size_t estimatedStorageBytes,
    const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
    const std::optional<ReplaySpecification>& replaySpecification,
    bool reuseExistingRecording)
{
    const auto activeQueryCount = runtimeMetrics.transform([](const auto& metrics) { return metrics.activeQueryCount; }).value_or(0);
    const auto recordingFileCount = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingFileCount; }).value_or(0);
    const auto liveWriteBytesPerSecond = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0);
    const auto candidateWriteBytesPerSecond = reuseExistingRecording ? 0 : estimatedIngressWriteBytesPerSecond(estimatedStorageBytes, replaySpecification);

    const auto contention = 1.0 + (static_cast<double>(activeQueryCount) * 0.25) + (static_cast<double>(recordingFileCount) * 0.05)
        + (static_cast<double>(liveWriteBytesPerSecond + candidateWriteBytesPerSecond) / DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND);
    const auto effectiveBandwidth = std::max(MIN_REPLAY_BANDWIDTH_BYTES_PER_SECOND, DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND / contention);
    return static_cast<size_t>(std::floor(effectiveBandwidth));
}

std::chrono::milliseconds estimatedReplayLatency(size_t estimatedStorageBytes, size_t estimatedReplayBandwidthBytesPerSecond)
{
    if (estimatedReplayBandwidthBytesPerSecond == 0)
    {
        return std::chrono::milliseconds::max();
    }
    return std::chrono::milliseconds(
        static_cast<int64_t>(std::ceil((static_cast<double>(estimatedStorageBytes) * 1000.0) / estimatedReplayBandwidthBytesPerSecond)));
}

double storageUtilization(const WorkerConfig& worker, const std::optional<WorkerRuntimeMetrics>& runtimeMetrics)
{
    if (!runtimeMetrics.has_value() || worker.recordingStorageBudget == 0)
    {
        return 0.0;
    }
    return static_cast<double>(runtimeMetrics->recordingStorageBytes) / worker.recordingStorageBudget;
}
}

RecordingCostEstimate RecordingCostModel::estimateNewRecording(
    const LogicalOperator& recordedSubplanRoot,
    const WorkerConfig& worker,
    const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
    const std::optional<ReplaySpecification>& replaySpecification) const
{
    const auto estimatedStorageBytes = estimateRecordingStorageBytes(recordedSubplanRoot.getOutputSchema(), replaySpecification);
    const auto operatorCount = countOperators(recordedSubplanRoot);
    const auto schemaBytes = std::max(recordedSubplanRoot.getOutputSchema().getSizeOfSchemaInBytes(), size_t{1});
    const auto effectiveReplayBandwidthBytesPerSecond
        = estimatedReplayBandwidthBytesPerSecond(estimatedStorageBytes, runtimeMetrics, replaySpecification, false);
    const auto replayLatency = estimatedReplayLatency(estimatedStorageBytes, effectiveReplayBandwidthBytesPerSecond);
    const auto activeQueryCount = runtimeMetrics.transform([](const auto& metrics) { return metrics.activeQueryCount; }).value_or(0);
    const auto liveWriteBytesPerSecond = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0);

    const auto maintenanceCost = (estimatedStorageBytes / COST_NORMALIZATION_BYTES) * retentionFactor(replaySpecification)
        * (1.0 + storageUtilization(worker, runtimeMetrics) + (activeQueryCount * 0.2)
           + (static_cast<double>(liveWriteBytesPerSecond) / DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND));
    const auto replayCost = (estimatedStorageBytes / COST_NORMALIZATION_BYTES)
        * (DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND / effectiveReplayBandwidthBytesPerSecond);
    const auto recomputeCost
        = ((operatorCount * schemaBytes) / COST_NORMALIZATION_COMPLEXITY) * (1.0 + (activeQueryCount * 0.15));

    return RecordingCostEstimate{
        .estimatedStorageBytes = estimatedStorageBytes,
        .operatorCount = operatorCount,
        .estimatedReplayBandwidthBytesPerSecond = effectiveReplayBandwidthBytesPerSecond,
        .estimatedReplayLatency = replayLatency,
        .maintenanceCost = maintenanceCost,
        .replayCost = replayCost,
        .recomputeCost = recomputeCost,
        .fitsBudget = estimatedStorageBytes <= availableRecordingStorageBytes(worker, runtimeMetrics),
        .satisfiesReplayLatency
        = replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; })
              .transform([&](const auto replayLatencyLimitMs) { return replayLatency <= std::chrono::milliseconds(replayLatencyLimitMs); })
              .value_or(true)};
}

RecordingCostEstimate RecordingCostModel::estimateReplayReuse(
    const LogicalOperator& recordedSubplanRoot,
    const WorkerConfig& worker,
    const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<uint64_t> retainedWindowMs) const
{
    const auto effectiveReplaySpecification = withRetentionCoverage(replaySpecification, retainedWindowMs);
    auto estimate = estimateNewRecording(recordedSubplanRoot, worker, runtimeMetrics, effectiveReplaySpecification);
    estimate.maintenanceCost = 0.0;
    estimate.fitsBudget = true;
    estimate.estimatedReplayBandwidthBytesPerSecond
        = estimatedReplayBandwidthBytesPerSecond(estimate.estimatedStorageBytes, runtimeMetrics, effectiveReplaySpecification, true);
    estimate.estimatedReplayLatency
        = estimatedReplayLatency(estimate.estimatedStorageBytes, estimate.estimatedReplayBandwidthBytesPerSecond);
    estimate.replayCost = (estimate.estimatedStorageBytes / COST_NORMALIZATION_BYTES)
        * (DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND / estimate.estimatedReplayBandwidthBytesPerSecond);
    estimate.satisfiesReplayLatency
        = replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; })
              .transform(
                  [&](const auto replayLatencyLimitMs)
                  { return estimate.estimatedReplayLatency <= std::chrono::milliseconds(replayLatencyLimitMs); })
              .value_or(true);
    return estimate;
}

RecordingCostEstimate RecordingCostModel::estimateRecordingUpgrade(
    const LogicalOperator& recordedSubplanRoot,
    const WorkerConfig& worker,
    const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<uint64_t> existingRetentionWindowMs) const
{
    auto upgradedEstimate = estimateNewRecording(recordedSubplanRoot, worker, runtimeMetrics, replaySpecification);
    const auto existingCoverage = withRetentionCoverage(std::nullopt, existingRetentionWindowMs);
    const auto existingEstimate = estimateNewRecording(recordedSubplanRoot, worker, runtimeMetrics, existingCoverage);

    upgradedEstimate.maintenanceCost = std::max(0.0, upgradedEstimate.maintenanceCost - existingEstimate.maintenanceCost);
    return upgradedEstimate;
}
}
