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
constexpr double DEFAULT_FALLBACK_INGRESS_ROWS_PER_SECOND
    = static_cast<double>(Replay::DEFAULT_ESTIMATED_RECORDING_ROWS) / (DEFAULT_RETENTION_WINDOW_MS / 1000.0);

double representationCompressionRatio(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return 1.0;
        case RecordingRepresentation::BinaryStoreZstdFast1:
            return 0.36;
        case RecordingRepresentation::BinaryStoreZstd:
            return 0.45;
        case RecordingRepresentation::BinaryStoreZstdFast6:
            return 0.58;
    }
    std::unreachable();
}

double representationBandwidthMultiplier(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return 1.0;
        case RecordingRepresentation::BinaryStoreZstdFast1:
            return 0.30;
        case RecordingRepresentation::BinaryStoreZstd:
            return 0.35;
        case RecordingRepresentation::BinaryStoreZstdFast6:
            return 0.42;
    }
    std::unreachable();
}

double replayTimeMultiplier(const RecordingPlacementContext& placementContext)
{
    return 1.0 + (std::max<size_t>(placementContext.mergedOccurrenceCount, 1) - 1) * 0.35
        + (std::max<size_t>(placementContext.activeReplayConsumerCount, 1) - 1) * 0.25 + placementContext.downstreamHopCount * 0.2
        + placementContext.installedRecordingCount * 0.05;
}

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

uint64_t retentionWindowMsOrDefault(const std::optional<ReplaySpecification>& replaySpecification)
{
    return replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; })
        .value_or(static_cast<uint64_t>(DEFAULT_RETENTION_WINDOW_MS));
}

size_t estimatedIngressWriteBytesPerSecond(const Schema& schema, const RecordingPlacementContext& placementContext)
{
    if (placementContext.estimatedIngressWriteBytesPerSecond > 0)
    {
        return placementContext.estimatedIngressWriteBytesPerSecond;
    }

    const auto schemaBytes = std::max(schema.getSizeOfSchemaInBytes(), size_t{1});
    return std::max<size_t>(1, static_cast<size_t>(std::ceil(schemaBytes * DEFAULT_FALLBACK_INGRESS_ROWS_PER_SECOND)));
}

size_t estimateRetainedRecordingBytes(
    const Schema& schema, const std::optional<ReplaySpecification>& replaySpecification, const RecordingPlacementContext& placementContext)
{
    const auto retentionWindowMs = retentionWindowMsOrDefault(replaySpecification);
    const auto rawRetainedBytes = static_cast<double>(estimatedIngressWriteBytesPerSecond(schema, placementContext))
        * (static_cast<double>(retentionWindowMs) / 1000.0);
    return std::max(
        static_cast<size_t>(std::ceil(rawRetainedBytes * representationCompressionRatio(placementContext.representation))),
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
    const auto retentionWindowMs = retentionWindowMsOrDefault(replaySpecification);
    return std::max(1.0, static_cast<double>(retentionWindowMs) / DEFAULT_RETENTION_WINDOW_MS);
}

size_t estimatedReplayBandwidthBytesPerSecond(
    size_t candidateWriteBytesPerSecond,
    const RecordingPlacementContext& placementContext,
    const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
    bool reuseExistingRecording)
{
    const auto activeQueryCount = runtimeMetrics.transform([](const auto& metrics) { return metrics.activeQueryCount; }).value_or(0);
    const auto recordingFileCount = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingFileCount; }).value_or(0);
    const auto liveWriteBytesPerSecond = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0);
    const auto effectiveCandidateWriteBytesPerSecond = reuseExistingRecording ? 0 : candidateWriteBytesPerSecond;

    const auto contention = 1.0 + (static_cast<double>(activeQueryCount) * 0.25) + (static_cast<double>(recordingFileCount) * 0.05)
        + (static_cast<double>(liveWriteBytesPerSecond + effectiveCandidateWriteBytesPerSecond) / DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND);
    const auto topologyPenalty = 1.0 + placementContext.totalRouteHopCount * 0.2 + placementContext.downstreamHopCount * 0.15;
    const auto effectiveBandwidth = std::max(
        MIN_REPLAY_BANDWIDTH_BYTES_PER_SECOND,
        (DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND * representationBandwidthMultiplier(placementContext.representation)) / (contention * topologyPenalty));
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
    const RecordingPlacementContext& placementContext,
    const WorkerConfig& worker,
    const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
    const std::optional<ReplaySpecification>& replaySpecification) const
{
    const auto estimatedStorageBytes = estimateRetainedRecordingBytes(recordedSubplanRoot.getOutputSchema(), replaySpecification, placementContext);
    const auto operatorCount = countOperators(recordedSubplanRoot);
    const auto schemaBytes = std::max(recordedSubplanRoot.getOutputSchema().getSizeOfSchemaInBytes(), size_t{1});
    const auto candidateWriteBytesPerSecond = estimatedIngressWriteBytesPerSecond(recordedSubplanRoot.getOutputSchema(), placementContext);
    const auto effectiveReplayBandwidthBytesPerSecond
        = estimatedReplayBandwidthBytesPerSecond(candidateWriteBytesPerSecond, placementContext, runtimeMetrics, false);
    const auto replayLatency = estimatedReplayLatency(estimatedStorageBytes, effectiveReplayBandwidthBytesPerSecond);
    const auto activeQueryCount = runtimeMetrics.transform([](const auto& metrics) { return metrics.activeQueryCount; }).value_or(0);
    const auto liveWriteBytesPerSecond = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0);
    const auto timeMultiplier = replayTimeMultiplier(placementContext);
    const auto candidateWritePressure = static_cast<double>(candidateWriteBytesPerSecond) / DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND;

    const auto maintenanceCost = (estimatedStorageBytes / COST_NORMALIZATION_BYTES) * retentionFactor(replaySpecification)
        * (1.0 + storageUtilization(worker, runtimeMetrics) + (activeQueryCount * 0.2)
           + (static_cast<double>(liveWriteBytesPerSecond) / DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND) + candidateWritePressure
           + placementContext.upstreamHopCount * 0.25
           + (std::max<size_t>(placementContext.mergedOccurrenceCount, 1) - 1) * 0.1);
    const auto replayCost = (estimatedStorageBytes / COST_NORMALIZATION_BYTES) * (DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND / effectiveReplayBandwidthBytesPerSecond)
        * (1.0 + placementContext.downstreamHopCount * 0.2 + placementContext.installedRecordingCount * 0.05);
    const auto recomputeCost = ((operatorCount * schemaBytes) / COST_NORMALIZATION_COMPLEXITY)
        * (1.0 + (activeQueryCount * 0.15) + placementContext.totalRouteHopCount * 0.2
           + (std::max<size_t>(placementContext.activeReplayConsumerCount, 1) - 1) * 0.25);

    return RecordingCostEstimate{
        .estimatedStorageBytes = estimatedStorageBytes,
        .incrementalStorageBytes = estimatedStorageBytes,
        .operatorCount = operatorCount,
        .estimatedReplayBandwidthBytesPerSecond = effectiveReplayBandwidthBytesPerSecond,
        .estimatedReplayLatency = replayLatency,
        .maintenanceCost = maintenanceCost,
        .replayCost = replayCost,
        .recomputeCost = recomputeCost,
        .replayTimeMultiplier = timeMultiplier,
        .fitsBudget = estimatedStorageBytes <= availableRecordingStorageBytes(worker, runtimeMetrics),
        .satisfiesReplayLatency
        = replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; })
              .transform([&](const auto replayLatencyLimitMs) { return replayLatency <= std::chrono::milliseconds(replayLatencyLimitMs); })
              .value_or(true)};
}

RecordingCostEstimate RecordingCostModel::estimateReplayReuse(
    const LogicalOperator& recordedSubplanRoot,
    const RecordingPlacementContext& placementContext,
    const WorkerConfig& worker,
    const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<uint64_t> retainedWindowMs) const
{
    const auto effectiveReplaySpecification = withRetentionCoverage(replaySpecification, retainedWindowMs);
    auto estimate = estimateNewRecording(recordedSubplanRoot, placementContext, worker, runtimeMetrics, effectiveReplaySpecification);
    estimate.maintenanceCost = 0.0;
    estimate.estimatedReplayBandwidthBytesPerSecond
        = estimatedReplayBandwidthBytesPerSecond(
            estimatedIngressWriteBytesPerSecond(recordedSubplanRoot.getOutputSchema(), placementContext),
            placementContext,
            runtimeMetrics,
            true);
    estimate.estimatedReplayLatency
        = estimatedReplayLatency(estimate.estimatedStorageBytes, estimate.estimatedReplayBandwidthBytesPerSecond);
    estimate.replayCost = (estimate.estimatedStorageBytes / COST_NORMALIZATION_BYTES)
        * (DEFAULT_REPLAY_BANDWIDTH_BYTES_PER_SECOND / estimate.estimatedReplayBandwidthBytesPerSecond);
    estimate.incrementalStorageBytes = 0;
    estimate.fitsBudget = true;
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
    const RecordingPlacementContext& placementContext,
    const WorkerConfig& worker,
    const std::optional<WorkerRuntimeMetrics>& runtimeMetrics,
    const std::optional<ReplaySpecification>& replaySpecification,
    const std::optional<uint64_t> existingRetentionWindowMs) const
{
    auto upgradedEstimate = estimateNewRecording(recordedSubplanRoot, placementContext, worker, runtimeMetrics, replaySpecification);
    const auto existingCoverage = withRetentionCoverage(std::nullopt, existingRetentionWindowMs);
    const auto existingEstimate = estimateNewRecording(recordedSubplanRoot, placementContext, worker, runtimeMetrics, existingCoverage);

    upgradedEstimate.incrementalStorageBytes = upgradedEstimate.estimatedStorageBytes > existingEstimate.estimatedStorageBytes
        ? upgradedEstimate.estimatedStorageBytes - existingEstimate.estimatedStorageBytes
        : 0;
    upgradedEstimate.maintenanceCost = std::max(0.0, upgradedEstimate.maintenanceCost - existingEstimate.maintenanceCost);
    upgradedEstimate.fitsBudget = upgradedEstimate.incrementalStorageBytes <= availableRecordingStorageBytes(worker, runtimeMetrics);
    return upgradedEstimate;
}
}
