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

#include <WorkerStatus.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <cpptrace/basic.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{
namespace
{
WorkerStatusResponse::ReplayMetrics::RecordingLifecycleState
serializeRecordingLifecycleState(const Replay::RecordingLifecycleState lifecycleState)
{
    switch (lifecycleState)
    {
        case Replay::RecordingLifecycleState::New:
            return WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_NEW;
        case Replay::RecordingLifecycleState::Installing:
            return WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_INSTALLING;
        case Replay::RecordingLifecycleState::Filling:
            return WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_FILLING;
        case Replay::RecordingLifecycleState::Ready:
            return WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_READY;
        case Replay::RecordingLifecycleState::Draining:
            return WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_DRAINING;
        case Replay::RecordingLifecycleState::Deleted:
            return WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_DELETED;
    }
    std::unreachable();
}

Replay::RecordingLifecycleState
deserializeRecordingLifecycleState(const WorkerStatusResponse::ReplayMetrics::RecordingLifecycleState lifecycleState)
{
    switch (lifecycleState)
    {
        case WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_NEW:
            return Replay::RecordingLifecycleState::New;
        case WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_INSTALLING:
            return Replay::RecordingLifecycleState::Installing;
        case WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_FILLING:
            return Replay::RecordingLifecycleState::Filling;
        case WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_READY:
            return Replay::RecordingLifecycleState::Ready;
        case WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_DRAINING:
            return Replay::RecordingLifecycleState::Draining;
        case WorkerStatusResponse::ReplayMetrics::RECORDING_LIFECYCLE_STATE_DELETED:
            return Replay::RecordingLifecycleState::Deleted;
        default:
            break;
    }
    throw CannotDeserialize("Unknown recording lifecycle state {}", static_cast<int>(lifecycleState));
}
}

void serializeWorkerStatus(const WorkerStatus& status, WorkerStatusResponse* response)
{
    for (const auto& activeQuery : status.activeQueries)
    {
        auto* activeQueryGRPC = response->add_active_queries();
        *activeQueryGRPC->mutable_query_id() = QueryPlanSerializationUtil::serializeQueryId(activeQuery.queryId);
        if (activeQuery.started)
        {
            activeQueryGRPC->set_started_unix_timestamp_in_milli_seconds(
                std::chrono::duration_cast<std::chrono::milliseconds>(activeQuery.started->time_since_epoch()).count());
        }
    }

    for (const auto& terminatedQuery : status.terminatedQueries)
    {
        auto* terminatedQueryGRPC = response->add_terminated_queries();
        *terminatedQueryGRPC->mutable_query_id() = QueryPlanSerializationUtil::serializeQueryId(terminatedQuery.queryId);
        if (terminatedQuery.started)
        {
            terminatedQueryGRPC->set_started_unix_timestamp_in_milli_seconds(
                std::chrono::duration_cast<std::chrono::milliseconds>(terminatedQuery.started->time_since_epoch()).count());
        }
        terminatedQueryGRPC->set_terminated_unix_timestamp_in_milli_seconds(
            std::chrono::duration_cast<std::chrono::milliseconds>(terminatedQuery.terminated.time_since_epoch()).count());
        if (terminatedQuery.error)
        {
            const auto& exception = terminatedQuery.error.value();
            auto* errorGRPC = terminatedQueryGRPC->mutable_error();
            errorGRPC->set_message(exception.what());
            errorGRPC->set_stacktrace(exception.trace().to_string());
            errorGRPC->set_code(exception.code());
            errorGRPC->set_location(fmt::format(
                "{}:{}",
                exception.where().transform([](const auto& where) { return where.filename; }).value_or("unknown"),
                exception.where().transform([](const auto& where) { return where.line.value_or(-1); }).value_or(-1)));
        }
    }
    response->set_after_unix_timestamp_in_milli_seconds(
        std::chrono::duration_cast<std::chrono::milliseconds>(status.after.time_since_epoch()).count());
    response->set_until_unix_timestamp_in_milli_seconds(
        std::chrono::duration_cast<std::chrono::milliseconds>(status.until.time_since_epoch()).count());
    auto* replayMetricsGRPC = response->mutable_replay_metrics();
    replayMetricsGRPC->set_recording_storage_bytes(status.replayMetrics.recordingStorageBytes);
    replayMetricsGRPC->set_recording_file_count(status.replayMetrics.recordingFileCount);
    replayMetricsGRPC->set_active_query_count(status.replayMetrics.activeQueryCount);
    replayMetricsGRPC->set_replay_read_bytes(status.replayMetrics.replayReadBytes);
    for (const auto& statistic : status.replayMetrics.operatorStatistics)
    {
        auto* operatorStatisticGRPC = replayMetricsGRPC->add_operator_statistics();
        operatorStatisticGRPC->set_node_fingerprint(statistic.nodeFingerprint);
        operatorStatisticGRPC->set_input_tuples(statistic.inputTuples);
        operatorStatisticGRPC->set_output_tuples(statistic.outputTuples);
        operatorStatisticGRPC->set_task_count(statistic.taskCount);
        operatorStatisticGRPC->set_execution_time_nanos(statistic.executionTimeNanos);
    }
    for (const auto& recordingStatus : status.replayMetrics.recordingStatuses)
    {
        auto* recordingStatusGRPC = replayMetricsGRPC->add_recording_statuses();
        recordingStatusGRPC->set_recording_id(recordingStatus.recordingId);
        recordingStatusGRPC->set_file_path(recordingStatus.filePath);
        recordingStatusGRPC->set_lifecycle_state(serializeRecordingLifecycleState(recordingStatus.lifecycleState));
        if (recordingStatus.retentionWindowMs.has_value())
        {
            recordingStatusGRPC->set_retention_window_ms(*recordingStatus.retentionWindowMs);
        }
        if (recordingStatus.retainedStartWatermark.has_value())
        {
            recordingStatusGRPC->set_retained_start_watermark(recordingStatus.retainedStartWatermark->getRawValue());
        }
        if (recordingStatus.retainedEndWatermark.has_value())
        {
            recordingStatusGRPC->set_retained_end_watermark(recordingStatus.retainedEndWatermark->getRawValue());
        }
        if (recordingStatus.fillWatermark.has_value())
        {
            recordingStatusGRPC->set_fill_watermark(recordingStatus.fillWatermark->getRawValue());
        }
        recordingStatusGRPC->set_segment_count(recordingStatus.segmentCount);
        recordingStatusGRPC->set_storage_bytes(recordingStatus.storageBytes);
        if (recordingStatus.successorRecordingId.has_value())
        {
            recordingStatusGRPC->set_successor_recording_id(*recordingStatus.successorRecordingId);
        }
    }
}

WorkerStatus deserializeWorkerStatus(const WorkerStatusResponse* response)
{
    auto fromMillis = [](uint64_t unixTimestampInMillis)
    { return std::chrono::system_clock::time_point{std::chrono::milliseconds{unixTimestampInMillis}}; };
    return {
        .after = fromMillis(response->after_unix_timestamp_in_milli_seconds()),
        .until = fromMillis(response->until_unix_timestamp_in_milli_seconds()),
        .activeQueries = response->active_queries()
            | std::views::transform(
                             [&](const auto& activeQuery)
                             {
                                 return WorkerStatus::ActiveQuery{
                                     .queryId = QueryPlanSerializationUtil::deserializeQueryId(activeQuery.query_id()),
                                     .started = activeQuery.has_started_unix_timestamp_in_milli_seconds()
                                         ? std::make_optional(fromMillis(activeQuery.started_unix_timestamp_in_milli_seconds()))
                                         : std::nullopt};
                             })
            | std::ranges::to<std::vector>(),
        .terminatedQueries
        = response->terminated_queries()
            | std::views::transform(
                [&](const auto& terminatedQuery)
                {
                    return WorkerStatus::TerminatedQuery{
                        .queryId = QueryPlanSerializationUtil::deserializeQueryId(terminatedQuery.query_id()),
                        .started = terminatedQuery.has_started_unix_timestamp_in_milli_seconds()
                            ? std::make_optional(fromMillis(terminatedQuery.started_unix_timestamp_in_milli_seconds()))
                            : std::nullopt,
                        .terminated = fromMillis(terminatedQuery.terminated_unix_timestamp_in_milli_seconds()),
                        .error = terminatedQuery.has_error()
                            ? std::make_optional(Exception(terminatedQuery.error().message(), terminatedQuery.error().code()))
                            : std::nullopt};
                })
            | std::ranges::to<std::vector>(),
        .replayMetrics
        = WorkerStatus::ReplayMetrics{
            .recordingStorageBytes = response->replay_metrics().recording_storage_bytes(),
            .recordingFileCount = response->replay_metrics().recording_file_count(),
            .activeQueryCount = response->replay_metrics().active_query_count(),
            .replayReadBytes = response->replay_metrics().replay_read_bytes(),
            .operatorStatistics
            = response->replay_metrics().operator_statistics()
                | std::views::transform(
                    [](const auto& statistic)
                    {
                        return ReplayOperatorStatistics{
                            .nodeFingerprint = statistic.node_fingerprint(),
                            .inputTuples = statistic.input_tuples(),
                            .outputTuples = statistic.output_tuples(),
                            .taskCount = statistic.task_count(),
                            .executionTimeNanos = statistic.execution_time_nanos()};
                    })
                | std::ranges::to<std::vector>(),
            .recordingStatuses
            = response->replay_metrics().recording_statuses()
                | std::views::transform(
                    [](const auto& recordingStatus)
                    {
                        return Replay::RecordingRuntimeStatus{
                            .recordingId = recordingStatus.recording_id(),
                            .filePath = recordingStatus.file_path(),
                            .lifecycleState = deserializeRecordingLifecycleState(recordingStatus.lifecycle_state()),
                            .retentionWindowMs = recordingStatus.has_retention_window_ms()
                                ? std::make_optional(recordingStatus.retention_window_ms())
                                : std::nullopt,
                            .retainedStartWatermark = recordingStatus.has_retained_start_watermark()
                                ? std::make_optional(Timestamp(recordingStatus.retained_start_watermark()))
                                : std::nullopt,
                            .retainedEndWatermark = recordingStatus.has_retained_end_watermark()
                                ? std::make_optional(Timestamp(recordingStatus.retained_end_watermark()))
                                : std::nullopt,
                            .fillWatermark = recordingStatus.has_fill_watermark()
                                ? std::make_optional(Timestamp(recordingStatus.fill_watermark()))
                                : std::nullopt,
                            .segmentCount = recordingStatus.segment_count(),
                            .storageBytes = recordingStatus.storage_bytes(),
                            .successorRecordingId = recordingStatus.has_successor_recording_id()
                                ? std::make_optional(recordingStatus.successor_recording_id())
                                : std::nullopt};
                    })
                | std::ranges::to<std::vector>()}};
}
}
