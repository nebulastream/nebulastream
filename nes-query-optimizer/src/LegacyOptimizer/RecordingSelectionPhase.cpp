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

#include <LegacyOptimizer/RecordingSelectionPhase.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>

#include <LegacyOptimizer/RecordingCostModel.hpp>
#include <LegacyOptimizer/RecordingFingerprint.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Replay/ReplayStorage.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <ErrorHandling.hpp>

#include <LegacyOptimizer/OperatorPlacement.hpp>

namespace NES
{

namespace
{
std::string replayLatencyLimitDescription(const std::optional<ReplaySpecification>& replaySpecification)
{
    return replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; })
        .transform([](const auto latencyLimitMs) { return std::to_string(latencyLimitMs); })
        .value_or("none");
}
}

RecordingSelectionResult
RecordingSelectionPhase::apply(
    LogicalPlan& placedPlan,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingCatalog& recordingCatalog) const
{
    if (!replaySpecification.has_value())
    {
        return {};
    }

    PRECONDITION(workerCatalog != nullptr, "Recording selection requires a valid worker catalog");

    auto newRoots = placedPlan.getRootOperators();
    PRECONDITION(
        newRoots.size() == 1,
        "Replay recording selection requires a single sink root, but got {} roots",
        newRoots.size());

    auto sink = newRoots.front().tryGetAs<SinkLogicalOperator>();
    PRECONDITION(sink.has_value(), "Replay recording selection requires a sink root, but found {}", newRoots.front().getName());
    PRECONDITION(
        newRoots.front().getChildren().size() == 1,
        "Replay recording selection requires a sink with exactly one child, but got {}",
        newRoots.front().getChildren().size());

    const auto placement = getPlacementFor(newRoots.front());
    const auto worker = workerCatalog->getWorker(placement);
    PRECONDITION(worker.has_value(), "Replay recording selection could not find worker {}", placement);
    const auto workerRuntimeMetrics = workerCatalog->getWorkerRuntimeMetrics(placement);

    const auto recordedChildren = newRoots.front().getChildren();
    const auto& recordedChild = recordedChildren.front();
    const auto recordingFingerprint = createRecordingFingerprint(recordedChild, placement, replaySpecification);
    const auto recordingId = recordingIdFromFingerprint(recordingFingerprint);
    const auto recordingFilePath = Replay::getRecordingFilePath(recordingId.getRawValue());
    const auto costModel = RecordingCostModel{};

    if (const auto existingRecording = recordingCatalog.getRecording(recordingId); existingRecording.has_value())
    {
        const auto reuseCost = costModel.estimateReplayReuse(recordedChild, *worker, workerRuntimeMetrics, replaySpecification);
        if (!reuseCost.satisfiesReplayLatency)
        {
            throw PlacementFailure(
                "Replay reuse on {} would exceed the replay latency limit of {} ms (estimated latency {} ms, replay cost {:.2f},"
                " recompute cost {:.2f}, write pressure {} B/s)",
                placement,
                replayLatencyLimitDescription(replaySpecification),
                reuseCost.estimatedReplayLatency.count(),
                reuseCost.replayCost,
                reuseCost.recomputeCost,
                workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0));
        }

        return RecordingSelectionResult{
            .selectedRecordings = {RecordingSelection{
                .recordingId = existingRecording->id,
                .node = existingRecording->node,
                .filePath = existingRecording->filePath}}};
    }

    const auto newRecordingCost = costModel.estimateNewRecording(recordedChild, *worker, workerRuntimeMetrics, replaySpecification);
    if (!newRecordingCost.fitsBudget)
    {
        throw PlacementFailure(
            "Replay recording on {} requires an estimated {} bytes, but only {} bytes remain in the recording budget of {} bytes"
            " (maintenance cost {:.2f}, replay cost {:.2f}, recompute cost {:.2f}, worker currently reports {} bytes in replay storage"
            " across {} recording files, {} active queries, and {} B/s replay write pressure)",
            placement,
            newRecordingCost.estimatedStorageBytes,
            worker->recordingStorageBudget
                - std::min(
                    worker->recordingStorageBudget,
                    workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0)),
            worker->recordingStorageBudget,
            newRecordingCost.maintenanceCost,
            newRecordingCost.replayCost,
            newRecordingCost.recomputeCost,
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0),
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingFileCount; }).value_or(0),
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.activeQueryCount; }).value_or(0),
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingWriteBytesPerSecond; }).value_or(0));
    }

    if (!newRecordingCost.satisfiesReplayLatency)
    {
        throw PlacementFailure(
            "Replay recording on {} would exceed the replay latency limit of {} ms (estimated latency {} ms at {} B/s,"
            " maintenance cost {:.2f}, replay cost {:.2f}, recompute cost {:.2f})",
            placement,
            replayLatencyLimitDescription(replaySpecification),
            newRecordingCost.estimatedReplayLatency.count(),
            newRecordingCost.estimatedReplayBandwidthBytesPerSecond,
            newRecordingCost.maintenanceCost,
            newRecordingCost.replayCost,
            newRecordingCost.recomputeCost);
    }

    auto store = StoreLogicalOperator(
                     StoreLogicalOperator::validateAndFormatConfig(
                         {{"file_path", recordingFilePath}, {"append", "false"}, {"header", "true"}}))
                     .withTraitSet(newRoots.front().getTraitSet())
                     .withInferredSchema({recordedChild.getOutputSchema()})
                     .withChildren(recordedChildren);

    newRoots.front() = newRoots.front().withChildren({std::move(store)});
    placedPlan = placedPlan.withRootOperators(newRoots);

    return RecordingSelectionResult{
        .selectedRecordings = {RecordingSelection{
            .recordingId = recordingId,
            .node = placement,
            .filePath = recordingFilePath}}};
}
}
