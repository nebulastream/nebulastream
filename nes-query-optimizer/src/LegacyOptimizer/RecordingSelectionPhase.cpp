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
#include <string>
#include <unordered_map>
#include <utility>

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
size_t estimateRecordingStorageBytes(const Schema& schema)
{
    const auto schemaBytes = std::max(schema.getSizeOfSchemaInBytes(), size_t{1});
    return std::max(
        schemaBytes * Replay::DEFAULT_ESTIMATED_RECORDING_ROWS,
        Replay::MIN_RECORDING_SIZE_BYTES);
}

size_t availableRecordingStorageBytes(const WorkerConfig& worker, const std::optional<WorkerRuntimeMetrics>& runtimeMetrics)
{
    const auto usedRecordingStorageBytes = runtimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0);
    return usedRecordingStorageBytes >= worker.recordingStorageBudget ? 0 : worker.recordingStorageBudget - usedRecordingStorageBytes;
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

    if (const auto existingRecording = recordingCatalog.getRecording(recordingId); existingRecording.has_value())
    {
        return RecordingSelectionResult{
            .selectedRecordings = {RecordingSelection{
                .recordingId = existingRecording->id,
                .node = existingRecording->node,
                .filePath = existingRecording->filePath}}};
    }

    const auto estimatedStorageBytes = estimateRecordingStorageBytes(recordedChild.getOutputSchema());
    const auto freeRecordingStorageBytes = availableRecordingStorageBytes(*worker, workerRuntimeMetrics);
    if (estimatedStorageBytes > freeRecordingStorageBytes)
    {
        throw PlacementFailure(
            "Replay recording on {} requires an estimated {} bytes, but only {} bytes remain in the recording budget of {} bytes"
            " (worker currently reports {} bytes in replay storage across {} recording files and {} active queries)",
            placement,
            estimatedStorageBytes,
            freeRecordingStorageBytes,
            worker->recordingStorageBudget,
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingStorageBytes; }).value_or(0),
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.recordingFileCount; }).value_or(0),
            workerRuntimeMetrics.transform([](const auto& metrics) { return metrics.activeQueryCount; }).value_or(0));
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
