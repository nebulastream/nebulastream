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
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include <LegacyOptimizer/RecordingCostModel.hpp>
#include <LegacyOptimizer/RecordingFingerprint.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Replay/ReplayStorage.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>

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

bool allowRecomputeFallback(const std::optional<ReplaySpecification>& replaySpecification)
{
    return replaySpecification.transform([](const auto& spec) { return spec.allowRecomputeFallback; }).value_or(false);
}

RecordingCostBreakdown toCostBreakdown(const RecordingCostEstimate& estimate, double decisionCost)
{
    return RecordingCostBreakdown{
        .decisionCost = decisionCost,
        .estimatedStorageBytes = estimate.estimatedStorageBytes,
        .operatorCount = estimate.operatorCount,
        .estimatedReplayBandwidthBytesPerSecond = estimate.estimatedReplayBandwidthBytesPerSecond,
        .estimatedReplayLatencyMs = static_cast<uint64_t>(std::max<int64_t>(estimate.estimatedReplayLatency.count(), 0)),
        .maintenanceCost = estimate.maintenanceCost,
        .replayCost = estimate.replayCost,
        .recomputeCost = estimate.recomputeCost,
        .fitsBudget = estimate.fitsBudget,
        .satisfiesReplayLatency = estimate.satisfiesReplayLatency};
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
    const auto newSelection = RecordingSelection{
        .recordingId = recordingId,
        .node = placement,
        .filePath = recordingFilePath};
    const auto newRecordingCost = costModel.estimateNewRecording(recordedChild, *worker, workerRuntimeMetrics, replaySpecification);
    const auto newRecordingFeasible = newRecordingCost.fitsBudget && newRecordingCost.satisfiesReplayLatency;

    const auto buildAlternative = [](RecordingSelectionDecision decision, const RecordingCostEstimate& estimate)
    {
        const auto decisionCost = decision == RecordingSelectionDecision::SkipRecording ? estimate.recomputeCost : estimate.totalCost();
        return RecordingSelectionAlternative{.decision = decision, .cost = toCostBreakdown(estimate, decisionCost)};
    };

    const auto buildSelectionResult =
        [](std::optional<RecordingSelection> selection,
           RecordingSelectionDecision decision,
           std::string reason,
           const RecordingCostEstimate& estimate,
           std::vector<RecordingSelectionAlternative> alternatives)
    {
        const auto decisionCost = decision == RecordingSelectionDecision::SkipRecording ? estimate.recomputeCost : estimate.totalCost();
        return RecordingSelectionResult{
            .selectedRecordings = selection.has_value() ? std::vector<RecordingSelection>{*selection} : std::vector<RecordingSelection>{},
            .explanations = {RecordingSelectionExplanation{
                .selection = std::move(selection),
                .decision = decision,
                .reason = std::move(reason),
                .chosenCost = toCostBreakdown(estimate, decisionCost),
                .alternatives = std::move(alternatives)}}};
    };

    if (const auto existingRecording = recordingCatalog.getRecording(recordingId); existingRecording.has_value())
    {
        const auto reuseSelection = RecordingSelection{
            .recordingId = existingRecording->id,
            .node = existingRecording->node,
            .filePath = existingRecording->filePath};
        const auto reuseCost = costModel.estimateReplayReuse(recordedChild, *worker, workerRuntimeMetrics, replaySpecification);
        const auto reuseFeasible = reuseCost.satisfiesReplayLatency;
        const auto recomputeFallbackAllowed = allowRecomputeFallback(replaySpecification);
        std::vector<RecordingSelectionAlternative> skipAlternatives = {
            buildAlternative(RecordingSelectionDecision::ReuseExistingRecording, reuseCost),
            buildAlternative(RecordingSelectionDecision::CreateNewRecording, newRecordingCost)};

        if (recomputeFallbackAllowed)
        {
            auto bestRecordingDecisionCost = std::numeric_limits<double>::infinity();
            if (reuseFeasible)
            {
                bestRecordingDecisionCost = std::min(bestRecordingDecisionCost, reuseCost.totalCost());
            }
            if (newRecordingFeasible)
            {
                bestRecordingDecisionCost = std::min(bestRecordingDecisionCost, newRecordingCost.totalCost());
            }

            if (!reuseFeasible && !newRecordingFeasible)
            {
                return buildSelectionResult(
                    std::nullopt,
                    RecordingSelectionDecision::SkipRecording,
                    "no feasible recording candidate satisfied the current budget and latency constraints; recompute fallback selected",
                    reuseCost,
                    std::move(skipAlternatives));
            }

            if (reuseCost.recomputeCost <= bestRecordingDecisionCost)
            {
                return buildSelectionResult(
                    std::nullopt,
                    RecordingSelectionDecision::SkipRecording,
                    fmt::format(
                        "recompute cost {:.2f} beat reuse_existing_recording cost {:.2f} and create_new_recording cost {:.2f}",
                        reuseCost.recomputeCost,
                        reuseCost.totalCost(),
                        newRecordingCost.totalCost()),
                    reuseCost,
                    std::move(skipAlternatives));
            }
        }

        if (reuseFeasible && (!newRecordingFeasible || reuseCost.totalCost() <= newRecordingCost.totalCost()))
        {
            std::vector<RecordingSelectionAlternative> alternatives = {
                buildAlternative(RecordingSelectionDecision::CreateNewRecording, newRecordingCost)};
            if (recomputeFallbackAllowed)
            {
                alternatives.push_back(buildAlternative(RecordingSelectionDecision::SkipRecording, reuseCost));
            }
            return buildSelectionResult(
                reuseSelection,
                RecordingSelectionDecision::ReuseExistingRecording,
                "exact-match recording fingerprint found in recording catalog and reuse_existing_recording was the lowest-cost feasible decision",
                reuseCost,
                std::move(alternatives));
        }

        if (newRecordingFeasible)
        {
            std::vector<RecordingSelectionAlternative> alternatives = {
                buildAlternative(RecordingSelectionDecision::ReuseExistingRecording, reuseCost)};
            if (recomputeFallbackAllowed)
            {
                alternatives.push_back(buildAlternative(RecordingSelectionDecision::SkipRecording, newRecordingCost));
            }

            auto store = StoreLogicalOperator(
                             StoreLogicalOperator::validateAndFormatConfig(
                                 {{"file_path", recordingFilePath}, {"append", "false"}, {"header", "true"}}))
                             .withTraitSet(newRoots.front().getTraitSet())
                             .withInferredSchema({recordedChild.getOutputSchema()})
                             .withChildren(recordedChildren);
            newRoots.front() = newRoots.front().withChildren({std::move(store)});
            placedPlan = placedPlan.withRootOperators(newRoots);

            return buildSelectionResult(
                newSelection,
                RecordingSelectionDecision::CreateNewRecording,
                "create_new_recording was the lowest-cost feasible decision for this replay request",
                newRecordingCost,
                std::move(alternatives));
        }

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

    if (allowRecomputeFallback(replaySpecification)
        && (!newRecordingFeasible || newRecordingCost.recomputeCost <= newRecordingCost.totalCost()))
    {
        return buildSelectionResult(
            std::nullopt,
            RecordingSelectionDecision::SkipRecording,
            newRecordingFeasible
                ? fmt::format(
                      "recompute cost {:.2f} beat create_new_recording cost {:.2f}",
                      newRecordingCost.recomputeCost,
                      newRecordingCost.totalCost())
                : "create_new_recording was not feasible under the current budget and latency constraints; recompute fallback selected",
            newRecordingCost,
            {buildAlternative(RecordingSelectionDecision::CreateNewRecording, newRecordingCost)});
    }

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

    std::vector<RecordingSelectionAlternative> alternatives;
    if (allowRecomputeFallback(replaySpecification))
    {
        alternatives.push_back(buildAlternative(RecordingSelectionDecision::SkipRecording, newRecordingCost));
    }

    return buildSelectionResult(
        newSelection,
        RecordingSelectionDecision::CreateNewRecording,
        "no exact-match recording fingerprint found in recording catalog",
        newRecordingCost,
        std::move(alternatives));
}
}
