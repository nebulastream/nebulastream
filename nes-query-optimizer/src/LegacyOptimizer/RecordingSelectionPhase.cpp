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

#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <LegacyOptimizer/RecordingBoundarySolver.hpp>
#include <LegacyOptimizer/RecordingCandidateSelectionPhase.hpp>
#include <LegacyOptimizer/RecordingPlanRewriter.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>

namespace NES
{
namespace
{
std::vector<RecordingRewriteEdge> rewriteEdgesForCandidate(const RecordingBoundaryCandidate& candidate)
{
    return candidate.materializationEdges
        | std::views::transform(
              [](const auto& edge)
              {
                  return RecordingRewriteEdge{.parentId = edge.parentId, .childId = edge.childId};
              })
        | std::ranges::to<std::vector>();
}

std::string recordingRepresentationDescription(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return "binary_store";
        case RecordingRepresentation::BinaryStoreZstdFast1:
            return "binary_store_zstd_fast1";
        case RecordingRepresentation::BinaryStoreZstd:
            return "binary_store_zstd";
        case RecordingRepresentation::BinaryStoreZstdFast6:
            return "binary_store_zstd_fast6";
    }
    std::unreachable();
}

std::string buildExplanationReason(
    const SelectedRecordingBoundary& selectedBoundary,
    const size_t candidateCount,
    const double objectiveCost)
{
    const auto prefix = fmt::format(
        "selected by replay min-cut over {} candidate edges with total cut cost {:.2f}; ", candidateCount, objectiveCost);
    switch (selectedBoundary.chosenOption.decision)
    {
        case RecordingSelectionDecision::CreateNewRecording:
            return prefix + fmt::format(
                                "create_new_recording on {} with representation {} was the lowest-cost feasible option on this cut edge",
                                selectedBoundary.chosenOption.selection.node,
                                recordingRepresentationDescription(selectedBoundary.chosenOption.selection.representation));
        case RecordingSelectionDecision::UpgradeExistingRecording:
            return prefix + fmt::format(
                                "upgrade_existing_recording on {} with representation {} was selected because a structurally compatible recording exists but does not satisfy the requested retention",
                                selectedBoundary.chosenOption.selection.node,
                                recordingRepresentationDescription(selectedBoundary.chosenOption.selection.representation));
        case RecordingSelectionDecision::ReuseExistingRecording:
            return prefix + fmt::format(
                                "a structurally compatible recording on {} with representation {} already satisfies the requested replay coverage, so reuse_existing_recording was the lowest-cost feasible option on this cut edge",
                                selectedBoundary.chosenOption.selection.node,
                                recordingRepresentationDescription(selectedBoundary.chosenOption.selection.representation));
    }
    std::unreachable();
}

RecordingSelectionExplanation buildSelectionExplanation(
    const SelectedRecordingBoundary& selectedBoundary, const size_t candidateCount, const double objectiveCost)
{
    return RecordingSelectionExplanation{
        .selection = selectedBoundary.chosenOption.selection,
        .decision = selectedBoundary.chosenOption.decision,
        .reason = buildExplanationReason(selectedBoundary, candidateCount, objectiveCost),
        .chosenCost = selectedBoundary.chosenOption.cost,
        .alternatives = selectedBoundary.alternatives
            | std::views::transform(
                  [](const auto& alternative)
                  {
                      return RecordingSelectionAlternative{.decision = alternative.decision, .cost = alternative.cost};
                  })
            | std::ranges::to<std::vector>()};
}
}

RecordingSelectionResult RecordingSelectionPhase::apply(
    LogicalPlan& placedPlan,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingCatalog& recordingCatalog) const
{
    if (!replaySpecification.has_value())
    {
        return {};
    }

    const auto candidateSet = RecordingCandidateSelectionPhase(copyPtr(workerCatalog)).apply(placedPlan, replaySpecification, recordingCatalog);
    const auto boundarySelection = RecordingBoundarySolver(copyPtr(workerCatalog)).solve(candidateSet);

    RecordingSelectionsByEdge storesToInsert;
    RecordingSelectionResult selectionResult;
    selectionResult.networkExplanations.reserve(boundarySelection.selectedBoundary.size());
    selectionResult.selectedRecordings.reserve(boundarySelection.selectedBoundary.size());
    selectionResult.explanations.reserve(boundarySelection.selectedBoundary.size());
    selectionResult.incomingQueryReplayBoundaries.reserve(boundarySelection.selectedBoundary.size());
    selectionResult.activeQueryPlanRewrites = candidateSet.activeQueryPlans
        | std::views::transform(
              [](const auto& activeQueryPlan)
              {
                  return QueryRecordingPlanRewrite{
                      .queryId = activeQueryPlan.queryId,
                      .basePlan = activeQueryPlan.plan,
                      .insertions = {}};
              })
        | std::ranges::to<std::vector>();

    for (const auto& selectedBoundary : boundarySelection.selectedBoundary)
    {
        auto explanation = buildSelectionExplanation(selectedBoundary, candidateSet.candidates.size(), boundarySelection.objectiveCost);
        selectionResult.networkExplanations.push_back(explanation);
        const auto incomingQueryBoundary = QueryRecordingPlanInsertion{
            .selection = selectedBoundary.chosenOption.selection,
            .materializationEdges = rewriteEdgesForCandidate(selectedBoundary.candidate)};
        if (selectedBoundary.chosenOption.decision != RecordingSelectionDecision::ReuseExistingRecording)
        {
            for (const auto& materializationEdge : incomingQueryBoundary.materializationEdges)
            {
                storesToInsert.emplace(
                    materializationEdge,
                    selectedBoundary.chosenOption.selection);
            }
            for (const auto& activeQueryTarget : selectedBoundary.candidate.activeQueryMaterializationTargets)
            {
                const auto rewriteIt = std::ranges::find(
                    selectionResult.activeQueryPlanRewrites, activeQueryTarget.queryId, &QueryRecordingPlanRewrite::queryId);
                PRECONDITION(
                    rewriteIt != selectionResult.activeQueryPlanRewrites.end(),
                    "Replay selection expected a rewrite target for active query {}",
                    activeQueryTarget.queryId);
                rewriteIt->insertions.push_back(QueryRecordingPlanInsertion{
                    .selection = selectedBoundary.chosenOption.selection,
                    .materializationEdges =
                        activeQueryTarget.materializationEdges
                        | std::views::transform(
                              [](const auto& edge)
                              {
                                  return RecordingRewriteEdge{.parentId = edge.parentId, .childId = edge.childId};
                              })
                        | std::ranges::to<std::vector>()});
            }
        }

        if (!selectedBoundary.candidate.coversIncomingQuery)
        {
            continue;
        }
        selectionResult.selectedRecordings.push_back(selectedBoundary.chosenOption.selection);
        selectionResult.explanations.push_back(std::move(explanation));
        selectionResult.incomingQueryReplayBoundaries.push_back(incomingQueryBoundary);
        if (selectedBoundary.chosenOption.decision != RecordingSelectionDecision::ReuseExistingRecording)
        {
            if (!selectionResult.incomingQueryPlanRewrite.has_value())
            {
                selectionResult.incomingQueryPlanRewrite = QueryRecordingPlanRewrite{.queryId = {}, .basePlan = placedPlan, .insertions = {}};
            }
            selectionResult.incomingQueryPlanRewrite->insertions.push_back(selectionResult.incomingQueryReplayBoundaries.back());
        }
    }

    if (!selectionResult.incomingQueryPlanRewrite.has_value())
    {
        selectionResult.incomingQueryPlanRewrite = QueryRecordingPlanRewrite{.queryId = {}, .basePlan = placedPlan, .insertions = {}};
    }
    placedPlan = rewriteReplayBoundary(placedPlan, storesToInsert);
    return selectionResult;
}
}
