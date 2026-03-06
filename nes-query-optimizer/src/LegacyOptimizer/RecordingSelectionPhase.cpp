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

#include <cstdint>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <LegacyOptimizer/RecordingBoundarySolver.hpp>
#include <LegacyOptimizer/RecordingCandidateSelectionPhase.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>

namespace NES
{
namespace
{
struct RecordingPlanEdgeHash
{
    [[nodiscard]] size_t operator()(const RecordingPlanEdge& edge) const
    {
        return std::hash<uint64_t>{}(edge.parentId.getRawValue()) ^ (std::hash<uint64_t>{}(edge.childId.getRawValue()) << 1U);
    }
};

LogicalOperator rewriteSelectedBoundary(
    const LogicalOperator& current,
    const std::unordered_map<RecordingPlanEdge, RecordingSelection, RecordingPlanEdgeHash>& storesToInsert)
{
    const auto originalChildren = current.getChildren();
    std::vector<LogicalOperator> rewrittenChildren;
    rewrittenChildren.reserve(originalChildren.size());
    for (const auto& child : originalChildren)
    {
        const RecordingPlanEdge edge{.parentId = current.getId(), .childId = child.getId()};
        auto rewrittenChild = rewriteSelectedBoundary(child, storesToInsert);
        if (!storesToInsert.contains(edge))
        {
            rewrittenChildren.push_back(std::move(rewrittenChild));
            continue;
        }

        const auto& selection = storesToInsert.at(edge);
        rewrittenChildren.push_back(
            StoreLogicalOperator(
                StoreLogicalOperator::validateAndFormatConfig(
                    {{"file_path", selection.filePath}, {"append", "false"}, {"header", "true"}}))
                .withTraitSet(current.getTraitSet())
                .withInferredSchema({rewrittenChild.getOutputSchema()})
                .withChildren({rewrittenChild}));
    }
    return current.withChildren(std::move(rewrittenChildren));
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
            return prefix + "create_new_recording was the lowest-cost feasible option on this cut edge";
        case RecordingSelectionDecision::ReuseExistingRecording:
            return prefix + "exact-match recording fingerprint found in recording catalog and reuse_existing_recording was the lowest-cost feasible option on this cut edge";
    }
    std::unreachable();
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

    std::unordered_map<RecordingPlanEdge, RecordingSelection, RecordingPlanEdgeHash> storesToInsert;
    RecordingSelectionResult selectionResult;
    selectionResult.selectedRecordings.reserve(boundarySelection.selectedBoundary.size());
    selectionResult.explanations.reserve(boundarySelection.selectedBoundary.size());

    for (const auto& selectedBoundary : boundarySelection.selectedBoundary)
    {
        if (selectedBoundary.chosenOption.decision == RecordingSelectionDecision::CreateNewRecording)
        {
            storesToInsert.emplace(selectedBoundary.candidate.edge, selectedBoundary.chosenOption.selection);
        }

        selectionResult.selectedRecordings.push_back(selectedBoundary.chosenOption.selection);
        selectionResult.explanations.push_back(RecordingSelectionExplanation{
            .selection = selectedBoundary.chosenOption.selection,
            .decision = selectedBoundary.chosenOption.decision,
            .reason = buildExplanationReason(selectedBoundary, candidateSet.candidates.size(), boundarySelection.objectiveCost),
            .chosenCost = selectedBoundary.chosenOption.cost,
            .alternatives = selectedBoundary.alternatives
                | std::views::transform(
                      [](const auto& alternative)
                      {
                          return RecordingSelectionAlternative{.decision = alternative.decision, .cost = alternative.cost};
                      })
                | std::ranges::to<std::vector>()});
    }

    auto rewrittenRoots = placedPlan.getRootOperators();
    for (auto& root : rewrittenRoots)
    {
        root = rewriteSelectedBoundary(root, storesToInsert);
    }
    placedPlan = placedPlan.withRootOperators(rewrittenRoots);
    return selectionResult;
}
}
