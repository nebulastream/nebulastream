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
#include <Replay/BinaryStoreFormat.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
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

std::string recordingRepresentationDescription(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return "binary_store";
        case RecordingRepresentation::BinaryStoreZstd:
            return "binary_store_zstd";
    }
    std::unreachable();
}

std::unordered_map<std::string, std::string> storeConfigForSelection(const RecordingSelection& selection)
{
    std::unordered_map<std::string, std::string> config{
        {"file_path", selection.filePath},
        {"append", "false"},
        {"header", "true"}};
    switch (selection.representation)
    {
        case RecordingRepresentation::BinaryStore:
            return config;
        case RecordingRepresentation::BinaryStoreZstd:
            config.emplace("compression", "Zstd");
            config.emplace("compression_level", std::to_string(Replay::DEFAULT_BINARY_STORE_ZSTD_COMPRESSION_LEVEL));
            return config;
    }
    std::unreachable();
}

TraitSet traitSetWithPlacement(const TraitSet& original, const Host& placement)
{
    std::vector<Trait> traits;
    traits.reserve(original.size());
    for (const auto& [_, trait] : original)
    {
        if (trait.getTypeInfo() == typeid(PlacementTrait))
        {
            continue;
        }
        traits.push_back(trait);
    }
    auto rewrittenTraitSet = TraitSet{traits};
    const auto addedPlacement = rewrittenTraitSet.tryInsert(PlacementTrait(placement));
    INVARIANT(addedPlacement, "Expected to inject placement trait for replay store on {}", placement);
    return rewrittenTraitSet;
}

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
            StoreLogicalOperator(StoreLogicalOperator::validateAndFormatConfig(storeConfigForSelection(selection)))
                .withTraitSet(traitSetWithPlacement(current.getTraitSet(), selection.node))
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

    std::unordered_map<RecordingPlanEdge, RecordingSelection, RecordingPlanEdgeHash> storesToInsert;
    RecordingSelectionResult selectionResult;
    selectionResult.networkExplanations.reserve(boundarySelection.selectedBoundary.size());
    selectionResult.selectedRecordings.reserve(boundarySelection.selectedBoundary.size());
    selectionResult.explanations.reserve(boundarySelection.selectedBoundary.size());

    for (const auto& selectedBoundary : boundarySelection.selectedBoundary)
    {
        auto explanation = buildSelectionExplanation(selectedBoundary, candidateSet.candidates.size(), boundarySelection.objectiveCost);
        selectionResult.networkExplanations.push_back(explanation);
        if (!selectedBoundary.candidate.coversIncomingQuery)
        {
            continue;
        }
        if (selectedBoundary.chosenOption.decision != RecordingSelectionDecision::ReuseExistingRecording)
        {
            for (const auto& materializationEdge : selectedBoundary.candidate.materializationEdges)
            {
                storesToInsert.emplace(materializationEdge, selectedBoundary.chosenOption.selection);
            }
        }

        selectionResult.selectedRecordings.push_back(selectedBoundary.chosenOption.selection);
        selectionResult.explanations.push_back(std::move(explanation));
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
