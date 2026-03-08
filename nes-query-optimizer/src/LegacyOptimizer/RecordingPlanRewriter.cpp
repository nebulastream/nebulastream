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

#include <LegacyOptimizer/RecordingPlanRewriter.hpp>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Replay/BinaryStoreFormat.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
std::unordered_map<std::string, std::string> storeConfigForSelection(const RecordingSelection& selection)
{
    std::unordered_map<std::string, std::string> config{
        {"file_path", selection.filePath},
        {"append", "false"},
        {"header", "true"}};
    if (selection.retentionWindowMs.has_value())
    {
        config.emplace("retention_window_ms", std::to_string(*selection.retentionWindowMs));
    }
    switch (selection.representation)
    {
        case RecordingRepresentation::BinaryStore:
            return config;
        case RecordingRepresentation::BinaryStoreZstdFast1:
            config.emplace("compression", "Zstd");
            config.emplace("compression_level", std::to_string(Replay::BINARY_STORE_ZSTD_FAST1_COMPRESSION_LEVEL));
            return config;
        case RecordingRepresentation::BinaryStoreZstd:
            config.emplace("compression", "Zstd");
            config.emplace("compression_level", std::to_string(Replay::DEFAULT_BINARY_STORE_ZSTD_COMPRESSION_LEVEL));
            return config;
        case RecordingRepresentation::BinaryStoreZstdFast6:
            config.emplace("compression", "Zstd");
            config.emplace("compression_level", std::to_string(Replay::BINARY_STORE_ZSTD_FAST6_COMPRESSION_LEVEL));
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

LogicalOperator stripReplayStores(const LogicalOperator& current)
{
    if (current.tryGetAs<StoreLogicalOperator>().has_value())
    {
        const auto children = current.getChildren();
        PRECONDITION(children.size() == 1, "Replay plan rewriting expected store operators to have a single child");
        return stripReplayStores(children.front());
    }

    auto strippedChildren = current.getChildren();
    for (auto& child : strippedChildren)
    {
        child = stripReplayStores(child);
    }
    return current.withChildren(std::move(strippedChildren));
}

bool hasReplayWatermarkAssignerOnCurrentPath(const LogicalOperator& current, const bool hasReplayWatermarkAssignerAncestor)
{
    return hasReplayWatermarkAssignerAncestor || current.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>().has_value()
        || current.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value();
}

LogicalOperator makeReplayStore(
    const LogicalOperator& current,
    LogicalOperator rewrittenChild,
    const RecordingSelection& selection,
    const bool hasReplayWatermarkAssignerOnPath)
{
    const auto placementTraitSet = traitSetWithPlacement(current.getTraitSet(), selection.node);
    auto replayStore = StoreLogicalOperator(StoreLogicalOperator::validateAndFormatConfig(storeConfigForSelection(selection)))
                           .withTraitSet(placementTraitSet)
                           .withInferredSchema({rewrittenChild.getOutputSchema()})
                           .withChildren({std::move(rewrittenChild)});
    if (hasReplayWatermarkAssignerOnPath)
    {
        return replayStore;
    }

    return IngestionTimeWatermarkAssignerLogicalOperator()
        .withTraitSet(placementTraitSet)
        .withInferredSchema({replayStore.getOutputSchema()})
        .withChildren({replayStore});
}

LogicalOperator rewriteReplayBoundary(
    const LogicalOperator& current,
    const RecordingSelectionsByEdge& storesToInsert,
    const bool hasReplayWatermarkAssignerAncestor)
{
    const auto hasReplayWatermarkAssignerOnPath = hasReplayWatermarkAssignerOnCurrentPath(current, hasReplayWatermarkAssignerAncestor);
    const auto originalChildren = current.getChildren();
    std::vector<LogicalOperator> rewrittenChildren;
    rewrittenChildren.reserve(originalChildren.size());
    for (const auto& child : originalChildren)
    {
        const RecordingRewriteEdge edge{.parentId = current.getId(), .childId = child.getId()};
        auto rewrittenChild = rewriteReplayBoundary(child, storesToInsert, hasReplayWatermarkAssignerOnPath);
        if (!storesToInsert.contains(edge))
        {
            rewrittenChildren.push_back(std::move(rewrittenChild));
            continue;
        }

        const auto& selection = storesToInsert.at(edge);
        rewrittenChildren.push_back(makeReplayStore(
            current, std::move(rewrittenChild), selection, hasReplayWatermarkAssignerOnPath));
    }
    return current.withChildren(std::move(rewrittenChildren));
}
}

LogicalPlan stripReplayStores(const LogicalPlan& plan)
{
    auto rewrittenRoots = plan.getRootOperators();
    for (auto& root : rewrittenRoots)
    {
        root = stripReplayStores(root);
    }
    return plan.withRootOperators(std::move(rewrittenRoots));
}

LogicalPlan rewriteReplayBoundary(const LogicalPlan& plan, const RecordingSelectionsByEdge& storesToInsert)
{
    auto rewrittenRoots = plan.getRootOperators();
    for (auto& root : rewrittenRoots)
    {
        root = rewriteReplayBoundary(root, storesToInsert, false);
    }
    return plan.withRootOperators(std::move(rewrittenRoots));
}
}
