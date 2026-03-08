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


#include <LegacyOptimizer.hpp>

#include <algorithm>
#include <ranges>
#include <string_view>

#include <LegacyOptimizer/BottomUpPlacement.hpp>
#include <LegacyOptimizer/InlineSinkBindingPhase.hpp>
#include <LegacyOptimizer/InlineSourceBindingPhase.hpp>
#include <LegacyOptimizer/LogicalSourceExpansionRule.hpp>
#include <LegacyOptimizer/OriginIdInferencePhase.hpp>
#include <LegacyOptimizer/RecordingPlanRewriter.hpp>
#include <LegacyOptimizer/QueryDecomposition.hpp>
#include <LegacyOptimizer/RecordingSelectionPhase.hpp>
#include <LegacyOptimizer/RedundantProjectionRemovalRule.hpp>
#include <LegacyOptimizer/RedundantUnionRemovalRule.hpp>
#include <LegacyOptimizer/SinkBindingRule.hpp>
#include <LegacyOptimizer/SourceInferencePhase.hpp>
#include <LegacyOptimizer/TypeInferencePhase.hpp>
#include <DistributedQuery.hpp>

namespace NES
{
namespace
{
bool selectionAffectsActiveQuery(const RecordingSelection& selection, const std::string_view queryId)
{
    return std::ranges::contains(selection.beneficiaryQueries, queryId);
}

RecordingSelectionResult selectionResultForActiveQuery(const RecordingSelectionResult& mergedSelectionResult, const std::string_view queryId)
{
    RecordingSelectionResult querySelectionResult;
    for (const auto& explanation : mergedSelectionResult.networkExplanations)
    {
        if (!selectionAffectsActiveQuery(explanation.selection, queryId))
        {
            continue;
        }

        auto queryLocalExplanation = explanation;
        queryLocalExplanation.selection.beneficiaryQueries.clear();
        queryLocalExplanation.selection.coversIncomingQuery = true;
        querySelectionResult.networkExplanations.push_back(queryLocalExplanation);
        if (!std::ranges::contains(
                querySelectionResult.selectedRecordings,
                queryLocalExplanation.selection.recordingId,
                &RecordingSelection::recordingId))
        {
            querySelectionResult.selectedRecordings.push_back(queryLocalExplanation.selection);
        }
    }
    querySelectionResult.explanations = querySelectionResult.networkExplanations;
    querySelectionResult.activeQueryPlanRewrites = mergedSelectionResult.activeQueryPlanRewrites
        | std::views::filter([&](const auto& rewrite) { return rewrite.queryId == queryId; })
        | std::ranges::to<std::vector>();
    return querySelectionResult;
}

const QueryRecordingPlanRewrite* queryPlanRewriteForActiveQuery(
    const RecordingSelectionResult& mergedSelectionResult, const std::string_view queryId)
{
    const auto rewriteIt = std::ranges::find(mergedSelectionResult.activeQueryPlanRewrites, queryId, &QueryRecordingPlanRewrite::queryId);
    return rewriteIt == mergedSelectionResult.activeQueryPlanRewrites.end() ? nullptr : std::addressof(*rewriteIt);
}

RecordingSelectionsByEdge storesToInsertForActiveQuery(const RecordingSelectionResult& mergedSelectionResult, const std::string_view queryId)
{
    RecordingSelectionsByEdge storesToInsert;
    const auto* queryPlanRewrite = queryPlanRewriteForActiveQuery(mergedSelectionResult, queryId);
    if (queryPlanRewrite == nullptr)
    {
        return storesToInsert;
    }
    for (const auto& insertion : queryPlanRewrite->insertions)
    {
        for (const auto& edge : insertion.materializationEdges)
        {
            storesToInsert.emplace(edge, insertion.selection);
        }
    }
    return storesToInsert;
}
}

DistributedLogicalPlan LegacyOptimizer::optimize(const LogicalPlan& plan) const
{
    return optimize(plan, std::nullopt, RecordingCatalog{});
}

DistributedLogicalPlan LegacyOptimizer::optimize(
    const LogicalPlan& plan, const std::optional<ReplaySpecification>& replaySpecification, const RecordingCatalog& recordingCatalog) const
{
    auto newPlan = LogicalPlan{plan};
    const auto sinkBindingRule = SinkBindingRule{sinkCatalog};
    const auto inlineSinkBindingPhase = InlineSinkBindingPhase{sinkCatalog};
    const auto inlineSourceBindingPhase = InlineSourceBindingPhase{sourceCatalog};
    const auto sourceInference = SourceInferencePhase{sourceCatalog};
    const auto logicalSourceExpansionRule = LogicalSourceExpansionRule{sourceCatalog};
    constexpr auto typeInference = TypeInferencePhase{};
    constexpr auto originIdInferencePhase = OriginIdInferencePhase{};
    constexpr auto redundantUnionRemovalRule = RedundantUnionRemovalRule{};
    constexpr auto redundantProjectionRemovalRule = RedundantProjectionRemovalRule{};

    inlineSinkBindingPhase.apply(newPlan);
    sinkBindingRule.apply(newPlan);
    inlineSourceBindingPhase.apply(newPlan);
    sourceInference.apply(newPlan);
    logicalSourceExpansionRule.apply(newPlan);
    NES_INFO("After Source Expansion:\n{}", newPlan);
    redundantUnionRemovalRule.apply(newPlan);
    NES_INFO("After Redundant Union Removal:\n{}", newPlan);
    typeInference.apply(newPlan);

    redundantProjectionRemovalRule.apply(newPlan);
    NES_INFO("After Redundant Projection Removal:\n{}", newPlan);

    originIdInferencePhase.apply(newPlan);
    typeInference.apply(newPlan);


    BottomUpOperatorPlacer(workerCatalog).apply(newPlan);
    const auto recordingSelectionResult = RecordingSelectionPhase(workerCatalog).apply(newPlan, replaySpecification, recordingCatalog);
    auto distributedPlan = QueryDecomposer(workerCatalog, sourceCatalog, sinkCatalog).decompose(newPlan);
    distributedPlan.setReplaySpecification(replaySpecification);
    distributedPlan.setRecordingSelectionResult(recordingSelectionResult);
    return distributedPlan;
}

DistributedLogicalPlan LegacyOptimizer::redeployWithFixedSelection(
    const LogicalPlan& globalPlan,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingSelectionResult& mergedSelectionResult,
    const std::string_view queryId) const
{
    static_cast<void>(globalPlan);
    const auto* queryPlanRewrite = queryPlanRewriteForActiveQuery(mergedSelectionResult, queryId);
    PRECONDITION(queryPlanRewrite != nullptr, "Replay redeployment requires a selected base plan for active query {}", queryId);
    auto rewrittenPlan = rewriteReplayBoundary(queryPlanRewrite->basePlan, storesToInsertForActiveQuery(mergedSelectionResult, queryId));

    auto distributedPlan = QueryDecomposer(workerCatalog, sourceCatalog, sinkCatalog).decompose(rewrittenPlan);
    distributedPlan.setReplaySpecification(replaySpecification);
    distributedPlan.setRecordingSelectionResult(selectionResultForActiveQuery(mergedSelectionResult, queryId));
    return distributedPlan;
}

DistributedLogicalPlan LegacyOptimizer::buildRecordingEpochWithFixedSelection(
    const LogicalPlan& globalPlan,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingSelectionResult& mergedSelectionResult,
    const std::string_view queryId) const
{
    static_cast<void>(globalPlan);
    const auto* queryPlanRewrite = queryPlanRewriteForActiveQuery(mergedSelectionResult, queryId);
    PRECONDITION(queryPlanRewrite != nullptr, "Replay epoch rollout requires a selected base plan for active query {}", queryId);
    auto rewrittenPlan = rewriteReplayBoundary(queryPlanRewrite->basePlan, storesToInsertForActiveQuery(mergedSelectionResult, queryId));
    rewrittenPlan = replaceSinksWithVoid(rewrittenPlan);

    auto distributedPlan = QueryDecomposer(workerCatalog, sourceCatalog, sinkCatalog).decompose(rewrittenPlan);
    distributedPlan.setReplaySpecification(replaySpecification);
    distributedPlan.setRecordingSelectionResult(selectionResultForActiveQuery(mergedSelectionResult, queryId));
    return distributedPlan;
}
}
