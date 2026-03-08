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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <LegacyOptimizer/RecordingCandidateSelectionPhase.hpp>
#include <LegacyOptimizer/RecordingFingerprint.hpp>
#include <LegacyOptimizer/RecordingSelectionPhase.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <RecordingCatalog.hpp>
#include <Replay/ReplaySpecification.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{
namespace
{
LogicalOperator addPlacementTraitRecursively(const LogicalOperator& current, const Host& host)
{
    auto traitSet = current.getTraitSet();
    EXPECT_TRUE(traitSet.tryInsert(PlacementTrait(host)));
    auto children = current.getChildren();
    for (auto& child : children)
    {
        child = addPlacementTraitRecursively(child, host);
    }
    return current.withTraitSet(traitSet).withChildren(children);
}

Schema createSchema()
{
    Schema schema;
    schema.addField("id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
    schema.addField("value", DataTypeProvider::provideDataType(DataType::Type::UINT64));
    return schema;
}

LogicalPlan createPlacedUnaryPlan(const std::string& sourceName, const Host& host)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan(sourceName, createSchema(), {}, {});
    const auto predicate = LogicalFunction{
        EqualsLogicalFunction(LogicalFunction{FieldAccessLogicalFunction("id")}, LogicalFunction{FieldAccessLogicalFunction("id")})};
    plan = LogicalPlanBuilder::addSelection(predicate, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);
    return plan.withRootOperators({addPlacementTraitRecursively(plan.getRootOperators().front(), host)});
}

LogicalPlan createPlacedUnaryPlan(const Host& host)
{
    return createPlacedUnaryPlan("TEST", host);
}

LogicalOperator withPlacement(const LogicalOperator& current, const Host& host)
{
    auto traitSet = current.getTraitSet();
    EXPECT_TRUE(traitSet.tryInsert(PlacementTrait(host)));
    return current.withTraitSet(traitSet);
}

LogicalPlan createPlacedSourceToSinkPlan(const Host& sourceHost, const Host& sinkHost)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan("TEST", createSchema(), {}, {});
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    auto sink = plan.getRootOperators().front();
    EXPECT_EQ(sink.getChildren().size(), 1U);
    if (sink.getChildren().size() != 1U)
    {
        return plan;
    }
    auto source = sink.getChildren().front();
    source = withPlacement(source, sourceHost);
    sink = withPlacement(sink, sinkHost).withChildren({source});
    return plan.withRootOperators({sink});
}

class RecordingSelectionPhaseTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("RecordingSelectionPhaseTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(RecordingSelectionPhaseTest, CandidatePhaseReturnsPlacedEdgeSetAndReuseOption)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    const auto plan = createPlacedUnaryPlan(host);
    const auto root = plan.getRootOperators().front();
    ASSERT_EQ(root.getChildren().size(), 1U);
    ReplaySpecification replaySpecification;
    replaySpecification.retentionWindowMs = 60'000;

    RecordingCatalog catalog;
    const auto structuralFingerprint = createStructuralRecordingFingerprint(root.getChildren().front(), host);
    const auto reusableRecordingId = recordingIdFromFingerprint(createRecordingFingerprint(root.getChildren().front(), host, replaySpecification));
    catalog.upsertRecording(
        RecordingEntry{
            .id = reusableRecordingId,
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/existing.bin",
            .structuralFingerprint = structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .ownerQueries = {DistributedQueryId("existing-query")}});

    const auto candidateSet = RecordingCandidateSelectionPhase(workerCatalog).apply(plan, replaySpecification, catalog);

    EXPECT_NE(candidateSet.rootOperatorId, INVALID_OPERATOR_ID);
    ASSERT_EQ(candidateSet.planEdges.size(), 2U);
    ASSERT_EQ(candidateSet.candidates.size(), 2U);
    ASSERT_EQ(candidateSet.leafOperatorIds.size(), 1U);

    const auto rootCandidate = std::ranges::find_if(
        candidateSet.candidates,
        [&](const auto& candidate)
        {
            return std::ranges::any_of(
                candidate.options,
                [&](const auto& option)
                {
                    return option.decision == RecordingSelectionDecision::ReuseExistingRecording && option.feasible
                        && option.selection.recordingId == reusableRecordingId;
                });
        });
    ASSERT_NE(rootCandidate, candidateSet.candidates.end());
    EXPECT_EQ(rootCandidate->upstreamNode, host);
    EXPECT_EQ(rootCandidate->downstreamNode, host);
    EXPECT_EQ(rootCandidate->routeNodes, std::vector<Host>({host}));
    ASSERT_EQ(rootCandidate->options.size(), 2U);
    EXPECT_TRUE(std::ranges::any_of(
        rootCandidate->options,
        [](const auto& option) { return option.decision == RecordingSelectionDecision::CreateNewRecording && option.feasible; }));
    EXPECT_TRUE(std::ranges::any_of(
        rootCandidate->options,
        [&](const auto& option)
        {
            return option.decision == RecordingSelectionDecision::ReuseExistingRecording && option.feasible
                && option.selection.recordingId == reusableRecordingId;
        }));
}

TEST_F(RecordingSelectionPhaseTest, CandidatePhaseReturnsUpgradeOptionForWeakerRetentionRecording)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    const auto plan = createPlacedUnaryPlan(host);
    const auto root = plan.getRootOperators().front();
    ASSERT_EQ(root.getChildren().size(), 1U);

    ReplaySpecification requestedReplaySpecification;
    requestedReplaySpecification.retentionWindowMs = 300'000;
    ReplaySpecification existingReplaySpecification;
    existingReplaySpecification.retentionWindowMs = 60'000;

    RecordingCatalog catalog;
    const auto structuralFingerprint = createStructuralRecordingFingerprint(root.getChildren().front(), host);
    const auto weakerRecordingId
        = recordingIdFromFingerprint(createRecordingFingerprint(root.getChildren().front(), host, existingReplaySpecification));
    catalog.upsertRecording(
        RecordingEntry{
            .id = weakerRecordingId,
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/weaker.bin",
            .structuralFingerprint = structuralFingerprint,
            .retentionWindowMs = existingReplaySpecification.retentionWindowMs,
            .ownerQueries = {DistributedQueryId("existing-query")}});

    const auto candidateSet = RecordingCandidateSelectionPhase(workerCatalog).apply(plan, requestedReplaySpecification, catalog);
    const auto rootCandidate = std::ranges::find_if(
        candidateSet.candidates,
        [&](const auto& candidate)
        {
            return std::ranges::any_of(
                candidate.options,
                [&](const auto& option)
                {
                    return option.decision == RecordingSelectionDecision::UpgradeExistingRecording && option.feasible
                        && option.selection.structuralFingerprint == structuralFingerprint && option.selection.recordingId != weakerRecordingId;
                });
        });
    ASSERT_NE(rootCandidate, candidateSet.candidates.end());

    EXPECT_TRUE(std::ranges::any_of(
        rootCandidate->options,
        [](const auto& option) { return option.decision == RecordingSelectionDecision::CreateNewRecording && option.feasible; }));
    EXPECT_FALSE(std::ranges::any_of(
        rootCandidate->options,
        [](const auto& option) { return option.decision == RecordingSelectionDecision::ReuseExistingRecording; }));
    EXPECT_TRUE(std::ranges::any_of(
        rootCandidate->options,
        [&](const auto& option)
        {
            return option.decision == RecordingSelectionDecision::UpgradeExistingRecording && option.feasible
                && option.selection.structuralFingerprint == structuralFingerprint && option.selection.recordingId != weakerRecordingId;
        }));
}

TEST_F(RecordingSelectionPhaseTest, CandidatePhaseEnumeratesAllRoutePlacementsForPlacedEdge)
{
    const auto sourceHost = Host("source-node:8080");
    const auto intermediateHost = Host("intermediate-node:8080");
    const auto sinkHost = Host("sink-node:8080");

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(sourceHost, "", 16, {intermediateHost}, {}, 1024 * 1024));
    ASSERT_TRUE(workerCatalog->addWorker(intermediateHost, "", 16, {sinkHost}, {}, 1024 * 1024));
    ASSERT_TRUE(workerCatalog->addWorker(sinkHost, "", 16, {}, {}, 1024 * 1024));

    const auto plan = createPlacedSourceToSinkPlan(sourceHost, sinkHost);
    const auto root = plan.getRootOperators().front();
    ASSERT_EQ(root.getChildren().size(), 1U);

    const auto candidateSet = RecordingCandidateSelectionPhase(workerCatalog).apply(
        plan,
        ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt},
        RecordingCatalog{});

    ASSERT_EQ(candidateSet.candidates.size(), 1U);
    const auto& candidate = candidateSet.candidates.front();
    EXPECT_EQ(candidate.upstreamNode, sourceHost);
    EXPECT_EQ(candidate.downstreamNode, sinkHost);
    EXPECT_EQ(candidate.routeNodes, std::vector<Host>({sourceHost, intermediateHost, sinkHost}));
    ASSERT_EQ(candidate.options.size(), 3U);
    EXPECT_EQ(
        candidate.options
            | std::views::transform([](const auto& option) { return option.selection.node; })
            | std::ranges::to<std::vector>(),
        std::vector<Host>({sourceHost, intermediateHost, sinkHost}));
    EXPECT_TRUE(std::ranges::all_of(
        candidate.options,
        [](const auto& option) { return option.decision == RecordingSelectionDecision::CreateNewRecording && option.feasible; }));
}

TEST_F(RecordingSelectionPhaseTest, SelectionResultExposesAllMergedNetworkDecisions)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    auto incomingPlan = createPlacedUnaryPlan("INCOMING_SOURCE", host);
    const auto activePlan = createPlacedUnaryPlan("ACTIVE_SOURCE", host);
    const ReplaySpecification replaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt};

    RecordingCatalog catalog;
    const auto activeRoot = activePlan.getRootOperators().front();
    ASSERT_EQ(activeRoot.getChildren().size(), 1U);
    const auto activeSubplanRoot = activeRoot.getChildren().front();
    const auto activeRecordingId = recordingIdFromFingerprint(createRecordingFingerprint(activeSubplanRoot, host, replaySpecification));
    catalog.upsertRecording(
        RecordingEntry{
            .id = activeRecordingId,
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/active-existing.bin",
            .structuralFingerprint = createStructuralRecordingFingerprint(activeSubplanRoot, host),
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .ownerQueries = {DistributedQueryId("active-query")}});
    catalog.upsertQueryMetadata(
        DistributedQueryId("active-query"),
        ReplayableQueryMetadata{
            .globalPlan = activePlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {},
            .networkExplanations = {}});

    const auto selectionResult = RecordingSelectionPhase(workerCatalog).apply(incomingPlan, replaySpecification, catalog);

    ASSERT_EQ(selectionResult.selectedRecordings.size(), 1U);
    ASSERT_EQ(selectionResult.explanations.size(), 1U);
    ASSERT_EQ(selectionResult.networkExplanations.size(), 2U);
    EXPECT_EQ(
        std::ranges::count_if(
            selectionResult.networkExplanations,
            [](const auto& explanation) { return explanation.selection.coversIncomingQuery; }),
        1);
    EXPECT_EQ(
        std::ranges::count_if(
            selectionResult.networkExplanations,
            [](const auto& explanation) { return !explanation.selection.coversIncomingQuery; }),
        1);
    const auto activeOnlySelection = std::ranges::find_if(
        selectionResult.networkExplanations,
        [](const auto& explanation) { return !explanation.selection.coversIncomingQuery; });
    ASSERT_NE(activeOnlySelection, selectionResult.networkExplanations.end());
    EXPECT_EQ(activeOnlySelection->selection.beneficiaryQueries, std::vector<std::string>({"active-query"}));
    EXPECT_EQ(activeOnlySelection->decision, RecordingSelectionDecision::ReuseExistingRecording);
    EXPECT_EQ(activeOnlySelection->selection.recordingId, activeRecordingId);
    const auto incomingSelection = std::ranges::find_if(
        selectionResult.networkExplanations,
        [](const auto& explanation) { return explanation.selection.coversIncomingQuery; });
    ASSERT_NE(incomingSelection, selectionResult.networkExplanations.end());
    EXPECT_EQ(incomingSelection->decision, RecordingSelectionDecision::CreateNewRecording);
    EXPECT_EQ(getOperatorByType<StoreLogicalOperator>(incomingPlan).size(), 1U);
}
}
}
