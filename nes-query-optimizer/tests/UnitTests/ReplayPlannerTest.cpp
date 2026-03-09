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
#include <LegacyOptimizer/RecordingFingerprint.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <RecordingCatalog.hpp>
#include <ReplayPlanning.hpp>
#include <Replay/ReplaySpecification.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
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

LogicalPlan createPlacedUnaryPlan(const Host& host)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan("TEST", createSchema(), {}, {});
    const auto predicate = LogicalFunction{
        EqualsLogicalFunction(LogicalFunction{FieldAccessLogicalFunction("id")}, LogicalFunction{FieldAccessLogicalFunction("id")})};
    plan = LogicalPlanBuilder::addSelection(predicate, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);
    return plan.withRootOperators({addPlacementTraitRecursively(plan.getRootOperators().front(), host)});
}

QueryRecordingPlanInsertion replayBoundaryForOperator(
    const LogicalOperator& parent, const LogicalOperator& child, const Host& host, const RecordingId& recordingId, const RecordingRepresentation representation)
{
    return QueryRecordingPlanInsertion{
        .selection =
            RecordingSelection{
                .recordingId = recordingId,
                .node = host,
                .filePath = {},
                .structuralFingerprint = createStructuralRecordingFingerprint(child, host),
                .representation = representation,
                .retentionWindowMs = std::nullopt,
                .beneficiaryQueries = {},
                .coversIncomingQuery = true},
        .materializationEdges = {RecordingRewriteEdge{.parentId = parent.getId(), .childId = child.getId()}}};
}

RecordingEntry readyRecording(
    const RecordingId& recordingId,
    const Host& host,
    std::string filePath,
    std::string structuralFingerprint,
    const RecordingRepresentation representation)
{
    return RecordingEntry{
        .id = recordingId,
        .node = host,
        .filePath = std::move(filePath),
        .structuralFingerprint = std::move(structuralFingerprint),
        .retentionWindowMs = 10'000,
        .representation = representation,
        .ownerQueries = {DistributedQueryId("replayable-query")},
        .lifecycleState = Replay::RecordingLifecycleState::Ready,
        .retainedStartWatermark = Timestamp(0),
        .retainedEndWatermark = Timestamp(10'000),
        .fillWatermark = Timestamp(10'000),
        .segmentCount = 4,
        .storageBytes = 2048,
        .successorRecordingId = std::nullopt};
}

class ReplayPlannerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ReplayPlannerTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(ReplayPlannerTest, PlannerSelectsReplayBoundaryViaReplayMinCut)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    const auto basePlan = createPlacedUnaryPlan(host);
    const auto sink = basePlan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto selection = sink.getChildren().front();
    ASSERT_EQ(selection.getChildren().size(), 1U);
    const auto source = selection.getChildren().front();

    const auto sinkBoundary = replayBoundaryForOperator(
        sink,
        selection,
        host,
        RecordingId("selection-recording"),
        RecordingRepresentation::BinaryStore);
    const auto sourceBoundary = replayBoundaryForOperator(
        selection,
        source,
        host,
        RecordingId("source-recording"),
        RecordingRepresentation::BinaryStoreZstdFast6);

    ReplayableQueryMetadata metadata{
        .originalPlan = std::nullopt,
        .globalPlan = basePlan,
        .replaySpecification = ReplaySpecification{.retentionWindowMs = 10'000, .replayLatencyLimitMs = std::nullopt},
        .selectedRecordings = {},
        .networkExplanations = {},
        .queryPlanRewrite = QueryRecordingPlanRewrite{.queryId = "replayable-query", .basePlan = basePlan, .insertions = {}},
        .replayBoundaries = {sinkBoundary, sourceBoundary}};

    RecordingCatalog catalog;
    catalog.upsertRecording(readyRecording(
        RecordingId("selection-recording"),
        host,
        "/tmp/REPLAY-NebulaStream/recordings/selection.bin",
        sinkBoundary.selection.structuralFingerprint,
        RecordingRepresentation::BinaryStore));
    catalog.upsertRecording(readyRecording(
        RecordingId("source-recording"),
        host,
        "/tmp/REPLAY-NebulaStream/recordings/source.bin",
        sourceBoundary.selection.structuralFingerprint,
        RecordingRepresentation::BinaryStoreZstdFast6));

    const auto replayPlan = ReplayPlanner(workerCatalog).plan(metadata, catalog, 1'000, 5'000);

    ASSERT_TRUE(replayPlan.has_value()) << replayPlan.error().what();
    EXPECT_THAT(replayPlan->selectedRecordingIds, testing::ElementsAre(RecordingId("selection-recording")));
    ASSERT_EQ(replayPlan->selectedReplayBoundaries.size(), 1U);
    EXPECT_EQ(
        replayPlan->selectedReplayBoundaries.front().materializationEdges,
        std::vector<RecordingRewriteEdge>({RecordingRewriteEdge{.parentId = sink.getId(), .childId = selection.getId()}}));
    EXPECT_THAT(replayPlan->queryPlanRewrite.insertions, testing::ElementsAre(replayPlan->selectedReplayBoundaries.front()));
    ASSERT_EQ(replayPlan->explanations.size(), 1U);
    EXPECT_NE(replayPlan->explanations.front().reason.find("selected by replay min-cut"), std::string::npos);
}

TEST_F(ReplayPlannerTest, PlannerRejectsReplayBoundaryWithoutRetainedCoverage)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    const auto basePlan = createPlacedUnaryPlan(host);
    const auto sink = basePlan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto selection = sink.getChildren().front();

    const auto replayBoundary = replayBoundaryForOperator(
        sink,
        selection,
        host,
        RecordingId("selection-recording"),
        RecordingRepresentation::BinaryStore);

    ReplayableQueryMetadata metadata{
        .originalPlan = std::nullopt,
        .globalPlan = basePlan,
        .replaySpecification = ReplaySpecification{.retentionWindowMs = 10'000, .replayLatencyLimitMs = std::nullopt},
        .selectedRecordings = {},
        .networkExplanations = {},
        .queryPlanRewrite = QueryRecordingPlanRewrite{.queryId = "replayable-query", .basePlan = basePlan, .insertions = {}},
        .replayBoundaries = {replayBoundary}};

    RecordingCatalog catalog;
    auto recording = readyRecording(
        RecordingId("selection-recording"),
        host,
        "/tmp/REPLAY-NebulaStream/recordings/selection.bin",
        replayBoundary.selection.structuralFingerprint,
        RecordingRepresentation::BinaryStore);
    recording.retainedStartWatermark = Timestamp(2'000);
    catalog.upsertRecording(std::move(recording));

    const auto replayPlan = ReplayPlanner(workerCatalog).plan(metadata, catalog, 1'000, 5'000);

    ASSERT_FALSE(replayPlan.has_value());
    EXPECT_NE(std::string(replayPlan.error().what()).find("does not cover replay start"), std::string::npos);
}
}
}
