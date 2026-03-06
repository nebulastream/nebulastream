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
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <LegacyOptimizer/RecordingCandidateSelectionPhase.hpp>
#include <LegacyOptimizer/RecordingFingerprint.hpp>
#include <Operators/LogicalOperator.hpp>
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

LogicalPlan createPlacedUnaryPlan(const Host& host)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan("TEST", createSchema(), {}, {});
    const auto predicate = LogicalFunction{
        EqualsLogicalFunction(LogicalFunction{FieldAccessLogicalFunction("id")}, LogicalFunction{FieldAccessLogicalFunction("id")})};
    plan = LogicalPlanBuilder::addSelection(predicate, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);
    return plan.withRootOperators({addPlacementTraitRecursively(plan.getRootOperators().front(), host)});
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
    const auto reusableRecordingId = recordingIdFromFingerprint(createRecordingFingerprint(root.getChildren().front(), host, replaySpecification));
    catalog.upsertRecording(
        RecordingEntry{
            .id = reusableRecordingId,
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/existing.bin",
            .ownerQueries = {DistributedQueryId("existing-query")}});

    const auto candidateSet = RecordingCandidateSelectionPhase(workerCatalog).apply(plan, replaySpecification, catalog);

    ASSERT_EQ(candidateSet.rootOperatorId, root.getId());
    ASSERT_EQ(candidateSet.planEdges.size(), 2U);
    ASSERT_EQ(candidateSet.candidates.size(), 2U);
    ASSERT_EQ(candidateSet.leafOperatorIds.size(), 1U);

    const auto rootCandidate = std::ranges::find_if(
        candidateSet.candidates,
        [&](const auto& candidate)
        {
            return candidate.edge.parentId == root.getId() && candidate.edge.childId == root.getChildren().front().getId();
        });
    ASSERT_NE(rootCandidate, candidateSet.candidates.end());
    EXPECT_EQ(rootCandidate->node, host);
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
}
}
