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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <LegacyOptimizer/RecordingBoundarySolver.hpp>
#include <LegacyOptimizer/RecordingSelectionTypes.hpp>
#include <RecordingSelectionResult.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{
namespace
{
RecordingCandidateOption makeNewRecordingOption(std::string recordingId, double cost)
{
    return RecordingCandidateOption{
        .decision = RecordingSelectionDecision::CreateNewRecording,
        .selection = RecordingSelection{.recordingId = RecordingId(std::move(recordingId)), .node = Host("worker-1:8080"), .filePath = {}},
        .cost = RecordingCostBreakdown{.decisionCost = cost, .estimatedStorageBytes = 1024, .fitsBudget = true, .satisfiesReplayLatency = true},
        .feasible = true,
        .infeasibilityReason = {}};
}

class RecordingBoundarySolverTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("RecordingBoundarySolverTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(RecordingBoundarySolverTest, SolverConsumesCandidateSetAndReturnsMinimumCut)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    const auto rootId = OperatorId(1);
    const auto leftId = OperatorId(2);
    const auto leftLeafId = OperatorId(3);
    const auto rightLeafId = OperatorId(4);

    RecordingCandidateSet candidateSet;
    candidateSet.rootOperatorId = rootId;
    candidateSet.leafOperatorIds = {leftLeafId, rightLeafId};
    candidateSet.planEdges = {
        RecordingPlanEdge{.parentId = rootId, .childId = leftId},
        RecordingPlanEdge{.parentId = leftId, .childId = leftLeafId},
        RecordingPlanEdge{.parentId = rootId, .childId = rightLeafId}};
    candidateSet.candidates = {
        RecordingBoundaryCandidate{
            .edge = {.parentId = rootId, .childId = leftId},
            .node = host,
            .options = {makeNewRecordingOption("root-left", 10.0)}},
        RecordingBoundaryCandidate{
            .edge = {.parentId = leftId, .childId = leftLeafId},
            .node = host,
            .options = {makeNewRecordingOption("left-leaf", 2.0)}},
        RecordingBoundaryCandidate{
            .edge = {.parentId = rootId, .childId = rightLeafId},
            .node = host,
            .options = {makeNewRecordingOption("root-right", 3.0)}}};

    const auto selection = RecordingBoundarySolver(workerCatalog).solve(candidateSet);

    ASSERT_EQ(selection.selectedBoundary.size(), 2U);
    EXPECT_DOUBLE_EQ(selection.objectiveCost, 5.0);
    EXPECT_THAT(
        selection.selectedBoundary
            | std::views::transform(
                  [](const auto& selected)
                  {
                      return std::pair{
                          selected.candidate.edge.parentId.getRawValue(),
                          selected.candidate.edge.childId.getRawValue()};
                  })
            | std::ranges::to<std::vector>(),
        ::testing::UnorderedElementsAre(std::pair{leftId.getRawValue(), leftLeafId.getRawValue()}, std::pair{rootId.getRawValue(), rightLeafId.getRawValue()}));
}
}
}
