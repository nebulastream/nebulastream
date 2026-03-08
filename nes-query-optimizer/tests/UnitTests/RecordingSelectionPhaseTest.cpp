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

#include <array>
#include <chrono>
#include <cmath>
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
#include <LegacyOptimizer/RecordingPlanRewriter.hpp>
#include <LegacyOptimizer/RecordingSelectionPhase.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <RecordingCatalog.hpp>
#include <Replay/BinaryStoreFormat.hpp>
#include <Replay/ReplayNodeFingerprint.hpp>
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

LogicalPlan createPlacedUnaryPlanWithNestedEventTimeWatermark(const Host& host)
{
    auto plan = createPlacedUnaryPlan(host);
    auto root = plan.getRootOperators().front();
    auto child = root.getChildren().front();
    child = EventTimeWatermarkAssignerLogicalOperator(LogicalFunction{FieldAccessLogicalFunction("id")}, Windowing::TimeUnit::Milliseconds())
               .withTraitSet(child.getTraitSet())
               .withInferredSchema({createSchema()})
               .withChildren({child});
    root = root.withChildren({child});
    return plan.withRootOperators({root});
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

constexpr std::array<RecordingRepresentation, 4> allRepresentations()
{
    return {
        RecordingRepresentation::BinaryStore,
        RecordingRepresentation::BinaryStoreZstdFast1,
        RecordingRepresentation::BinaryStoreZstd,
        RecordingRepresentation::BinaryStoreZstdFast6};
}

constexpr std::array<RecordingRepresentation, 3> compressedRepresentations()
{
    return {
        RecordingRepresentation::BinaryStoreZstdFast1,
        RecordingRepresentation::BinaryStoreZstd,
        RecordingRepresentation::BinaryStoreZstdFast6};
}

int32_t compressionLevelForRepresentation(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return 0;
        case RecordingRepresentation::BinaryStoreZstdFast1:
            return Replay::BINARY_STORE_ZSTD_FAST1_COMPRESSION_LEVEL;
        case RecordingRepresentation::BinaryStoreZstd:
            return Replay::DEFAULT_BINARY_STORE_ZSTD_COMPRESSION_LEVEL;
        case RecordingRepresentation::BinaryStoreZstdFast6:
            return Replay::BINARY_STORE_ZSTD_FAST6_COMPRESSION_LEVEL;
    }
    std::unreachable();
}

TEST_F(RecordingSelectionPhaseTest, RecordingFingerprintIncludesRepresentation)
{
    const auto host = Host("worker-1:8080");
    const auto plan = createPlacedUnaryPlan(host);
    const auto root = plan.getRootOperators().front();
    ASSERT_EQ(root.getChildren().size(), 1U);

    const ReplaySpecification replaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt};
    const auto recordedSubplanRoot = root.getChildren().front();
    std::vector<std::string> fingerprints;
    fingerprints.reserve(allRepresentations().size());
    for (const auto representation : allRepresentations())
    {
        fingerprints.push_back(createRecordingFingerprint(recordedSubplanRoot, host, replaySpecification, representation));
    }
    std::ranges::sort(fingerprints);
    const auto [firstDuplicate, duplicatesEnd] = std::ranges::unique(fingerprints);
    EXPECT_EQ(firstDuplicate, duplicatesEnd);
}

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
    const auto reusableRecordingId = recordingIdFromFingerprint(createRecordingFingerprint(
        root.getChildren().front(), host, replaySpecification, RecordingRepresentation::BinaryStore));
    catalog.upsertRecording(
        RecordingEntry{
            .id = reusableRecordingId,
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/existing.bin",
            .structuralFingerprint = structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {DistributedQueryId("existing-query")},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

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
    ASSERT_EQ(rootCandidate->options.size(), 5U);
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
    for (const auto representation : compressedRepresentations())
    {
        EXPECT_TRUE(std::ranges::any_of(
            rootCandidate->options,
            [&](const auto& option)
            {
                return option.decision == RecordingSelectionDecision::CreateNewRecording && option.feasible
                    && option.selection.representation == representation;
            }));
    }
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
        = recordingIdFromFingerprint(createRecordingFingerprint(
            root.getChildren().front(), host, existingReplaySpecification, RecordingRepresentation::BinaryStore));
    catalog.upsertRecording(
        RecordingEntry{
            .id = weakerRecordingId,
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/weaker.bin",
            .structuralFingerprint = structuralFingerprint,
            .retentionWindowMs = existingReplaySpecification.retentionWindowMs,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {DistributedQueryId("existing-query")},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

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
    ASSERT_EQ(rootCandidate->options.size(), 5U);
    for (const auto representation : compressedRepresentations())
    {
        EXPECT_TRUE(std::ranges::any_of(
            rootCandidate->options,
            [&](const auto& option)
            {
                return option.decision == RecordingSelectionDecision::CreateNewRecording && option.feasible
                    && option.selection.representation == representation;
            }));
    }
}

TEST_F(RecordingSelectionPhaseTest, CandidatePhaseBudgetsUpgradeByIncrementalStorageBytes)
{
    const auto host = Host("worker-1:8080");
    const auto plan = createPlacedUnaryPlan(host);
    const auto root = plan.getRootOperators().front();
    ASSERT_EQ(root.getChildren().size(), 1U);

    ReplaySpecification requestedReplaySpecification;
    requestedReplaySpecification.retentionWindowMs = 300'000;
    ReplaySpecification existingReplaySpecification;
    existingReplaySpecification.retentionWindowMs = 60'000;

    const auto structuralFingerprint = createStructuralRecordingFingerprint(root.getChildren().front(), host);
    const auto findRawCreateCost = [&](const RecordingCandidateSet& candidateSet, const ReplaySpecification& replaySpecification)
    {
        const auto candidate = std::ranges::find_if(
            candidateSet.candidates,
            [&](const auto& boundaryCandidate)
            {
                return std::ranges::any_of(
                    boundaryCandidate.options,
                    [&](const auto& option)
                    {
                        return option.decision == RecordingSelectionDecision::CreateNewRecording
                            && option.selection.structuralFingerprint == structuralFingerprint
                            && option.selection.representation == RecordingRepresentation::BinaryStore
                            && option.selection.recordingId
                                == recordingIdFromFingerprint(
                                    createRecordingFingerprint(root.getChildren().front(), host, replaySpecification, RecordingRepresentation::BinaryStore));
                    });
            });
        EXPECT_NE(candidate, candidateSet.candidates.end());
        return candidate;
    };

    auto sizingCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(sizingCatalog->addWorker(host, {}, 16, {}, {}, 1'024 * 1'024));
    const auto requestedCandidateSet = RecordingCandidateSelectionPhase(sizingCatalog).apply(plan, requestedReplaySpecification, RecordingCatalog{});
    const auto existingCandidateSet = RecordingCandidateSelectionPhase(sizingCatalog).apply(plan, existingReplaySpecification, RecordingCatalog{});

    const auto requestedCandidate = findRawCreateCost(requestedCandidateSet, requestedReplaySpecification);
    const auto existingCandidate = findRawCreateCost(existingCandidateSet, existingReplaySpecification);
    const auto requestedOption = std::ranges::find_if(
        requestedCandidate->options,
        [&](const auto& option)
        {
            return option.decision == RecordingSelectionDecision::CreateNewRecording
                && option.selection.structuralFingerprint == structuralFingerprint
                && option.selection.representation == RecordingRepresentation::BinaryStore;
        });
    const auto existingOption = std::ranges::find_if(
        existingCandidate->options,
        [&](const auto& option)
        {
            return option.decision == RecordingSelectionDecision::CreateNewRecording
                && option.selection.structuralFingerprint == structuralFingerprint
                && option.selection.representation == RecordingRepresentation::BinaryStore;
        });
    ASSERT_NE(requestedOption, requestedCandidate->options.end());
    ASSERT_NE(existingOption, existingCandidate->options.end());

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, requestedOption->cost.estimatedStorageBytes));
    ASSERT_TRUE(workerCatalog->updateWorkerRuntimeMetrics(
        host,
        WorkerRuntimeMetrics{
            .observedAt = std::chrono::system_clock::now(),
            .recordingStorageBytes = existingOption->cost.estimatedStorageBytes,
            .recordingFileCount = 1,
            .activeQueryCount = 1,
            .replayOperatorStatistics = {},
            .recordingStatuses = {}}));

    RecordingCatalog catalog;
    const auto weakerRecordingId = recordingIdFromFingerprint(createRecordingFingerprint(
        root.getChildren().front(), host, existingReplaySpecification, RecordingRepresentation::BinaryStore));
    catalog.upsertRecording(
        RecordingEntry{
            .id = weakerRecordingId,
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/weaker.bin",
            .structuralFingerprint = structuralFingerprint,
            .retentionWindowMs = existingReplaySpecification.retentionWindowMs,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {DistributedQueryId("existing-query")},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

    const auto candidateSet = RecordingCandidateSelectionPhase(workerCatalog).apply(plan, requestedReplaySpecification, catalog);
    const auto candidate = findRawCreateCost(candidateSet, requestedReplaySpecification);
    const auto newRecording = std::ranges::find_if(
        candidate->options,
        [&](const auto& option)
        {
            return option.decision == RecordingSelectionDecision::CreateNewRecording
                && option.selection.structuralFingerprint == structuralFingerprint
                && option.selection.representation == RecordingRepresentation::BinaryStore;
        });
    const auto upgrade = std::ranges::find_if(
        candidate->options,
        [&](const auto& option)
        {
            return option.decision == RecordingSelectionDecision::UpgradeExistingRecording
                && option.selection.structuralFingerprint == structuralFingerprint
                && option.selection.representation == RecordingRepresentation::BinaryStore;
        });
    ASSERT_NE(newRecording, candidate->options.end());
    ASSERT_NE(upgrade, candidate->options.end());

    EXPECT_FALSE(newRecording->feasible);
    EXPECT_TRUE(upgrade->feasible);
    EXPECT_EQ(newRecording->cost.incrementalStorageBytes, newRecording->cost.estimatedStorageBytes);
    EXPECT_EQ(upgrade->cost.estimatedStorageBytes, requestedOption->cost.estimatedStorageBytes);
    EXPECT_EQ(
        upgrade->cost.incrementalStorageBytes,
        requestedOption->cost.estimatedStorageBytes - existingOption->cost.estimatedStorageBytes);
}

TEST_F(RecordingSelectionPhaseTest, CandidatePhaseEnumeratesAllRoutePlacementsWhenRouteFitsBottomUpLimit)
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
    ASSERT_EQ(candidate.options.size(), 12U);
    for (const auto& node : std::array<Host, 3>{sourceHost, intermediateHost, sinkHost})
    {
        EXPECT_EQ(
            std::ranges::count_if(candidate.options, [&](const auto& option) { return option.selection.node == node; }),
            allRepresentations().size());
        for (const auto representation : allRepresentations())
        {
            EXPECT_TRUE(std::ranges::any_of(
                candidate.options,
                [&](const auto& option)
                {
                    return option.selection.node == node && option.selection.representation == representation;
                }));
        }
    }
    EXPECT_TRUE(std::ranges::all_of(
        candidate.options,
        [](const auto& option) { return option.decision == RecordingSelectionDecision::CreateNewRecording && option.feasible; }));
}

TEST_F(RecordingSelectionPhaseTest, CandidatePhaseLimitsRoutePlacementsToBottomUpNDefault)
{
    const auto sourceHost = Host("source-node:8080");
    const auto firstIntermediateHost = Host("intermediate-node-1:8080");
    const auto secondIntermediateHost = Host("intermediate-node-2:8080");
    const auto thirdIntermediateHost = Host("intermediate-node-3:8080");
    const auto sinkHost = Host("sink-node:8080");

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(sourceHost, "", 16, {firstIntermediateHost}, {}, 1024 * 1024));
    ASSERT_TRUE(workerCatalog->addWorker(firstIntermediateHost, "", 16, {secondIntermediateHost}, {}, 1024 * 1024));
    ASSERT_TRUE(workerCatalog->addWorker(secondIntermediateHost, "", 16, {thirdIntermediateHost}, {}, 1024 * 1024));
    ASSERT_TRUE(workerCatalog->addWorker(thirdIntermediateHost, "", 16, {sinkHost}, {}, 1024 * 1024));
    ASSERT_TRUE(workerCatalog->addWorker(sinkHost, "", 16, {}, {}, 1024 * 1024));

    const auto plan = createPlacedSourceToSinkPlan(sourceHost, sinkHost);
    const auto candidateSet = RecordingCandidateSelectionPhase(workerCatalog).apply(
        plan,
        ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt},
        RecordingCatalog{});

    ASSERT_EQ(candidateSet.candidates.size(), 1U);
    const auto& candidate = candidateSet.candidates.front();
    EXPECT_EQ(
        candidate.routeNodes,
        std::vector<Host>({sourceHost, firstIntermediateHost, secondIntermediateHost, thirdIntermediateHost, sinkHost}));
    ASSERT_EQ(candidate.options.size(), 16U);
    for (const auto& node : std::array<Host, 4>{sourceHost, firstIntermediateHost, secondIntermediateHost, thirdIntermediateHost})
    {
        EXPECT_EQ(
            std::ranges::count_if(candidate.options, [&](const auto& option) { return option.selection.node == node; }),
            allRepresentations().size());
        for (const auto representation : allRepresentations())
        {
            EXPECT_TRUE(std::ranges::any_of(
                candidate.options,
                [&](const auto& option)
                {
                    return option.selection.node == node && option.selection.representation == representation;
                }));
        }
    }
    EXPECT_EQ(
        std::ranges::count_if(candidate.options, [&](const auto& option) { return option.selection.node == sinkHost; }),
        0);
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
    const auto activeRecordingId = recordingIdFromFingerprint(
        createRecordingFingerprint(activeSubplanRoot, host, replaySpecification, RecordingRepresentation::BinaryStore));
    catalog.upsertRecording(
        RecordingEntry{
            .id = activeRecordingId,
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/active-existing.bin",
            .structuralFingerprint = createStructuralRecordingFingerprint(activeSubplanRoot, host),
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {DistributedQueryId("active-query")},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});
    catalog.upsertQueryMetadata(
        DistributedQueryId("active-query"),
        ReplayableQueryMetadata{
            .originalPlan = activePlan,
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

TEST_F(RecordingSelectionPhaseTest, SelectionPhaseInjectsIngestionTimeWatermarkAssignerForReplayStoreWithoutExistingWatermark)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    auto incomingPlan = createPlacedUnaryPlan(host);
    const ReplaySpecification replaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt};

    const auto selectionResult = RecordingSelectionPhase(workerCatalog).apply(incomingPlan, replaySpecification, RecordingCatalog{});

    ASSERT_EQ(selectionResult.selectedRecordings.size(), 1U);
    EXPECT_EQ(getOperatorByType<StoreLogicalOperator>(incomingPlan).size(), 1U);
    EXPECT_EQ(getOperatorByType<IngestionTimeWatermarkAssignerLogicalOperator>(incomingPlan).size(), 1U);
    EXPECT_TRUE(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(incomingPlan).empty());
}

TEST_F(RecordingSelectionPhaseTest, ReplayPlanRewriterReusesExistingEventTimeWatermarkAssignerForReplayStore)
{
    const auto host = Host("worker-1:8080");
    auto plan = createPlacedUnaryPlanWithNestedEventTimeWatermark(host);
    const auto sink = plan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto assigner = sink.getChildren().front();
    ASSERT_TRUE(assigner.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value());
    ASSERT_EQ(assigner.getChildren().size(), 1U);
    const auto selection = assigner.getChildren().front();

    RecordingSelectionsByEdge storesToInsert;
    storesToInsert.emplace(
        RecordingRewriteEdge{.parentId = assigner.getId(), .childId = selection.getId()},
        RecordingSelection{
            .recordingId = RecordingId("replay-store-1"),
            .node = host,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/replay-store-1.bin",
            .structuralFingerprint = "selection-fingerprint",
            .representation = RecordingRepresentation::BinaryStore,
            .retentionWindowMs = std::nullopt,
            .beneficiaryQueries = {},
            .coversIncomingQuery = true});

    plan = rewriteReplayBoundary(plan, storesToInsert);

    EXPECT_EQ(getOperatorByType<StoreLogicalOperator>(plan).size(), 1U);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan).size(), 1U);
    EXPECT_TRUE(getOperatorByType<IngestionTimeWatermarkAssignerLogicalOperator>(plan).empty());
}

TEST_F(RecordingSelectionPhaseTest, SelectionPhaseRewritesCompressedRepresentationIntoStoreConfig)
{
    const auto host = Host("worker-1:8080");
    const ReplaySpecification replaySpecification{.retentionWindowMs = 600'000, .replayLatencyLimitMs = std::nullopt};

    auto sizingCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(sizingCatalog->addWorker(host, {}, 16, {}, {}, 1'024 * 1'024));

    const auto sizingCandidateSet
        = RecordingCandidateSelectionPhase(sizingCatalog).apply(createPlacedUnaryPlan(host), replaySpecification, RecordingCatalog{});
    std::optional<size_t> binaryStoreBytes;
    std::optional<std::pair<RecordingRepresentation, size_t>> bestCompressedRepresentation;
    std::optional<size_t> secondBestCompressedBytes;
    for (const auto& candidate : sizingCandidateSet.candidates)
    {
        for (const auto& option : candidate.options)
        {
            if (option.decision != RecordingSelectionDecision::CreateNewRecording)
            {
                continue;
            }

            if (option.selection.representation == RecordingRepresentation::BinaryStore)
            {
                binaryStoreBytes = std::min(binaryStoreBytes.value_or(option.cost.estimatedStorageBytes), option.cost.estimatedStorageBytes);
            }
            if (option.selection.representation != RecordingRepresentation::BinaryStore)
            {
                if (!bestCompressedRepresentation.has_value() || option.cost.estimatedStorageBytes < bestCompressedRepresentation->second)
                {
                    if (bestCompressedRepresentation.has_value())
                    {
                        secondBestCompressedBytes = std::min(
                            secondBestCompressedBytes.value_or(bestCompressedRepresentation->second), bestCompressedRepresentation->second);
                    }
                    bestCompressedRepresentation = std::pair{option.selection.representation, option.cost.estimatedStorageBytes};
                }
                else
                {
                    secondBestCompressedBytes
                        = std::min(secondBestCompressedBytes.value_or(option.cost.estimatedStorageBytes), option.cost.estimatedStorageBytes);
                }
            }
        }
    }

    ASSERT_TRUE(binaryStoreBytes.has_value());
    ASSERT_TRUE(bestCompressedRepresentation.has_value());
    ASSERT_TRUE(secondBestCompressedBytes.has_value());
    ASSERT_GT(*binaryStoreBytes, bestCompressedRepresentation->second);

    const auto constrainedBudget
        = bestCompressedRepresentation->second + std::max<size_t>(1, (*secondBestCompressedBytes - bestCompressedRepresentation->second) / 2);
    ASSERT_LT(constrainedBudget, *binaryStoreBytes);
    ASSERT_LT(constrainedBudget, *secondBestCompressedBytes);

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, constrainedBudget));

    auto incomingPlan = createPlacedUnaryPlan(host);
    const auto selectionResult = RecordingSelectionPhase(workerCatalog).apply(incomingPlan, replaySpecification, RecordingCatalog{});

    ASSERT_EQ(selectionResult.selectedRecordings.size(), 1U);
    EXPECT_EQ(selectionResult.selectedRecordings.front().representation, bestCompressedRepresentation->first);

    const auto stores = getOperatorByType<StoreLogicalOperator>(incomingPlan);
    ASSERT_EQ(stores.size(), 1U);
    const Descriptor descriptor{DescriptorConfig::Config(stores.front()->getConfig())};
    EXPECT_EQ(descriptor.getFromConfig(StoreLogicalOperator::ConfigParameters::COMPRESSION), Replay::BinaryStoreCompressionCodec::Zstd);
    EXPECT_EQ(
        descriptor.getFromConfig(StoreLogicalOperator::ConfigParameters::COMPRESSION_LEVEL),
        compressionLevelForRepresentation(bestCompressedRepresentation->first));
    EXPECT_EQ(descriptor.getFromConfig(StoreLogicalOperator::ConfigParameters::RETENTION_WINDOW_MS), "600000");
}

TEST_F(RecordingSelectionPhaseTest, SelectionResultCanCreateRecordingForActiveOnlyMergedBoundary)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    auto incomingPlan = createPlacedUnaryPlan("INCOMING_SOURCE", host);
    const auto activePlan = createPlacedUnaryPlan("ACTIVE_SOURCE", host);
    const ReplaySpecification replaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt};

    RecordingCatalog catalog;
    catalog.upsertQueryMetadata(
        DistributedQueryId("active-query"),
        ReplayableQueryMetadata{
            .originalPlan = activePlan,
            .globalPlan = activePlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {},
            .networkExplanations = {}});

    const auto selectionResult = RecordingSelectionPhase(workerCatalog).apply(incomingPlan, replaySpecification, catalog);

    const auto activeOnlySelection = std::ranges::find_if(
        selectionResult.networkExplanations,
        [](const auto& explanation) { return !explanation.selection.coversIncomingQuery; });
    ASSERT_NE(activeOnlySelection, selectionResult.networkExplanations.end());
    EXPECT_EQ(activeOnlySelection->selection.beneficiaryQueries, std::vector<std::string>({"active-query"}));
    EXPECT_EQ(activeOnlySelection->decision, RecordingSelectionDecision::CreateNewRecording);

    ASSERT_EQ(selectionResult.activeQueryPlanRewrites.size(), 1U);
    EXPECT_EQ(selectionResult.activeQueryPlanRewrites.front().queryId, "active-query");
    ASSERT_EQ(selectionResult.activeQueryPlanRewrites.front().insertions.size(), 1U);
    EXPECT_EQ(selectionResult.activeQueryPlanRewrites.front().insertions.front().selection, activeOnlySelection->selection);
    ASSERT_FALSE(selectionResult.activeQueryPlanRewrites.front().insertions.front().materializationEdges.empty());

}

TEST_F(RecordingSelectionPhaseTest, CandidatePhaseUsesRuntimeReplayStatisticsForOperatorReplayTimes)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    const auto plan = createPlacedUnaryPlan(host);
    const auto root = plan.getRootOperators().front();
    ASSERT_EQ(root.getChildren().size(), 1U);
    const auto measuredOperator = root.getChildren().front();
    const auto fingerprint = Replay::createStructuralReplayNodeFingerprint(measuredOperator);
    const auto structuralFingerprint = createStructuralRecordingFingerprint(measuredOperator, host);

    ASSERT_TRUE(workerCatalog->updateWorkerRuntimeMetrics(
        host,
        WorkerRuntimeMetrics{
            .observedAt = std::chrono::system_clock::now(),
            .recordingStorageBytes = 0,
            .recordingFileCount = 0,
            .activeQueryCount = 0,
            .replayOperatorStatistics
            = {{fingerprint,
                ReplayOperatorStatistics{
                    .nodeFingerprint = fingerprint,
                    .inputTuples = 128,
                    .outputTuples = 64,
                    .taskCount = 2,
                    .executionTimeNanos = 34'000'000}}},
            .recordingStatuses = {}}));

    const auto candidateSet = RecordingCandidateSelectionPhase(workerCatalog).apply(
        plan,
        ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt},
        RecordingCatalog{});

    EXPECT_TRUE(std::ranges::any_of(
        candidateSet.operatorReplayTimes,
        [](const auto& replayTime) { return std::abs(replayTime.replayTimeMs - 17.0) < 0.001; }));

    const auto candidate = std::ranges::find_if(
        candidateSet.candidates,
        [&](const auto& boundaryCandidate)
        {
            return std::ranges::any_of(
                boundaryCandidate.options,
                [&](const auto& option)
                {
                    return option.decision == RecordingSelectionDecision::CreateNewRecording
                        && option.selection.structuralFingerprint == structuralFingerprint
                        && option.selection.representation == RecordingRepresentation::BinaryStore;
                });
        });
    ASSERT_NE(candidate, candidateSet.candidates.end());
    const auto rawRecording = std::ranges::find_if(
        candidate->options,
        [&](const auto& option)
        {
            return option.decision == RecordingSelectionDecision::CreateNewRecording
                && option.selection.structuralFingerprint == structuralFingerprint
                && option.selection.representation == RecordingRepresentation::BinaryStore;
        });
    ASSERT_NE(rawRecording, candidate->options.end());

    const auto schemaBytes = std::max(measuredOperator.getOutputSchema().getSizeOfSchemaInBytes(), size_t{1});
    const auto expectedWriteBytesPerSecond = static_cast<size_t>(std::ceil(static_cast<double>(64U * schemaBytes) / 0.034));
    const auto expectedRetainedBytes = static_cast<size_t>(std::ceil(static_cast<double>(expectedWriteBytesPerSecond) * 60.0));
    EXPECT_EQ(rawRecording->cost.estimatedStorageBytes, expectedRetainedBytes);
    EXPECT_EQ(rawRecording->cost.incrementalStorageBytes, expectedRetainedBytes);
}

TEST_F(RecordingSelectionPhaseTest, CandidatePhaseUsesMeasuredReplayReadBandwidthForReplayLatency)
{
    const auto host = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, {}, 16, {}, {}, 1024 * 1024));

    ASSERT_TRUE(workerCatalog->updateWorkerRuntimeMetrics(
        host,
        WorkerRuntimeMetrics{
            .observedAt = std::chrono::system_clock::time_point(std::chrono::seconds(1)),
            .recordingStorageBytes = 0,
            .recordingFileCount = 0,
            .activeQueryCount = 0,
            .replayReadBytes = 0,
            .replayOperatorStatistics = {},
            .recordingStatuses = {}}));
    constexpr size_t measuredReplayReadBytesPerSecond = 5 * 1024 * 1024;
    ASSERT_TRUE(workerCatalog->updateWorkerRuntimeMetrics(
        host,
        WorkerRuntimeMetrics{
            .observedAt = std::chrono::system_clock::time_point(std::chrono::seconds(3)),
            .recordingStorageBytes = 0,
            .recordingFileCount = 0,
            .activeQueryCount = 0,
            .replayReadBytes = measuredReplayReadBytesPerSecond * 2,
            .replayOperatorStatistics = {},
            .recordingStatuses = {}}));

    const auto plan = createPlacedUnaryPlan(host);
    const auto root = plan.getRootOperators().front();
    ASSERT_EQ(root.getChildren().size(), 1U);
    const auto structuralFingerprint = createStructuralRecordingFingerprint(root.getChildren().front(), host);

    const auto candidateSet = RecordingCandidateSelectionPhase(workerCatalog).apply(
        plan,
        ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt},
        RecordingCatalog{});

    const auto candidate = std::ranges::find_if(
        candidateSet.candidates,
        [&](const auto& boundaryCandidate)
        {
            return std::ranges::any_of(
                boundaryCandidate.options,
                [&](const auto& option)
                {
                    return option.decision == RecordingSelectionDecision::CreateNewRecording
                        && option.selection.structuralFingerprint == structuralFingerprint
                        && option.selection.representation == RecordingRepresentation::BinaryStore;
                });
        });
    ASSERT_NE(candidate, candidateSet.candidates.end());
    const auto rawRecording = std::ranges::find_if(
        candidate->options,
        [&](const auto& option)
        {
            return option.decision == RecordingSelectionDecision::CreateNewRecording
                && option.selection.structuralFingerprint == structuralFingerprint
                && option.selection.representation == RecordingRepresentation::BinaryStore;
        });
    ASSERT_NE(rawRecording, candidate->options.end());

    EXPECT_EQ(rawRecording->cost.estimatedReplayBandwidthBytesPerSecond, measuredReplayReadBytesPerSecond);
    EXPECT_EQ(
        rawRecording->cost.estimatedReplayLatencyMs,
        static_cast<uint64_t>(std::ceil((static_cast<double>(rawRecording->cost.estimatedStorageBytes) * 1000.0) / measuredReplayReadBytesPerSecond)));
}
}
}
