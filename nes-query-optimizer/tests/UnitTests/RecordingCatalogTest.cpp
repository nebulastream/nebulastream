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

#include <RecordingCatalog.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace NES
{

TEST(RecordingCatalogTest, UpsertRecordingMergesOwnerQueriesForExistingRecording)
{
    RecordingCatalog catalog;
    const auto recordingId = RecordingId("recording-1");

    catalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/store_read_out.bin",
            .structuralFingerprint = {},
            .retentionWindowMs = std::nullopt,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {DistributedQueryId("query-1")},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});
    catalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/store_read_out.bin",
            .structuralFingerprint = {},
            .retentionWindowMs = std::nullopt,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {DistributedQueryId("query-2")},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

    const auto recording = catalog.getRecording(recordingId);
    ASSERT_TRUE(recording.has_value());
    EXPECT_THAT(
        recording->ownerQueries,
        testing::UnorderedElementsAre(DistributedQueryId("query-1"), DistributedQueryId("query-2")));
}

TEST(RecordingCatalogTest, RemoveQueryMetadataRetainsSharedRecordingUntilLastOwner)
{
    RecordingCatalog catalog;
    const auto recordingId = RecordingId("recording-1");
    const auto query1 = DistributedQueryId("query-1");
    const auto query2 = DistributedQueryId("query-2");

    catalog.upsertQueryMetadata(query1, ReplayableQueryMetadata{});
    catalog.upsertQueryMetadata(query2, ReplayableQueryMetadata{});
    catalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/store_read_out.bin",
            .structuralFingerprint = {},
            .retentionWindowMs = std::nullopt,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {query1, query2},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

    catalog.removeQueryMetadata(query1);
    const auto remainingRecording = catalog.getRecording(recordingId);
    ASSERT_TRUE(remainingRecording.has_value());
    EXPECT_THAT(remainingRecording->ownerQueries, testing::ElementsAre(query2));

    catalog.removeQueryMetadata(query2);
    const auto drainingRecording = catalog.getRecording(recordingId);
    ASSERT_TRUE(drainingRecording.has_value());
    EXPECT_TRUE(drainingRecording->ownerQueries.empty());
}

TEST(RecordingCatalogTest, TimeTravelReadRecordingTracksExplicitActiveAndPendingSelection)
{
    RecordingCatalog catalog;
    const auto recording1 = RecordingId("recording-1");
    const auto recording2 = RecordingId("recording-2");
    const auto query1 = DistributedQueryId("query-1");
    const auto query2 = DistributedQueryId("query-2");

    catalog.upsertQueryMetadata(query1, ReplayableQueryMetadata{});
    catalog.upsertQueryMetadata(query2, ReplayableQueryMetadata{});
    catalog.upsertRecording(
        RecordingEntry{
            .id = recording1,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/recording-1.bin",
            .structuralFingerprint = {},
            .retentionWindowMs = std::nullopt,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {query1},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});
    catalog.upsertRecording(
        RecordingEntry{
            .id = recording2,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/recording-2.bin",
            .structuralFingerprint = {},
            .retentionWindowMs = std::nullopt,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {query2},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

    EXPECT_FALSE(catalog.getTimeTravelReadRecording().has_value());
    EXPECT_FALSE(catalog.getPendingTimeTravelReadRecording().has_value());

    catalog.setPendingTimeTravelReadRecording(recording2);
    ASSERT_TRUE(catalog.getPendingTimeTravelReadRecording().has_value());
    EXPECT_EQ(catalog.getPendingTimeTravelReadRecording()->id, recording2);

    catalog.setTimeTravelReadRecording(recording1);
    ASSERT_TRUE(catalog.getTimeTravelReadRecording().has_value());
    EXPECT_EQ(catalog.getTimeTravelReadRecording()->id, recording1);

    catalog.setPendingTimeTravelReadRecording(std::nullopt);
    EXPECT_FALSE(catalog.getPendingTimeTravelReadRecording().has_value());
}

TEST(RecordingCatalogTest, ReconcileQuerySelectionsRemovesStaleOwnershipAndUpdatesMetadata)
{
    RecordingCatalog catalog;
    const auto queryId = DistributedQueryId("query-1");
    const auto sharedQueryId = DistributedQueryId("query-2");
    const auto staleRecordingId = RecordingId("stale-recording");
    const auto sharedRecordingId = RecordingId("shared-recording");
    const auto replacementRecordingId = RecordingId("replacement-recording");

    catalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = std::nullopt,
            .globalPlan = std::nullopt,
            .replaySpecification = std::nullopt,
            .selectedRecordings = {staleRecordingId, sharedRecordingId},
            .networkExplanations = {},
            .queryPlanRewrite = std::nullopt,
            .replayBoundaries = {}});
    catalog.upsertQueryMetadata(sharedQueryId, ReplayableQueryMetadata{});
    catalog.upsertRecording(
        RecordingEntry{
            .id = staleRecordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/stale.bin",
            .structuralFingerprint = "stale",
            .retentionWindowMs = std::nullopt,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {queryId},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});
    catalog.upsertRecording(
        RecordingEntry{
            .id = sharedRecordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/shared.bin",
            .structuralFingerprint = "shared",
            .retentionWindowMs = std::nullopt,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {queryId, sharedQueryId},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});
    catalog.upsertRecording(
        RecordingEntry{
            .id = replacementRecordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/replacement.bin",
            .structuralFingerprint = "replacement",
            .retentionWindowMs = 60'000,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {queryId},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

    catalog.reconcileQuerySelections(
        queryId,
        {sharedRecordingId, replacementRecordingId},
        {RecordingSelectionExplanation{
            .selection =
                RecordingSelection{
                    .recordingId = replacementRecordingId,
                    .node = Host("worker-1:8080"),
                    .filePath = "/tmp/REPLAY-NebulaStream/recordings/replacement.bin",
                    .structuralFingerprint = "replacement",
                    .representation = RecordingRepresentation::BinaryStore,
                    .retentionWindowMs = std::nullopt,
                    .beneficiaryQueries = {},
                    .coversIncomingQuery = true},
            .decision = RecordingSelectionDecision::ReuseExistingRecording,
            .reason = "",
            .chosenCost = {},
            .alternatives = {}}});

    ASSERT_TRUE(catalog.getRecording(staleRecordingId).has_value());
    EXPECT_TRUE(catalog.getRecording(staleRecordingId)->ownerQueries.empty());
    ASSERT_TRUE(catalog.getRecording(sharedRecordingId).has_value());
    EXPECT_THAT(catalog.getRecording(sharedRecordingId)->ownerQueries, testing::UnorderedElementsAre(queryId, sharedQueryId));
    ASSERT_TRUE(catalog.getRecording(replacementRecordingId).has_value());
    EXPECT_THAT(catalog.getRecording(replacementRecordingId)->ownerQueries, testing::ElementsAre(queryId));
    ASSERT_TRUE(catalog.getQueryMetadata().contains(queryId));
    EXPECT_THAT(
        catalog.getQueryMetadata().at(queryId).selectedRecordings,
        testing::ElementsAre(sharedRecordingId, replacementRecordingId));
    ASSERT_EQ(catalog.getQueryMetadata().at(queryId).networkExplanations.size(), 1U);
    EXPECT_EQ(catalog.getQueryMetadata().at(queryId).networkExplanations.front().selection.recordingId, replacementRecordingId);
}

TEST(RecordingCatalogTest, ReconcileWorkerRuntimeStatusUpdatesKnownRecordings)
{
    RecordingCatalog catalog;
    const auto recordingId = RecordingId("recording-1");
    catalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/recording-1.bin",
            .structuralFingerprint = "recording-1",
            .retentionWindowMs = 60'000,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {DistributedQueryId("query-1")},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

    catalog.reconcileWorkerRuntimeStatus(
        Host("worker-1:8080"),
        {Replay::RecordingRuntimeStatus{
            .recordingId = "recording-1",
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/recording-1.bin",
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retentionWindowMs = 60'000,
            .retainedStartWatermark = Timestamp(1000),
            .retainedEndWatermark = Timestamp(61'000),
            .fillWatermark = Timestamp(61'000),
            .segmentCount = 4,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt}});

    const auto recording = catalog.getRecording(recordingId);
    ASSERT_TRUE(recording.has_value());
    EXPECT_EQ(recording->lifecycleState, std::optional(Replay::RecordingLifecycleState::Ready));
    EXPECT_EQ(recording->retainedStartWatermark, std::optional<Timestamp>(Timestamp(1000)));
    EXPECT_EQ(recording->retainedEndWatermark, std::optional<Timestamp>(Timestamp(61'000)));
    EXPECT_EQ(recording->fillWatermark, std::optional<Timestamp>(Timestamp(61'000)));
    EXPECT_EQ(recording->segmentCount, std::optional<size_t>(4));
    EXPECT_EQ(recording->storageBytes, std::optional<size_t>(4096));
}

}
