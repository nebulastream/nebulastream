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
            .ownerQueries = {DistributedQueryId("query-1")}});
    catalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/store_read_out.bin",
            .structuralFingerprint = {},
            .retentionWindowMs = std::nullopt,
            .ownerQueries = {DistributedQueryId("query-2")}});

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
            .ownerQueries = {query1, query2}});

    catalog.removeQueryMetadata(query1);
    const auto remainingRecording = catalog.getRecording(recordingId);
    ASSERT_TRUE(remainingRecording.has_value());
    EXPECT_THAT(remainingRecording->ownerQueries, testing::ElementsAre(query2));

    catalog.removeQueryMetadata(query2);
    EXPECT_FALSE(catalog.getRecording(recordingId).has_value());
}

TEST(RecordingCatalogTest, TimeTravelReadRecordingTracksMostRecentRecordingAndFallsBackAfterRemoval)
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
            .ownerQueries = {query1}});
    catalog.upsertRecording(
        RecordingEntry{
            .id = recording2,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/recording-2.bin",
            .structuralFingerprint = {},
            .retentionWindowMs = std::nullopt,
            .ownerQueries = {query2}});

    ASSERT_TRUE(catalog.getTimeTravelReadRecording().has_value());
    EXPECT_EQ(catalog.getTimeTravelReadRecording()->id, recording2);

    catalog.removeQueryMetadata(query2);
    ASSERT_TRUE(catalog.getTimeTravelReadRecording().has_value());
    EXPECT_EQ(catalog.getTimeTravelReadRecording()->id, recording1);
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
            .globalPlan = std::nullopt,
            .replaySpecification = std::nullopt,
            .selectedRecordings = {staleRecordingId, sharedRecordingId},
            .networkExplanations = {}});
    catalog.upsertQueryMetadata(sharedQueryId, ReplayableQueryMetadata{});
    catalog.upsertRecording(
        RecordingEntry{
            .id = staleRecordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/stale.bin",
            .structuralFingerprint = "stale",
            .retentionWindowMs = std::nullopt,
            .ownerQueries = {queryId}});
    catalog.upsertRecording(
        RecordingEntry{
            .id = sharedRecordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/shared.bin",
            .structuralFingerprint = "shared",
            .retentionWindowMs = std::nullopt,
            .ownerQueries = {queryId, sharedQueryId}});
    catalog.upsertRecording(
        RecordingEntry{
            .id = replacementRecordingId,
            .node = Host("worker-1:8080"),
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/replacement.bin",
            .structuralFingerprint = "replacement",
            .retentionWindowMs = 60'000,
            .ownerQueries = {queryId}});

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
                    .beneficiaryQueries = {},
                    .coversIncomingQuery = true},
            .decision = RecordingSelectionDecision::ReuseExistingRecording,
            .reason = "",
            .chosenCost = {},
            .alternatives = {}}});

    EXPECT_FALSE(catalog.getRecording(staleRecordingId).has_value());
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

}
