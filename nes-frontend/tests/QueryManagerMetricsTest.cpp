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

#include <QueryManager/QueryManager.hpp>

#include <chrono>
#include <cstddef>
#include <expected>
#include <optional>
#include <string>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <QueryId.hpp>
#include <Replay/ReplayStorage.hpp>
#include <RecordingSelectionResult.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
namespace
{
class FakeWorkerStatusBackend final : public QuerySubmissionBackend
{
    std::expected<WorkerStatus, Exception> workerStatusResult;

public:
    explicit FakeWorkerStatusBackend(std::expected<WorkerStatus, Exception> workerStatusResult)
        : workerStatusResult(std::move(workerStatusResult))
    {
    }

    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan) override
    {
        return std::unexpected(QueryRegistrationFailed("unused in FakeWorkerStatusBackend"));
    }

    std::expected<void, Exception> start(QueryId) override { return std::unexpected(QueryStartFailed("unused in FakeWorkerStatusBackend")); }
    std::expected<void, Exception> stop(QueryId) override { return std::unexpected(QueryStopFailed("unused in FakeWorkerStatusBackend")); }
    std::expected<void, Exception> unregister(QueryId) override
    {
        return std::unexpected(QueryUnregistrationFailed("unused in FakeWorkerStatusBackend"));
    }

    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId) const override
    {
        return std::unexpected(QueryStatusFailed("unused in FakeWorkerStatusBackend"));
    }

    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point) const override
    {
        return workerStatusResult;
    }
};

class FakeLifecycleBackend final : public QuerySubmissionBackend
{
    size_t nextQueryId = 1;

public:
    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan plan) override
    {
        static_cast<void>(nextQueryId++);
        return QueryId::create(
            LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), plan.getQueryId().getDistributedQueryId());
    }

    std::expected<void, Exception> start(QueryId) override { return {}; }
    std::expected<void, Exception> stop(QueryId) override { return {}; }
    std::expected<void, Exception> unregister(QueryId) override { return {}; }

    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId) const override
    {
        return std::unexpected(QueryStatusFailed("unused in FakeLifecycleBackend"));
    }

    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point) const override
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        return status;
    }
};

Schema createSchema()
{
    Schema schema;
    schema.addField("id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
    return schema;
}

LogicalPlan createReplayPlan(const DistributedQueryId& queryId, const std::string& sourceName)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan(sourceName, createSchema(), {}, {});
    plan = LogicalPlanBuilder::addSink("test_sink", plan);
    plan.setQueryId(QueryId::createDistributed(queryId));
    return plan;
}

class ScopedTimeTravelReadAliasReset
{
public:
    ScopedTimeTravelReadAliasReset() { Replay::clearTimeTravelReadAlias(); }
    ~ScopedTimeTravelReadAliasReset() { Replay::clearTimeTravelReadAlias(); }
};
}

TEST(QueryManagerMetricsTest, RefreshWorkerMetricsUpdatesWorkerCatalogFromBackendStatus)
{
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(Host("worker-1:8080"), "worker-1-data", INFINITE_CAPACITY, {}));
    const auto observedAt = std::chrono::system_clock::now();

    QueryManager queryManager(
        workerCatalog,
        [observedAt](const WorkerConfig&)
        {
            WorkerStatus status;
            status.until = observedAt;
            status.replayMetrics = WorkerStatus::ReplayMetrics{
                .recordingStorageBytes = 1337,
                .recordingFileCount = 2,
                .activeQueryCount = 4};
            return std::make_unique<FakeWorkerStatusBackend>(status);
        });

    queryManager.refreshWorkerMetrics();

    const auto metrics = workerCatalog->getWorkerRuntimeMetrics(Host("worker-1:8080"));
    ASSERT_TRUE(metrics.has_value());
    EXPECT_EQ(
        metrics.value(),
        (WorkerRuntimeMetrics{
            .observedAt = observedAt,
            .recordingStorageBytes = 1337,
            .recordingFileCount = 2,
            .activeQueryCount = 4}));
}

TEST(QueryManagerMetricsTest, RefreshWorkerMetricsKeepsCatalogUntouchedWhenBackendFails)
{
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(Host("worker-1:8080"), "worker-1-data", INFINITE_CAPACITY, {}));

    QueryManager queryManager(
        workerCatalog,
        [](const WorkerConfig&)
        {
            return std::make_unique<FakeWorkerStatusBackend>(
                std::unexpected(UnknownException("simulated worker status failure")));
        });

    queryManager.refreshWorkerMetrics();

    EXPECT_FALSE(workerCatalog->getWorkerRuntimeMetrics(Host("worker-1:8080")).has_value());
}

TEST(QueryManagerMetricsTest, RegisterQueryReconcilesActiveReplaySelectionsFromNetworkExplanations)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto activeQueryId = DistributedQueryId("active-query");
    const auto incomingQueryId = DistributedQueryId("incoming-query");
    const auto staleRecordingId = RecordingId("stale-recording");
    const auto reusedRecordingId = RecordingId("reused-recording");
    const auto incomingRecordingId = RecordingId("incoming-recording");

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    const auto activePlan = createReplayPlan(activeQueryId, "ACTIVE_SOURCE");
    const auto incomingPlan = createReplayPlan(incomingQueryId, "INCOMING_SOURCE");

    QueryManagerState state;
    state.queries.emplace(
        activeQueryId,
        DistributedQuery(
            {{worker, {QueryId::createLocal(LocalQueryId(std::string("11111111-1111-1111-1111-111111111111")))}}}));
    state.recordingCatalog.upsertQueryMetadata(
        activeQueryId,
        ReplayableQueryMetadata{
            .globalPlan = activePlan,
            .replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt},
            .selectedRecordings = {staleRecordingId},
            .networkExplanations = {}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = staleRecordingId,
            .node = worker,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/stale.bin",
            .structuralFingerprint = "stale",
            .retentionWindowMs = 60'000,
            .ownerQueries = {activeQueryId}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = reusedRecordingId,
            .node = worker,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/reused.bin",
            .structuralFingerprint = "reused",
            .retentionWindowMs = 60'000,
            .ownerQueries = {DistributedQueryId("other-query")}});

    QueryManager queryManager(workerCatalog, [](const WorkerConfig&) { return std::make_unique<FakeLifecycleBackend>(); }, std::move(state));

    const auto incomingSelection = RecordingSelection{
        .recordingId = incomingRecordingId,
        .node = worker,
        .filePath = "/tmp/REPLAY-NebulaStream/recordings/incoming.bin",
        .structuralFingerprint = "incoming",
        .representation = RecordingRepresentation::BinaryStore,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    const auto reusedSelection = RecordingSelection{
        .recordingId = reusedRecordingId,
        .node = worker,
        .filePath = "/tmp/REPLAY-NebulaStream/recordings/reused.bin",
        .structuralFingerprint = "reused",
        .representation = RecordingRepresentation::BinaryStore,
        .beneficiaryQueries = {activeQueryId.getRawValue()},
        .coversIncomingQuery = false};

    const auto distributedPlan = DistributedLogicalPlan(
        DecomposedLogicalPlan<Host>{{{worker, {incomingPlan}}}},
        incomingPlan,
        ReplaySpecification{.retentionWindowMs = 300'000, .replayLatencyLimitMs = std::nullopt},
        RecordingSelectionResult{
            .networkExplanations =
                {
                    RecordingSelectionExplanation{
                        .selection = incomingSelection,
                        .decision = RecordingSelectionDecision::CreateNewRecording,
                        .reason = "incoming create",
                        .chosenCost = {},
                        .alternatives = {}},
                    RecordingSelectionExplanation{
                        .selection = reusedSelection,
                        .decision = RecordingSelectionDecision::ReuseExistingRecording,
                        .reason = "active reuse",
                        .chosenCost = {},
                        .alternatives = {}},
                },
            .selectedRecordings = {incomingSelection},
            .explanations = {RecordingSelectionExplanation{
                .selection = incomingSelection,
                .decision = RecordingSelectionDecision::CreateNewRecording,
                .reason = "incoming create",
                .chosenCost = {},
                .alternatives = {}}}});

    const auto registerResult = queryManager.registerQuery(distributedPlan);
    ASSERT_TRUE(registerResult.has_value()) << registerResult.error().what();

    const auto& recordingCatalog = queryManager.getRecordingCatalog();
    ASSERT_TRUE(recordingCatalog.getQueryMetadata().contains(activeQueryId));
    EXPECT_THAT(recordingCatalog.getQueryMetadata().at(activeQueryId).selectedRecordings, testing::ElementsAre(reusedRecordingId));
    ASSERT_EQ(recordingCatalog.getQueryMetadata().at(activeQueryId).networkExplanations.size(), 1U);
    EXPECT_EQ(
        recordingCatalog.getQueryMetadata().at(activeQueryId).networkExplanations.front().decision,
        RecordingSelectionDecision::ReuseExistingRecording);

    EXPECT_FALSE(recordingCatalog.getRecording(staleRecordingId).has_value());
    ASSERT_TRUE(recordingCatalog.getRecording(reusedRecordingId).has_value());
    EXPECT_THAT(
        recordingCatalog.getRecording(reusedRecordingId)->ownerQueries,
        testing::UnorderedElementsAre(DistributedQueryId("other-query"), activeQueryId));
    ASSERT_TRUE(recordingCatalog.getRecording(incomingRecordingId).has_value());
    EXPECT_THAT(recordingCatalog.getRecording(incomingRecordingId)->ownerQueries, testing::ElementsAre(*registerResult));
    ASSERT_TRUE(recordingCatalog.getTimeTravelReadRecording().has_value());
    EXPECT_EQ(recordingCatalog.getTimeTravelReadRecording()->id, incomingRecordingId);
}

}
