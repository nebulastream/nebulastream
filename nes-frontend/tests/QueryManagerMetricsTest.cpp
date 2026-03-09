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
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Listeners/QueryLog.hpp>
#include <LegacyOptimizer.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <QueryId.hpp>
#include <Replay/ReplayStorage.hpp>
#include <Replay/TimeTravelReadResolver.hpp>
#include <RecordingSelectionResult.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
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

struct TrackingLifecycleBackendState
{
    size_t nextUuidIndex = 2;
    std::unordered_map<QueryId, LocalQueryStatus> statuses;
    std::unordered_map<DistributedQueryId, size_t> registrations;
    std::unordered_map<DistributedQueryId, size_t> starts;
    std::unordered_map<DistributedQueryId, size_t> stops;
    std::unordered_map<DistributedQueryId, size_t> unregisters;
    std::expected<WorkerStatus, Exception> workerStatusResult = []()
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        return std::expected<WorkerStatus, Exception>(status);
    }();

    [[nodiscard]] LocalQueryId nextLocalQueryId()
    {
        return LocalQueryId(std::string(
            nextUuidIndex++ == 2 ? "00000000-0000-0000-0000-000000000002" : "00000000-0000-0000-0000-000000000003"));
    }
};

class TrackingLifecycleBackend final : public QuerySubmissionBackend
{
    std::shared_ptr<TrackingLifecycleBackendState> state;

public:
    explicit TrackingLifecycleBackend(std::shared_ptr<TrackingLifecycleBackendState> state) : state(std::move(state)) { }

    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan plan) override
    {
        const auto queryId = QueryId::create(state->nextLocalQueryId(), plan.getQueryId().getDistributedQueryId());
        state->statuses.insert_or_assign(queryId, LocalQueryStatus{.queryId = queryId, .state = QueryState::Registered});
        ++state->registrations[plan.getQueryId().getDistributedQueryId()];
        return queryId;
    }

    std::expected<void, Exception> start(QueryId queryId) override
    {
        auto it = state->statuses.find(queryId);
        if (it == state->statuses.end())
        {
            return std::unexpected(QueryStartFailed("unknown query {}", queryId));
        }
        it->second.state = QueryState::Running;
        ++state->starts[queryId.getDistributedQueryId()];
        return {};
    }

    std::expected<void, Exception> stop(QueryId queryId) override
    {
        auto it = state->statuses.find(queryId);
        if (it == state->statuses.end())
        {
            return std::unexpected(QueryStopFailed("unknown query {}", queryId));
        }
        it->second.state = QueryState::Stopped;
        ++state->stops[queryId.getDistributedQueryId()];
        return {};
    }

    std::expected<void, Exception> unregister(QueryId queryId) override
    {
        if (!state->statuses.erase(queryId))
        {
            return std::unexpected(QueryUnregistrationFailed("unknown query {}", queryId));
        }
        ++state->unregisters[queryId.getDistributedQueryId()];
        return {};
    }

    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId queryId) const override
    {
        if (const auto it = state->statuses.find(queryId); it != state->statuses.end())
        {
            return it->second;
        }
        return std::unexpected(QueryStatusFailed("unknown query {}", queryId));
    }

    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point) const override
    {
        return state->workerStatusResult;
    }
};

Schema createSchema()
{
    Schema schema;
    schema.addField("id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
    return schema;
}

Schema createQualifiedSchema(const std::string& sourceName)
{
    Schema schema;
    schema.addField(sourceName + "$id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
    return schema;
}

LogicalPlan createReplayPlan(
    const DistributedQueryId& queryId, const std::string& sourceName, const std::string& sinkName = "test_sink")
{
    auto plan = LogicalPlanBuilder::createLogicalPlan(sourceName);
    plan = LogicalPlanBuilder::addSink(sinkName, plan);
    plan.setQueryId(QueryId::createDistributed(queryId));
    return plan;
}

void addSourceAndSinkCatalogEntries(
    const std::shared_ptr<SourceCatalog>& sourceCatalog,
    const std::shared_ptr<SinkCatalog>& sinkCatalog,
    const Host& worker,
    const std::string& sourceName,
    const std::string& sinkName)
{
    const auto logicalSource = sourceCatalog->addLogicalSource(sourceName, createSchema());
    ASSERT_TRUE(logicalSource.has_value());
    ASSERT_TRUE(sourceCatalog
                    ->addPhysicalSource(*logicalSource, "File", worker, {{"file_path", "does_not_exist"}}, {{"type", "CSV"}})
                    .has_value());
    if (!sinkCatalog->containsSinkDescriptor(sinkName))
    {
        ASSERT_TRUE(sinkCatalog->addSinkDescriptor(sinkName, createQualifiedSchema(sourceName), "Void", worker, {}).has_value());
    }
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
    const auto fingerprint = std::string("selection@worker-1:8080");
    const auto recordingStatus = Replay::RecordingRuntimeStatus{
        .recordingId = "recording-1",
        .filePath = "/tmp/REPLAY-NebulaStream/recordings/recording-1.bin",
        .lifecycleState = Replay::RecordingLifecycleState::Ready,
        .retentionWindowMs = 60'000,
        .retainedStartWatermark = Timestamp(1000),
        .retainedEndWatermark = Timestamp(61'000),
        .fillWatermark = Timestamp(61'000),
        .segmentCount = 2,
        .storageBytes = 1337,
        .successorRecordingId = std::nullopt};

    QueryManager queryManager(
        workerCatalog,
        [observedAt, fingerprint, recordingStatus](const WorkerConfig&)
        {
            WorkerStatus status;
            status.until = observedAt;
            status.replayMetrics = WorkerStatus::ReplayMetrics{
                .recordingStorageBytes = 1337,
                .recordingFileCount = 1,
                .activeQueryCount = 4,
                .replayReadBytes = 8192,
                .operatorStatistics = {ReplayOperatorStatistics{
                    .nodeFingerprint = fingerprint,
                    .inputTuples = 128,
                    .outputTuples = 32,
                    .taskCount = 4,
                    .executionTimeNanos = 20'000'000}},
                .recordingStatuses = {recordingStatus}};
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
            .recordingFileCount = 1,
            .activeQueryCount = 4,
            .replayReadBytes = 8192,
            .replayOperatorStatistics
            = {{fingerprint,
                ReplayOperatorStatistics{
                    .nodeFingerprint = fingerprint,
                    .inputTuples = 128,
                    .outputTuples = 32,
                    .taskCount = 4,
                    .executionTimeNanos = 20'000'000}}},
            .recordingStatuses = {recordingStatus}}));
    const auto operatorStatistics = workerCatalog->getReplayOperatorStatistics(Host("worker-1:8080"), fingerprint);
    ASSERT_TRUE(operatorStatistics.has_value());
    EXPECT_DOUBLE_EQ(operatorStatistics->averageExecutionTimeMs(), 5.0);
    EXPECT_DOUBLE_EQ(operatorStatistics->selectivity(), 0.25);
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

TEST(QueryManagerMetricsTest, WorkerStatusSerializationRoundTripsReplayOperatorStatistics)
{
    WorkerStatus status;
    status.after = std::chrono::system_clock::time_point(std::chrono::milliseconds(10));
    status.until = std::chrono::system_clock::time_point(std::chrono::milliseconds(20));
    status.replayMetrics = WorkerStatus::ReplayMetrics{
        .recordingStorageBytes = 1024,
        .recordingFileCount = 3,
        .activeQueryCount = 2,
        .replayReadBytes = 2048,
        .operatorStatistics = {ReplayOperatorStatistics{
            .nodeFingerprint = "selection@worker-1:8080",
            .inputTuples = 200,
            .outputTuples = 50,
            .taskCount = 4,
            .executionTimeNanos = 80'000'000}},
        .recordingStatuses = {Replay::RecordingRuntimeStatus{
            .recordingId = "recording-1",
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/recording-1.bin",
            .lifecycleState = Replay::RecordingLifecycleState::Filling,
            .retentionWindowMs = 60'000,
            .retainedStartWatermark = Timestamp(1000),
            .retainedEndWatermark = Timestamp(2000),
            .fillWatermark = Timestamp(2000),
            .segmentCount = 1,
            .storageBytes = 1024,
            .successorRecordingId = std::nullopt}}};

    WorkerStatusResponse response;
    serializeWorkerStatus(status, &response);
    const auto roundTrip = deserializeWorkerStatus(&response);

    EXPECT_EQ(roundTrip.after, status.after);
    EXPECT_EQ(roundTrip.until, status.until);
    EXPECT_EQ(roundTrip.replayMetrics, status.replayMetrics);
}

TEST(QueryManagerMetricsTest, RefreshWorkerMetricsMirrorsPerRecordingStatusIntoRecordingCatalog)
{
    const auto worker = Host("worker-1:8080");
    const auto recordingId = RecordingId("recording-1");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    QueryManagerState state;
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = worker,
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

    QueryManager queryManager(
        workerCatalog,
        [](const WorkerConfig&)
        {
            WorkerStatus status;
            status.until = std::chrono::system_clock::now();
            status.replayMetrics = WorkerStatus::ReplayMetrics{
                .recordingStorageBytes = 2048,
                .recordingFileCount = 1,
                .activeQueryCount = 0,
                .replayReadBytes = 0,
                .operatorStatistics = {},
                .recordingStatuses = {Replay::RecordingRuntimeStatus{
                    .recordingId = "recording-1",
                    .filePath = "/tmp/REPLAY-NebulaStream/recordings/recording-1.bin",
                    .lifecycleState = Replay::RecordingLifecycleState::Ready,
                    .retentionWindowMs = 60'000,
                    .retainedStartWatermark = Timestamp(1000),
                    .retainedEndWatermark = Timestamp(61'000),
                    .fillWatermark = Timestamp(61'000),
                    .segmentCount = 3,
                    .storageBytes = 2048,
                    .successorRecordingId = std::nullopt}}};
            return std::make_unique<FakeWorkerStatusBackend>(status);
        },
        std::move(state));

    queryManager.refreshWorkerMetrics();

    const auto recording = queryManager.getRecordingCatalog().getRecording(recordingId);
    ASSERT_TRUE(recording.has_value());
    EXPECT_EQ(recording->lifecycleState, std::optional(Replay::RecordingLifecycleState::Ready));
    EXPECT_EQ(recording->retentionWindowMs, std::optional<uint64_t>(60'000));
    EXPECT_EQ(recording->retainedStartWatermark, std::optional<Timestamp>(Timestamp(1000)));
    EXPECT_EQ(recording->retainedEndWatermark, std::optional<Timestamp>(Timestamp(61'000)));
    EXPECT_EQ(recording->fillWatermark, std::optional<Timestamp>(Timestamp(61'000)));
    EXPECT_EQ(recording->segmentCount, std::optional<size_t>(3));
    EXPECT_EQ(recording->storageBytes, std::optional<size_t>(2048));
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
            .originalPlan = activePlan,
            .globalPlan = activePlan,
            .replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt},
            .selectedRecordings = {staleRecordingId},
            .networkExplanations = {},
            .queryPlanRewrite = std::nullopt,
            .replayBoundaries = {}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = staleRecordingId,
            .node = worker,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/stale.bin",
            .structuralFingerprint = "stale",
            .retentionWindowMs = 60'000,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {activeQueryId},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = reusedRecordingId,
            .node = worker,
            .filePath = "/tmp/REPLAY-NebulaStream/recordings/reused.bin",
            .structuralFingerprint = "reused",
            .retentionWindowMs = 60'000,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {DistributedQueryId("other-query")},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

    QueryManager queryManager(workerCatalog, [](const WorkerConfig&) { return std::make_unique<FakeLifecycleBackend>(); }, std::move(state));

    const auto incomingSelection = RecordingSelection{
        .recordingId = incomingRecordingId,
        .node = worker,
        .filePath = "/tmp/REPLAY-NebulaStream/recordings/incoming.bin",
        .structuralFingerprint = "incoming",
        .representation = RecordingRepresentation::BinaryStore,
        .retentionWindowMs = std::nullopt,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    const auto reusedSelection = RecordingSelection{
        .recordingId = reusedRecordingId,
        .node = worker,
        .filePath = "/tmp/REPLAY-NebulaStream/recordings/reused.bin",
        .structuralFingerprint = "reused",
        .representation = RecordingRepresentation::BinaryStore,
        .retentionWindowMs = std::nullopt,
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
                .alternatives = {}}},
            .activeQueryPlanRewrites = {},
            .incomingQueryPlanRewrite = std::nullopt,
            .incomingQueryReplayBoundaries = {}});

    const auto registerResult = queryManager.registerQuery(distributedPlan);
    ASSERT_TRUE(registerResult.has_value()) << registerResult.error().what();

    const auto& recordingCatalog = queryManager.getRecordingCatalog();
    ASSERT_TRUE(recordingCatalog.getQueryMetadata().contains(activeQueryId));
    EXPECT_THAT(recordingCatalog.getQueryMetadata().at(activeQueryId).selectedRecordings, testing::ElementsAre(reusedRecordingId));
    ASSERT_EQ(recordingCatalog.getQueryMetadata().at(activeQueryId).networkExplanations.size(), 1U);
    EXPECT_EQ(
        recordingCatalog.getQueryMetadata().at(activeQueryId).networkExplanations.front().decision,
        RecordingSelectionDecision::ReuseExistingRecording);

    ASSERT_TRUE(recordingCatalog.getRecording(staleRecordingId).has_value());
    EXPECT_TRUE(recordingCatalog.getRecording(staleRecordingId)->ownerQueries.empty());
    EXPECT_EQ(recordingCatalog.getRecording(staleRecordingId)->lifecycleState, std::optional(Replay::RecordingLifecycleState::Draining));
    ASSERT_TRUE(recordingCatalog.getRecording(reusedRecordingId).has_value());
    EXPECT_THAT(
        recordingCatalog.getRecording(reusedRecordingId)->ownerQueries,
        testing::UnorderedElementsAre(DistributedQueryId("other-query"), activeQueryId));
    ASSERT_TRUE(recordingCatalog.getRecording(incomingRecordingId).has_value());
    EXPECT_THAT(recordingCatalog.getRecording(incomingRecordingId)->ownerQueries, testing::ElementsAre(*registerResult));
    EXPECT_EQ(recordingCatalog.getRecording(incomingRecordingId)->lifecycleState, std::optional(Replay::RecordingLifecycleState::Installing));
    EXPECT_FALSE(recordingCatalog.getTimeTravelReadRecording().has_value());
}

TEST(QueryManagerMetricsTest, RegisterQueryPersistsReplayBoundaryMetadataForReplayPlanning)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("incoming-query");
    const auto recordingId = RecordingId("incoming-recording");

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    QueryManager queryManager(workerCatalog, [](const WorkerConfig&) { return std::make_unique<FakeLifecycleBackend>(); });

    const auto incomingPlan = createReplayPlan(queryId, "INCOMING_SOURCE");
    const auto selection = RecordingSelection{
        .recordingId = recordingId,
        .node = worker,
        .filePath = "/tmp/REPLAY-NebulaStream/recordings/incoming.bin",
        .structuralFingerprint = "incoming",
        .representation = RecordingRepresentation::BinaryStore,
        .retentionWindowMs = std::nullopt,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    const auto replayBoundary = QueryRecordingPlanInsertion{
        .selection = selection,
        .materializationEdges = {RecordingRewriteEdge{.parentId = OperatorId(11), .childId = OperatorId(7)}}};
    const auto distributedPlan = DistributedLogicalPlan(
        DecomposedLogicalPlan<Host>{{{worker, {incomingPlan}}}},
        incomingPlan,
        ReplaySpecification{.retentionWindowMs = 300'000, .replayLatencyLimitMs = std::nullopt},
        RecordingSelectionResult{
            .networkExplanations = {RecordingSelectionExplanation{
                .selection = selection,
                .decision = RecordingSelectionDecision::CreateNewRecording,
                .reason = "incoming create",
                .chosenCost = {},
                .alternatives = {}}},
            .selectedRecordings = {selection},
            .explanations = {RecordingSelectionExplanation{
                .selection = selection,
                .decision = RecordingSelectionDecision::CreateNewRecording,
                .reason = "incoming create",
                .chosenCost = {},
                .alternatives = {}}},
            .activeQueryPlanRewrites = {},
            .incomingQueryPlanRewrite = QueryRecordingPlanRewrite{.queryId = {}, .basePlan = incomingPlan, .insertions = {replayBoundary}},
            .incomingQueryReplayBoundaries = {replayBoundary}});

    const auto registerResult = queryManager.registerQuery(distributedPlan);
    ASSERT_TRUE(registerResult.has_value()) << registerResult.error().what();

    const auto& metadata = queryManager.getRecordingCatalog().getQueryMetadata().at(queryId);
    ASSERT_TRUE(metadata.queryPlanRewrite.has_value());
    EXPECT_EQ(metadata.queryPlanRewrite->queryId, queryId.getRawValue());
    EXPECT_EQ(metadata.queryPlanRewrite->basePlan, incomingPlan);
    EXPECT_THAT(metadata.queryPlanRewrite->insertions, testing::ElementsAre(replayBoundary));
    EXPECT_THAT(metadata.replayBoundaries, testing::ElementsAre(replayBoundary));
}

TEST(QueryManagerMetricsTest, RegisterQueryMarksNewRecordingInstallingUntilWorkerReportsReady)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto recordingId = RecordingId("incoming-recording");
    const auto filePath = "/tmp/REPLAY-NebulaStream/recordings/incoming.bin";

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    QueryManager queryManager(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); });

    const auto queryId = DistributedQueryId("incoming-query");
    const auto incomingPlan = createReplayPlan(queryId, "INCOMING_SOURCE");
    const auto selection = RecordingSelection{
        .recordingId = recordingId,
        .node = worker,
        .filePath = filePath,
        .structuralFingerprint = "incoming",
        .representation = RecordingRepresentation::BinaryStore,
        .retentionWindowMs = std::nullopt,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    const auto distributedPlan = DistributedLogicalPlan(
        DecomposedLogicalPlan<Host>{{{worker, {incomingPlan}}}},
        incomingPlan,
        ReplaySpecification{.retentionWindowMs = 300'000, .replayLatencyLimitMs = std::nullopt},
        RecordingSelectionResult{
            .networkExplanations = {RecordingSelectionExplanation{
                .selection = selection,
                .decision = RecordingSelectionDecision::CreateNewRecording,
                .reason = "incoming create",
                .chosenCost = {},
                .alternatives = {}}},
            .selectedRecordings = {selection},
            .explanations = {RecordingSelectionExplanation{
                .selection = selection,
                .decision = RecordingSelectionDecision::CreateNewRecording,
                .reason = "incoming create",
                .chosenCost = {},
                .alternatives = {}}},
            .activeQueryPlanRewrites = {},
            .incomingQueryPlanRewrite = std::nullopt,
            .incomingQueryReplayBoundaries = {}});

    const auto registerResult = queryManager.registerQuery(distributedPlan);
    ASSERT_TRUE(registerResult.has_value()) << registerResult.error().what();

    const auto recording = queryManager.getRecordingCatalog().getRecording(recordingId);
    ASSERT_TRUE(recording.has_value());
    EXPECT_EQ(recording->lifecycleState, std::optional(Replay::RecordingLifecycleState::Installing));
    EXPECT_FALSE(queryManager.getRecordingCatalog().getTimeTravelReadRecording().has_value());
    EXPECT_FALSE(std::filesystem::exists(Replay::getTimeTravelReadAliasPath()));
}

TEST(QueryManagerMetricsTest, RefreshWorkerMetricsActivatesReadyRecordingAndUnregisterDrainsIt)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto recordingId = RecordingId("incoming-recording");
    const auto filePath = "/tmp/REPLAY-NebulaStream/recordings/incoming.bin";

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    QueryManager queryManager(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); });

    const auto queryId = DistributedQueryId("incoming-query");
    const auto incomingPlan = createReplayPlan(queryId, "INCOMING_SOURCE");
    const auto selection = RecordingSelection{
        .recordingId = recordingId,
        .node = worker,
        .filePath = filePath,
        .structuralFingerprint = "incoming",
        .representation = RecordingRepresentation::BinaryStore,
        .retentionWindowMs = std::nullopt,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    const auto distributedPlan = DistributedLogicalPlan(
        DecomposedLogicalPlan<Host>{{{worker, {incomingPlan}}}},
        incomingPlan,
        ReplaySpecification{.retentionWindowMs = 300'000, .replayLatencyLimitMs = std::nullopt},
        RecordingSelectionResult{
            .networkExplanations = {RecordingSelectionExplanation{
                .selection = selection,
                .decision = RecordingSelectionDecision::CreateNewRecording,
                .reason = "incoming create",
                .chosenCost = {},
                .alternatives = {}}},
            .selectedRecordings = {selection},
            .explanations = {RecordingSelectionExplanation{
                .selection = selection,
                .decision = RecordingSelectionDecision::CreateNewRecording,
                .reason = "incoming create",
                .chosenCost = {},
                .alternatives = {}}},
            .activeQueryPlanRewrites = {},
            .incomingQueryPlanRewrite = std::nullopt,
            .incomingQueryReplayBoundaries = {}});

    const auto registerResult = queryManager.registerQuery(distributedPlan);
    ASSERT_TRUE(registerResult.has_value()) << registerResult.error().what();

    WorkerStatus readyStatus;
    readyStatus.until = std::chrono::system_clock::now();
    readyStatus.replayMetrics = WorkerStatus::ReplayMetrics{
        .recordingStorageBytes = 2048,
        .recordingFileCount = 1,
        .activeQueryCount = 1,
        .replayReadBytes = 0,
        .operatorStatistics = {},
        .recordingStatuses = {Replay::RecordingRuntimeStatus{
            .recordingId = recordingId.getRawValue(),
            .filePath = filePath,
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retentionWindowMs = 300'000,
            .retainedStartWatermark = Timestamp(1000),
            .retainedEndWatermark = Timestamp(301'000),
            .fillWatermark = Timestamp(301'000),
            .segmentCount = 4,
            .storageBytes = 2048,
            .successorRecordingId = std::nullopt}}};
    backendState->workerStatusResult = readyStatus;

    queryManager.refreshWorkerMetrics();

    auto recording = queryManager.getRecordingCatalog().getRecording(recordingId);
    ASSERT_TRUE(recording.has_value());
    EXPECT_EQ(recording->lifecycleState, std::optional(Replay::RecordingLifecycleState::Ready));
    ASSERT_TRUE(queryManager.getRecordingCatalog().getTimeTravelReadRecording().has_value());
    EXPECT_EQ(queryManager.getRecordingCatalog().getTimeTravelReadRecording()->id, recordingId);
    EXPECT_TRUE(std::filesystem::is_symlink(Replay::getTimeTravelReadAliasPath()));
    EXPECT_EQ(Replay::resolveTimeTravelReadProbePath(), filePath);

    const auto unregisterResult = queryManager.unregister(*registerResult);
    ASSERT_TRUE(unregisterResult.has_value());

    recording = queryManager.getRecordingCatalog().getRecording(recordingId);
    ASSERT_TRUE(recording.has_value());
    EXPECT_TRUE(recording->ownerQueries.empty());
    EXPECT_EQ(recording->lifecycleState, std::optional(Replay::RecordingLifecycleState::Draining));
    EXPECT_FALSE(queryManager.getRecordingCatalog().getTimeTravelReadRecording().has_value());
    EXPECT_FALSE(std::filesystem::exists(Replay::getTimeTravelReadAliasPath()));
}

TEST(QueryManagerMetricsTest, ReplayStatementHandlerRegistersPlannedReplayExecutionForReplayableQuery)
{
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("replayable-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = std::nullopt,
            .globalPlan = std::nullopt,
            .replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000},
            .selectedRecordings = {},
            .networkExplanations = {},
            .queryPlanRewrite = std::nullopt,
            .replayBoundaries = {}});

    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [](const WorkerConfig&) { return std::make_unique<FakeLifecycleBackend>(); },
        std::move(state));
    QueryStatementHandler handler(queryManager, nullptr, nullptr);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 1'000, .intervalEndMs = 5'000});
    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_EQ(result->execution.queryId, queryId);
    EXPECT_EQ(result->execution.intervalStartMs, 1'000U);
    EXPECT_EQ(result->execution.intervalEndMs, 5'000U);
    EXPECT_EQ(result->execution.state, ReplayExecutionState::Planned);
    EXPECT_TRUE(result->execution.selectedRecordingIds.empty());
    EXPECT_TRUE(result->execution.pinnedSegments.empty());
    EXPECT_TRUE(result->execution.internalQueryIds.empty());
    EXPECT_TRUE(queryManager->getReplayExecutions().contains(result->execution.id));
    EXPECT_EQ(queryManager->getReplayExecutions().at(result->execution.id), result->execution);
}

TEST(QueryManagerMetricsTest, ReplayStatementHandlerRejectsNonReplayableQuery)
{
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("plain-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = std::nullopt,
            .globalPlan = std::nullopt,
            .replaySpecification = std::nullopt,
            .selectedRecordings = {},
            .networkExplanations = {},
            .queryPlanRewrite = std::nullopt,
            .replayBoundaries = {}});

    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [](const WorkerConfig&) { return std::make_unique<FakeLifecycleBackend>(); },
        std::move(state));
    QueryStatementHandler handler(queryManager, nullptr, nullptr);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 1'000, .intervalEndMs = 5'000});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::UnsupportedQuery);
}

TEST(QueryManagerMetricsTest, QueryStatementHandlerRedeploysActiveQueryForActiveOnlyRecordingCreation)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto activeQueryId = DistributedQueryId("active-query");
    const auto activeLocalQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), activeQueryId);

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}, {}, 1024 * 1024));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    addSourceAndSinkCatalogEntries(sourceCatalog, sinkCatalog, worker, "ACTIVE_SOURCE", "active_sink");
    addSourceAndSinkCatalogEntries(sourceCatalog, sinkCatalog, worker, "INCOMING_SOURCE", "incoming_sink");

    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    const auto activeOriginalPlan = createReplayPlan(activeQueryId, "ACTIVE_SOURCE", "active_sink");
    const auto activeOptimizedPlan = optimizer->optimize(
        activeOriginalPlan,
        ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt},
        RecordingCatalog{});

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->statuses.insert_or_assign(
        activeLocalQueryId, LocalQueryStatus{.queryId = activeLocalQueryId, .state = QueryState::Running});
    backendState->registrations[activeQueryId] = 1;
    backendState->starts[activeQueryId] = 1;

    QueryManagerState state;
    state.queries.emplace(activeQueryId, DistributedQuery({{worker, {activeLocalQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        activeQueryId,
        ReplayableQueryMetadata{
            .originalPlan = activeOriginalPlan,
            .globalPlan = activeOptimizedPlan.getGlobalPlan(),
            .replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt},
            .selectedRecordings = {},
            .networkExplanations = {},
            .queryPlanRewrite = std::nullopt,
            .replayBoundaries = {}});

    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto incomingPlan = createReplayPlan(DistributedQueryId("incoming-query"), "INCOMING_SOURCE", "incoming_sink");
    const auto result = handler(QueryStatement{
        .plan = incomingPlan,
        .replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt}});
    ASSERT_TRUE(result.has_value()) << result.error().what();
    const auto incomingQueryId = result->id;

    EXPECT_EQ(backendState->registrations[activeQueryId], 1U);
    EXPECT_EQ(backendState->starts[activeQueryId], 1U);
    EXPECT_EQ(backendState->stops[activeQueryId], 0U);
    EXPECT_EQ(backendState->unregisters[activeQueryId], 0U);
    EXPECT_EQ(backendState->registrations[incomingQueryId], 1U);
    EXPECT_EQ(backendState->starts[incomingQueryId], 1U);
    EXPECT_THAT(queryManager->queries(), testing::UnorderedElementsAre(activeQueryId, incomingQueryId));

    const auto& recordingCatalog = queryManager->getRecordingCatalog();
    ASSERT_TRUE(recordingCatalog.getQueryMetadata().contains(activeQueryId));
    EXPECT_EQ(recordingCatalog.getQueryMetadata().at(activeQueryId).originalPlan->getQueryId().getDistributedQueryId(), activeQueryId);
    EXPECT_TRUE(recordingCatalog.getQueryMetadata().at(activeQueryId).selectedRecordings.empty());
    ASSERT_TRUE(recordingCatalog.getQueryMetadata().at(activeQueryId).globalPlan.has_value());
    EXPECT_EQ(getOperatorByType<StoreLogicalOperator>(*recordingCatalog.getQueryMetadata().at(activeQueryId).globalPlan).size(), 1U);

    const auto internalQueryIds = recordingCatalog.getQueryMetadata() | std::views::keys
        | std::views::filter([&](const auto& queryId) { return queryId != activeQueryId && queryId != incomingQueryId; })
        | std::ranges::to<std::vector>();
    ASSERT_THAT(internalQueryIds, testing::SizeIs(1));
    const auto& internalQueryId = internalQueryIds.front();
    EXPECT_FALSE(std::ranges::contains(queryManager->queries(), internalQueryId));
    ASSERT_FALSE(recordingCatalog.getQueryMetadata().at(internalQueryId).selectedRecordings.empty());
    const auto activeRecordingId = recordingCatalog.getQueryMetadata().at(internalQueryId).selectedRecordings.front();
    ASSERT_TRUE(recordingCatalog.getRecording(activeRecordingId).has_value());
    EXPECT_THAT(recordingCatalog.getRecording(activeRecordingId)->ownerQueries, testing::ElementsAre(internalQueryId));

    const auto unregisterResult = queryManager->unregister(activeQueryId);
    ASSERT_TRUE(unregisterResult.has_value());
    EXPECT_EQ(backendState->unregisters[internalQueryId], 1U);
}

TEST(QueryManagerMetricsTest, QueryStatementHandlerRedeploysActiveQueryForActiveOnlyRecordingReuse)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto activeQueryId = DistributedQueryId("active-query");
    const auto activeLocalQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), activeQueryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = std::nullopt};

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}, {}, 1024 * 1024));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    addSourceAndSinkCatalogEntries(sourceCatalog, sinkCatalog, worker, "ACTIVE_SOURCE", "active_sink");
    addSourceAndSinkCatalogEntries(sourceCatalog, sinkCatalog, worker, "INCOMING_SOURCE", "incoming_sink");

    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    const auto activeOriginalPlan = createReplayPlan(activeQueryId, "ACTIVE_SOURCE", "active_sink");
    const auto activeOptimizedPlan = optimizer->optimize(activeOriginalPlan, replaySpecification, RecordingCatalog{});
    ASSERT_EQ(activeOptimizedPlan.getRecordingSelectionResult().selectedRecordings.size(), 1U);
    ASSERT_EQ(getOperatorByType<StoreLogicalOperator>(activeOptimizedPlan.getGlobalPlan()).size(), 1U);
    const auto activeRecording = activeOptimizedPlan.getRecordingSelectionResult().selectedRecordings.front();

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->statuses.insert_or_assign(
        activeLocalQueryId, LocalQueryStatus{.queryId = activeLocalQueryId, .state = QueryState::Running});
    backendState->registrations[activeQueryId] = 1;
    backendState->starts[activeQueryId] = 1;

    QueryManagerState state;
    state.queries.emplace(activeQueryId, DistributedQuery({{worker, {activeLocalQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        activeQueryId,
        ReplayableQueryMetadata{
            .originalPlan = activeOriginalPlan,
            .globalPlan = activeOptimizedPlan.getGlobalPlan(),
            .replaySpecification = replaySpecification,
            .selectedRecordings = {activeRecording.recordingId},
            .networkExplanations = activeOptimizedPlan.getRecordingSelectionResult().networkExplanations,
            .queryPlanRewrite = std::nullopt,
            .replayBoundaries = {}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = activeRecording.recordingId,
            .node = activeRecording.node,
            .filePath = activeRecording.filePath,
            .structuralFingerprint = activeRecording.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = activeRecording.representation,
            .ownerQueries = {activeQueryId},
            .lifecycleState = std::nullopt,
            .retainedStartWatermark = std::nullopt,
            .retainedEndWatermark = std::nullopt,
            .fillWatermark = std::nullopt,
            .segmentCount = std::nullopt,
            .storageBytes = std::nullopt,
            .successorRecordingId = std::nullopt});

    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto incomingPlan = createReplayPlan(DistributedQueryId("incoming-query"), "INCOMING_SOURCE", "incoming_sink");
    const auto result = handler(QueryStatement{.plan = incomingPlan, .replaySpecification = replaySpecification});
    ASSERT_TRUE(result.has_value()) << result.error().what();
    const auto incomingQueryId = result->id;

    EXPECT_EQ(backendState->registrations[activeQueryId], 1U);
    EXPECT_EQ(backendState->starts[activeQueryId], 1U);
    EXPECT_EQ(backendState->stops[activeQueryId], 0U);
    EXPECT_EQ(backendState->unregisters[activeQueryId], 0U);
    EXPECT_EQ(backendState->registrations[incomingQueryId], 1U);
    EXPECT_EQ(backendState->starts[incomingQueryId], 1U);
    EXPECT_THAT(queryManager->queries(), testing::UnorderedElementsAre(activeQueryId, incomingQueryId));

    const auto& recordingCatalog = queryManager->getRecordingCatalog();
    ASSERT_TRUE(recordingCatalog.getQueryMetadata().contains(activeQueryId));
    EXPECT_THAT(recordingCatalog.getQueryMetadata().at(activeQueryId).selectedRecordings, testing::ElementsAre(activeRecording.recordingId));
    ASSERT_TRUE(recordingCatalog.getQueryMetadata().at(activeQueryId).globalPlan.has_value());
    EXPECT_EQ(getOperatorByType<StoreLogicalOperator>(*recordingCatalog.getQueryMetadata().at(activeQueryId).globalPlan).size(), 1U);
    EXPECT_EQ(recordingCatalog.getQueryMetadata().size(), 2U);
}

TEST(QueryManagerMetricsTest, TimeTravelReadResolverTargetsConcreteRecordingFile)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto tempDir = std::filesystem::temp_directory_path()
        / ("time-travel-read-resolver-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(tempDir);

    const auto recordingFilePath = tempDir / "resolver-recording.bin";
    const auto manifestPath = Replay::getRecordingManifestPath(recordingFilePath.string());
    {
        std::ofstream manifest(manifestPath);
        ASSERT_TRUE(manifest.good());
        manifest << Replay::BINARY_STORE_MANIFEST_MAGIC << '\n';
    }

    Replay::updateTimeTravelReadAlias(recordingFilePath.string());

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    ASSERT_TRUE(sourceCatalog->addLogicalSource("fallback", createSchema()).has_value());

    auto plan = LogicalPlanBuilder::createLogicalPlan("TIME_TRAVEL_READ");
    resolveTimeTravelReadSources(plan, sourceCatalog);

    const auto inlineSources = getOperatorByType<InlineSourceLogicalOperator>(plan);
    ASSERT_THAT(inlineSources, testing::SizeIs(1));
    const auto sourceConfig = inlineSources.front().get().getSourceConfig();
    EXPECT_EQ(sourceConfig.at("file_path"), recordingFilePath.string());
    EXPECT_EQ(sourceConfig.at("recording_id"), "resolver-recording");

    std::error_code errorCode;
    std::filesystem::remove_all(tempDir, errorCode);
}

}
