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
#include <functional>
#include <fstream>
#include <optional>
#include <string>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Listeners/QueryLog.hpp>
#include <LegacyOptimizer.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <QueryId.hpp>
#include <Replay/ReplayStorage.hpp>
#include <ReplayCheckpointPlanning.hpp>
#include <Replay/TimeTravelReadResolver.hpp>
#include <RecordingSelectionResult.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Traits/PlacementTrait.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

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

    [[nodiscard]] std::expected<QueryId, Exception>
    registerQuery(
        LogicalPlan,
        std::optional<ReplayCheckpointRegistration> = std::nullopt,
        std::optional<ReplayQueryRuntimeControl> = std::nullopt) override
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

    [[nodiscard]] std::expected<std::vector<uint64_t>, Exception>
    selectReplaySegments(const std::string&, const Replay::BinaryStoreReplaySelection&) const override
    {
        return std::unexpected(PlacementFailure("unused in FakeWorkerStatusBackend"));
    }

    [[nodiscard]] std::expected<std::vector<uint64_t>, Exception>
    pinReplaySegments(const std::string&, const std::vector<uint64_t>&) override
    {
        return std::unexpected(PlacementFailure("unused in FakeWorkerStatusBackend"));
    }

    std::expected<void, Exception> unpinReplaySegments(const std::string&, const std::vector<uint64_t>&) override
    {
        return std::unexpected(PlacementFailure("unused in FakeWorkerStatusBackend"));
    }
};

class FakeLifecycleBackend final : public QuerySubmissionBackend
{
    size_t nextQueryId = 1;

public:
    [[nodiscard]] std::expected<QueryId, Exception>
    registerQuery(
        LogicalPlan plan,
        std::optional<ReplayCheckpointRegistration> = std::nullopt,
        std::optional<ReplayQueryRuntimeControl> = std::nullopt) override
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

    [[nodiscard]] std::expected<std::vector<uint64_t>, Exception>
    selectReplaySegments(const std::string& recordingFilePath, const Replay::BinaryStoreReplaySelection& selection) const override
    {
        try
        {
            const auto selectedSegments = Replay::selectBinaryStoreSegments(recordingFilePath, selection);
            return selectedSegments | std::views::transform([](const auto& segment) { return segment.segmentId; })
                | std::ranges::to<std::vector>();
        }
        catch (const Exception& exception)
        {
            return std::unexpected(exception);
        }
    }

    [[nodiscard]] std::expected<std::vector<uint64_t>, Exception>
    pinReplaySegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds) override
    {
        try
        {
            return Replay::pinBinaryStoreSegments(recordingFilePath, segmentIds);
        }
        catch (const Exception& exception)
        {
            return std::unexpected(exception);
        }
    }

    std::expected<void, Exception> unpinReplaySegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds) override
    {
        try
        {
            Replay::unpinBinaryStoreSegments(recordingFilePath, segmentIds);
            return {};
        }
        catch (const Exception& exception)
        {
            return std::unexpected(exception);
        }
    }
};

struct ReplaySegmentSelectionCall
{
    std::string recordingFilePath;
    Replay::BinaryStoreReplaySelection selection;
};

struct ReplaySegmentPinCall
{
    std::string recordingFilePath;
    std::vector<uint64_t> segmentIds;
};

struct TrackingLifecycleBackendState
{
    size_t nextUuidIndex = 2;
    std::unordered_map<QueryId, LocalQueryStatus> statuses;
    std::unordered_map<DistributedQueryId, size_t> registrations;
    std::unordered_map<DistributedQueryId, size_t> starts;
    std::unordered_map<DistributedQueryId, size_t> stops;
    std::unordered_map<DistributedQueryId, size_t> unregisters;
    std::unordered_map<DistributedQueryId, LogicalPlan> registeredPlans;
    std::unordered_map<DistributedQueryId, std::vector<std::optional<ReplayCheckpointRegistration>>> registeredReplayCheckpoints;
    std::vector<ReplaySegmentSelectionCall> replaySelections;
    std::vector<ReplaySegmentPinCall> replayPins;
    std::vector<ReplaySegmentPinCall> replayUnpins;
    std::optional<std::string> replayCheckpointRegistrationFailureMessage;
    std::unordered_map<DistributedQueryId, std::vector<std::optional<ReplayQueryRuntimeControl>>> registeredReplayRuntimeControls;
    bool autoStopRunningQueriesOnStatus = false;
    size_t workerStatusCallCount = 0;
    size_t statusCallCount = 0;
    std::optional<size_t> stopRunningQueriesAfterWorkerStatusCallCount;
    std::optional<size_t> fallbackStopRunningQueriesAfterStatusCallCount;
    std::function<std::expected<WorkerStatus, Exception>(const TrackingLifecycleBackendState&)> workerStatusProvider;
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

    [[nodiscard]] std::expected<QueryId, Exception>
    registerQuery(
        LogicalPlan plan,
        std::optional<ReplayCheckpointRegistration> replayCheckpointRegistration = std::nullopt,
        std::optional<ReplayQueryRuntimeControl> replayRuntimeControl = std::nullopt) override
    {
        if (replayCheckpointRegistration.has_value() && replayCheckpointRegistration->restoreCheckpoint.has_value()
            && state->replayCheckpointRegistrationFailureMessage.has_value())
        {
            return std::unexpected(QueryRegistrationFailed("{}", *state->replayCheckpointRegistrationFailureMessage));
        }
        const auto queryId = QueryId::create(state->nextLocalQueryId(), plan.getQueryId().getDistributedQueryId());
        state->statuses.insert_or_assign(queryId, LocalQueryStatus{.queryId = queryId, .state = QueryState::Registered});
        ++state->registrations[plan.getQueryId().getDistributedQueryId()];
        state->registeredPlans.insert_or_assign(plan.getQueryId().getDistributedQueryId(), plan);
        state->registeredReplayCheckpoints[plan.getQueryId().getDistributedQueryId()].push_back(std::move(replayCheckpointRegistration));
        state->registeredReplayRuntimeControls[plan.getQueryId().getDistributedQueryId()].push_back(std::move(replayRuntimeControl));
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
        ++state->statusCallCount;
        if (const auto it = state->statuses.find(queryId); it != state->statuses.end())
        {
            const auto stopOnReplayPhase = state->stopRunningQueriesAfterWorkerStatusCallCount.has_value()
                && state->workerStatusCallCount >= *state->stopRunningQueriesAfterWorkerStatusCallCount;
            const auto stopOnFallback = state->fallbackStopRunningQueriesAfterStatusCallCount.has_value()
                && state->statusCallCount >= *state->fallbackStopRunningQueriesAfterStatusCallCount;
            if ((state->autoStopRunningQueriesOnStatus || stopOnReplayPhase || stopOnFallback) && it->second.state == QueryState::Running)
            {
                it->second.state = QueryState::Stopped;
            }
            return it->second;
        }
        return std::unexpected(QueryStatusFailed("unknown query {}", queryId));
    }

    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point) const override
    {
        ++state->workerStatusCallCount;
        if (state->workerStatusProvider)
        {
            return state->workerStatusProvider(*state);
        }
        return state->workerStatusResult;
    }

    [[nodiscard]] std::expected<std::vector<uint64_t>, Exception>
    selectReplaySegments(const std::string& recordingFilePath, const Replay::BinaryStoreReplaySelection& selection) const override
    {
        state->replaySelections.push_back(ReplaySegmentSelectionCall{.recordingFilePath = recordingFilePath, .selection = selection});
        try
        {
            const auto selectedSegments = Replay::selectBinaryStoreSegments(recordingFilePath, selection);
            return selectedSegments | std::views::transform([](const auto& segment) { return segment.segmentId; })
                | std::ranges::to<std::vector>();
        }
        catch (const Exception& exception)
        {
            return std::unexpected(exception);
        }
    }

    [[nodiscard]] std::expected<std::vector<uint64_t>, Exception>
    pinReplaySegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds) override
    {
        state->replayPins.push_back(ReplaySegmentPinCall{.recordingFilePath = recordingFilePath, .segmentIds = segmentIds});
        try
        {
            return Replay::pinBinaryStoreSegments(recordingFilePath, segmentIds);
        }
        catch (const Exception& exception)
        {
            return std::unexpected(exception);
        }
    }

    std::expected<void, Exception> unpinReplaySegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds) override
    {
        state->replayUnpins.push_back(ReplaySegmentPinCall{.recordingFilePath = recordingFilePath, .segmentIds = segmentIds});
        try
        {
            Replay::unpinBinaryStoreSegments(recordingFilePath, segmentIds);
            return {};
        }
        catch (const Exception& exception)
        {
            return std::unexpected(exception);
        }
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

LogicalPlan createPlacedWindowReplayPlan(
    const DistributedQueryId& queryId,
    const Host& worker,
    const Windowing::TimeCharacteristic& timeCharacteristic,
    const std::string& sinkName)
{
    const auto placement = TraitSet{PlacementTrait(worker)};
    const auto source = LogicalOperator(InlineSourceLogicalOperator("INLINE_REPLAY_SOURCE", createSchema(), {}, {}).withTraitSet(placement));
    const auto watermarkAssigner = [&]() -> LogicalOperator
    {
        if (timeCharacteristic.getType() == Windowing::TimeCharacteristic::Type::EventTime)
        {
            return EventTimeWatermarkAssignerLogicalOperator(
                       FieldAccessLogicalFunction(timeCharacteristic.field.name),
                       timeCharacteristic.getTimeUnit())
                .withTraitSet(placement)
                .withInferredSchema({source.getOutputSchema()})
                .withChildren({source});
        }
        return IngestionTimeWatermarkAssignerLogicalOperator()
            .withTraitSet(placement)
            .withInferredSchema({source.getOutputSchema()})
            .withChildren({source});
    }();

    const auto window = LogicalOperator(
        WindowedAggregationLogicalOperator(
            {},
            {std::make_shared<WindowAggregationLogicalFunction>(CountAggregationLogicalFunction(FieldAccessLogicalFunction("id")))},
            std::make_shared<Windowing::TumblingWindow>(timeCharacteristic, Windowing::TimeMeasure(1'000)))
            .withTraitSet(placement)
            .withInferredSchema({watermarkAssigner.getOutputSchema()})
            .withChildren({watermarkAssigner}));
    const auto sink = LogicalOperator(
        SinkLogicalOperator(sinkName).withTraitSet(placement).withInferredSchema({window.getOutputSchema()}).withChildren({window}));
    return LogicalPlan(QueryId::createDistributed(queryId), {sink});
}

LogicalPlan createPlacedEventTimeReplayPlan(const DistributedQueryId& queryId, const Host& worker, const std::string& sinkName)
{
    const auto placement = TraitSet{PlacementTrait(worker)};
    const auto source = LogicalOperator(InlineSourceLogicalOperator("INLINE_REPLAY_SOURCE", createSchema(), {}, {}).withTraitSet(placement));
    const auto watermarkAssigner = LogicalOperator(
        EventTimeWatermarkAssignerLogicalOperator(FieldAccessLogicalFunction("id"), Windowing::TimeUnit::Milliseconds())
            .withTraitSet(placement)
            .withInferredSchema({source.getOutputSchema()})
            .withChildren({source}));
    const auto sink = LogicalOperator(
        SinkLogicalOperator(sinkName).withTraitSet(placement).withInferredSchema({watermarkAssigner.getOutputSchema()}).withChildren(
            {watermarkAssigner}));
    return LogicalPlan(QueryId::createDistributed(queryId), {sink});
}

LogicalPlan createReplayPlan(
    const DistributedQueryId& queryId, const std::string& sourceName, const std::string& sinkName = "test_sink")
{
    auto plan = LogicalPlanBuilder::createLogicalPlan(sourceName);
    plan = LogicalPlanBuilder::addSink(sinkName, plan);
    plan.setQueryId(QueryId::createDistributed(queryId));
    return plan;
}

std::filesystem::path createReplayRecordingFixturePath(const std::string& prefix)
{
    const auto tempDir = std::filesystem::temp_directory_path()
        / (prefix + "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(tempDir);
    return tempDir / "recording.bin";
}

void initializeReplayRecordingFixture(
    const std::filesystem::path& recordingFilePath, const std::vector<Replay::BinaryStoreManifestEntry>& manifestEntries)
{
    std::filesystem::create_directories(recordingFilePath.parent_path());
    {
        std::ofstream recordingFile(recordingFilePath, std::ios::binary);
        ASSERT_TRUE(recordingFile.good());
    }
    Replay::writeBinaryStoreManifest(
        recordingFilePath.string(),
        Replay::BinaryStoreManifest{
            .retentionWindowMs = 60'000,
            .successorRecordingId = std::nullopt,
            .segments = manifestEntries});
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
                .recordingStatuses = {recordingStatus},
                .replayCheckpoints = {},
                .replayQueryStatuses = {}};
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
            .recordingStatuses = {recordingStatus},
            .replayCheckpoints = {}}));
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
            .successorRecordingId = std::nullopt}},
        .replayCheckpoints = {ReplayCheckpointStatus{
            .queryId = QueryId::create(
                LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")),
                DistributedQueryId("query-1")),
            .bundleName = "plan_query-1_replay_checkpoint_00000000000000001000",
            .planFingerprint = "fingerprint-1",
            .replayRestoreFingerprint = "restore-fingerprint-1",
            .checkpointWatermarkMs = 1000}},
        .replayQueryStatuses = {ReplayQueryStatus{
            .queryId = QueryId::create(
                LocalQueryId(std::string("00000000-0000-0000-0000-000000000009")),
                DistributedQueryId("query-9")),
            .phase = ReplayQueryPhase::Emitting,
            .lastOutputWatermarkMs = 1500}}};

    WorkerStatusResponse response;
    serializeWorkerStatus(status, &response);
    const auto roundTrip = deserializeWorkerStatus(&response);

    EXPECT_EQ(roundTrip.after, status.after);
    EXPECT_EQ(roundTrip.until, status.until);
    EXPECT_EQ(roundTrip.replayMetrics, status.replayMetrics);
}

TEST(QueryManagerMetricsTest, RefreshWorkerMetricsMirrorsReplayCheckpointStatusIntoWorkerCatalog)
{
    const auto worker = Host("worker-1:8080");
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    QueryManager queryManager(
        workerCatalog,
        [](const WorkerConfig&)
        {
            WorkerStatus status;
            status.until = std::chrono::system_clock::now();
            status.replayMetrics = WorkerStatus::ReplayMetrics{
                .recordingStorageBytes = 0,
                .recordingFileCount = 0,
                .activeQueryCount = 0,
                .replayReadBytes = 0,
                .operatorStatistics = {},
                .recordingStatuses = {},
                .replayCheckpoints = {ReplayCheckpointStatus{
                    .queryId = QueryId::create(
                        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")),
                        DistributedQueryId("query-1")),
                    .bundleName = "plan_query-1_replay_checkpoint_00000000000000001000",
                    .planFingerprint = "fingerprint-1",
                    .replayRestoreFingerprint = "restore-fingerprint-1",
                    .checkpointWatermarkMs = 1000}},
                .replayQueryStatuses = {}};
            return std::make_unique<FakeWorkerStatusBackend>(status);
        });

    queryManager.refreshWorkerMetrics();

    const auto metrics = workerCatalog->getWorkerRuntimeMetrics(worker);
    ASSERT_TRUE(metrics.has_value());
    ASSERT_THAT(metrics->replayCheckpoints, testing::SizeIs(1));
    EXPECT_EQ(metrics->replayCheckpoints.front().queryId.getDistributedQueryId(), DistributedQueryId("query-1"));
    EXPECT_EQ(metrics->replayCheckpoints.front().bundleName, "plan_query-1_replay_checkpoint_00000000000000001000");
    EXPECT_EQ(metrics->replayCheckpoints.front().planFingerprint, "fingerprint-1");
    EXPECT_EQ(metrics->replayCheckpoints.front().checkpointWatermarkMs, 1000U);
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
                    .successorRecordingId = std::nullopt}},
                .replayCheckpoints = {}};
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
            .successorRecordingId = std::nullopt}},
        .replayCheckpoints = {}};
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

TEST(QueryManagerMetricsTest, ReplayStatementHandlerExecutesReplayExecutionToCompletionForReplayableQuery)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("replayable-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000};
    const auto recordingFilePath = createReplayRecordingFixturePath("replay-phase-5");
    initializeReplayRecordingFixture(
        recordingFilePath,
        {
            Replay::BinaryStoreManifestEntry{.segmentId = 11, .payloadOffset = 128, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 0, .maxWatermark = 1'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 12, .payloadOffset = 192, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 2'000, .maxWatermark = 3'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 13, .payloadOffset = 256, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 4'000, .maxWatermark = 5'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 14, .payloadOffset = 320, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 6'000, .maxWatermark = 7'999},
        });

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    const auto originalPlan = createPlacedWindowReplayPlan(
        queryId,
        worker,
        Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction("id")),
        "replay_sink");
    const auto sink = originalPlan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto window = sink.getChildren().front();
    ASSERT_TRUE(window.tryGetAs<WindowedAggregationLogicalOperator>().has_value());
    ASSERT_EQ(window.getChildren().size(), 1U);
    const auto assigner = window.getChildren().front();
    ASSERT_TRUE(assigner.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value());
    const auto selectedRecording = RecordingSelection{
        .recordingId = RecordingId("replayable-query-recording"),
        .node = worker,
        .filePath = recordingFilePath.string(),
        .structuralFingerprint = "replayable-window-boundary",
        .representation = RecordingRepresentation::BinaryStore,
        .retentionWindowMs = replaySpecification.retentionWindowMs,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    const auto replayBoundary = QueryRecordingPlanInsertion{
        .selection = selectedRecording,
        .materializationEdges = {RecordingRewriteEdge{.parentId = window.getId(), .childId = assigner.getId()}}};
    const auto replayCheckpointBoundaryRewrite = QueryRecordingPlanRewrite{
        .queryId = queryId.getRawValue(),
        .basePlan = originalPlan,
        .insertions = {replayBoundary}};
    const auto replayCheckpointBoundaries = buildReplayCheckpointBoundaries(replayCheckpointBoundaryRewrite);
    ASSERT_TRUE(replayCheckpointBoundaries.has_value()) << replayCheckpointBoundaries.error().what();
    const auto replayCheckpointFingerprints = computeReplayCheckpointFingerprints(optimizer->decomposePlacedPlan(originalPlan), *replayCheckpointBoundaries);
    ASSERT_TRUE(replayCheckpointFingerprints.has_value()) << replayCheckpointFingerprints.error().what();
    ASSERT_TRUE(replayCheckpointFingerprints->contains(worker));
    ASSERT_THAT(replayCheckpointFingerprints->at(worker), testing::SizeIs(1));
    ASSERT_TRUE(replayCheckpointFingerprints->at(worker).front().has_value());
    const auto replayRestoreFingerprint = *replayCheckpointFingerprints->at(worker).front();

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = originalPlan,
            .globalPlan = originalPlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {selectedRecording.recordingId},
            .networkExplanations = {},
            .queryPlanRewrite = QueryRecordingPlanRewrite{
                .queryId = queryId.getRawValue(),
                .basePlan = originalPlan,
                .insertions = {}},
            .replayBoundaries = {replayBoundary},
            .replayCheckpointRequirements = {ReplayCheckpointRequirement{
                .host = worker.getRawValue(),
                .replayRestoreFingerprint = replayRestoreFingerprint}}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = selectedRecording.recordingId,
            .node = selectedRecording.node,
            .filePath = recordingFilePath.string(),
            .structuralFingerprint = selectedRecording.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = selectedRecording.representation,
            .ownerQueries = {queryId},
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retainedStartWatermark = Timestamp(0),
            .retainedEndWatermark = Timestamp(70'000),
            .fillWatermark = Timestamp(70'000),
            .segmentCount = 4,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt});

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->autoStopRunningQueriesOnStatus = true;
    backendState->workerStatusResult = [](
                                         const QueryId& queryId,
                                         const RecordingId& recordingId,
                                         const std::filesystem::path& recordingFilePath,
                                         const std::string& replayRestoreFingerprint) -> std::expected<WorkerStatus, Exception>
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        status.replayMetrics.recordingStatuses.push_back(
            Replay::RecordingRuntimeStatus{
                .recordingId = recordingId.getRawValue(),
                .filePath = recordingFilePath.string(),
                .lifecycleState = Replay::RecordingLifecycleState::Ready,
                .retentionWindowMs = 60'000,
                .retainedStartWatermark = Timestamp(0),
                .retainedEndWatermark = Timestamp(70'000),
                .fillWatermark = Timestamp(70'000),
                .segmentCount = 4,
                .storageBytes = 4096,
                .successorRecordingId = std::nullopt});
        status.replayMetrics.replayCheckpoints.push_back(
            ReplayCheckpointStatus{
                .queryId = queryId,
                .bundleName = "plan_replayable-query_replay_checkpoint_00000000000000000500",
                .planFingerprint = "replay-checkpoint-fingerprint",
                .replayRestoreFingerprint = replayRestoreFingerprint,
                .checkpointWatermarkMs = 500});
        return status;
    }(localQueryId, selectedRecording.recordingId, recordingFilePath, replayRestoreFingerprint);
    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 1'000, .intervalEndMs = 5'000});
    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_EQ(result->execution.queryId, queryId);
    EXPECT_EQ(result->execution.intervalStartMs, 1'000U);
    EXPECT_EQ(result->execution.intervalEndMs, 5'000U);
    EXPECT_EQ(result->execution.state, ReplayExecutionState::Done);
    ASSERT_THAT(result->execution.selectedCheckpoints, testing::SizeIs(1));
    EXPECT_EQ(result->execution.selectedCheckpoints.front().host, worker.getRawValue());
    EXPECT_EQ(
        result->execution.selectedCheckpoints.front().bundleName,
        "plan_replayable-query_replay_checkpoint_00000000000000000500");
    EXPECT_EQ(result->execution.selectedCheckpoints.front().planFingerprint, "replay-checkpoint-fingerprint");
    EXPECT_EQ(result->execution.selectedCheckpoints.front().checkpointWatermarkMs, 500U);
    EXPECT_EQ(result->execution.warmupStartMs, 500U);
    EXPECT_THAT(result->execution.selectedRecordingIds, testing::ElementsAre(selectedRecording.recordingId.getRawValue()));
    EXPECT_THAT(
        result->execution.pinnedSegments,
        testing::ElementsAre(
            ReplayPinnedSegment{.recordingId = selectedRecording.recordingId.getRawValue(), .segmentId = 11},
            ReplayPinnedSegment{.recordingId = selectedRecording.recordingId.getRawValue(), .segmentId = 12},
            ReplayPinnedSegment{.recordingId = selectedRecording.recordingId.getRawValue(), .segmentId = 13}));
    ASSERT_THAT(result->execution.internalQueryIds, testing::SizeIs(1));
    EXPECT_FALSE(result->execution.failureReason.has_value());
    EXPECT_TRUE(queryManager->getReplayExecutions().contains(result->execution.id));
    EXPECT_EQ(queryManager->getReplayExecutions().at(result->execution.id), result->execution);
    ASSERT_TRUE(queryManager->getReplayPlans().contains(result->execution.id));
    EXPECT_THAT(queryManager->getReplayPlans().at(result->execution.id).selectedRecordingIds, testing::ElementsAre(selectedRecording.recordingId));
    ASSERT_THAT(queryManager->getReplayPlans().at(result->execution.id).selectedCheckpoints, testing::SizeIs(1));
    EXPECT_EQ(
        queryManager->getReplayPlans().at(result->execution.id).selectedCheckpoints.front().bundleName,
        "plan_replayable-query_replay_checkpoint_00000000000000000500");

    const auto internalQueryId = result->execution.internalQueryIds.front();
    EXPECT_EQ(backendState->registrations[internalQueryId], 1U);
    EXPECT_EQ(backendState->starts[internalQueryId], 1U);
    EXPECT_EQ(backendState->unregisters[internalQueryId], 1U);
    ASSERT_TRUE(backendState->registeredReplayCheckpoints.contains(internalQueryId));
    ASSERT_THAT(backendState->registeredReplayCheckpoints.at(internalQueryId), testing::SizeIs(1));
    ASSERT_TRUE(backendState->registeredReplayCheckpoints.at(internalQueryId).front().has_value());
    EXPECT_EQ(
        backendState->registeredReplayCheckpoints.at(internalQueryId).front()->restoreCheckpoint->bundleName,
        "plan_replayable-query_replay_checkpoint_00000000000000000500");
    EXPECT_EQ(
        backendState->registeredReplayCheckpoints.at(internalQueryId).front()->restoreCheckpoint->checkpointWatermarkMs,
        500U);
    ASSERT_TRUE(backendState->registeredReplayCheckpoints.at(internalQueryId).front()->replayRestoreFingerprint.has_value());
    ASSERT_TRUE(backendState->registeredReplayRuntimeControls.contains(internalQueryId));
    ASSERT_THAT(backendState->registeredReplayRuntimeControls.at(internalQueryId), testing::SizeIs(1));
    ASSERT_TRUE(backendState->registeredReplayRuntimeControls.at(internalQueryId).front().has_value());
    EXPECT_EQ(backendState->registeredReplayRuntimeControls.at(internalQueryId).front()->emitStartWatermarkMs, 1'000U);
    ASSERT_THAT(backendState->replaySelections, testing::SizeIs(1));
    EXPECT_EQ(backendState->replaySelections.front().recordingFilePath, recordingFilePath.string());
    EXPECT_EQ(backendState->replaySelections.front().selection.scanStartMs, std::optional<uint64_t>(500));
    EXPECT_EQ(backendState->replaySelections.front().selection.scanEndMs, std::optional<uint64_t>(5'000));
    ASSERT_THAT(backendState->replayPins, testing::SizeIs(1));
    EXPECT_EQ(backendState->replayPins.front().recordingFilePath, recordingFilePath.string());
    EXPECT_THAT(backendState->replayPins.front().segmentIds, testing::ElementsAre(11, 12, 13));
    ASSERT_THAT(backendState->replayUnpins, testing::SizeIs(1));
    EXPECT_EQ(backendState->replayUnpins.front().recordingFilePath, recordingFilePath.string());
    EXPECT_THAT(backendState->replayUnpins.front().segmentIds, testing::ElementsAre(11, 12, 13));

    const auto manifest = Replay::readBinaryStoreManifest(recordingFilePath.string());
    EXPECT_THAT(manifest.segments | std::views::transform(&Replay::BinaryStoreManifestEntry::pinCount) | std::ranges::to<std::vector>(), testing::Each(0U));

    std::error_code errorCode;
    std::filesystem::remove_all(recordingFilePath.parent_path(), errorCode);
}

TEST(QueryManagerMetricsTest, ReplayStatementHandlerWaitsForWorkerReplayPhaseBeforeMarkingEmitting)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("replay-phase-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000};
    const auto recordingFilePath = createReplayRecordingFixturePath("replay-phase-5-emit-transition");
    initializeReplayRecordingFixture(
        recordingFilePath,
        {
            Replay::BinaryStoreManifestEntry{.segmentId = 11, .payloadOffset = 128, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 0, .maxWatermark = 1'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 12, .payloadOffset = 192, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 2'000, .maxWatermark = 3'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 13, .payloadOffset = 256, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 4'000, .maxWatermark = 5'999},
        });

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    const auto originalPlan = createPlacedWindowReplayPlan(
        queryId,
        worker,
        Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction("id")),
        "replay_sink");
    const auto sink = originalPlan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto window = sink.getChildren().front();
    ASSERT_TRUE(window.tryGetAs<WindowedAggregationLogicalOperator>().has_value());
    ASSERT_EQ(window.getChildren().size(), 1U);
    const auto assigner = window.getChildren().front();
    ASSERT_TRUE(assigner.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value());
    const auto selectedRecording = RecordingSelection{
        .recordingId = RecordingId("replay-phase-query-recording"),
        .node = worker,
        .filePath = recordingFilePath.string(),
        .structuralFingerprint = "replay-phase-window-boundary",
        .representation = RecordingRepresentation::BinaryStore,
        .retentionWindowMs = replaySpecification.retentionWindowMs,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    const auto replayBoundary = QueryRecordingPlanInsertion{
        .selection = selectedRecording,
        .materializationEdges = {RecordingRewriteEdge{.parentId = window.getId(), .childId = assigner.getId()}}};
    const auto replayCheckpointBoundaryRewrite = QueryRecordingPlanRewrite{
        .queryId = queryId.getRawValue(),
        .basePlan = originalPlan,
        .insertions = {replayBoundary}};
    const auto replayCheckpointBoundaries = buildReplayCheckpointBoundaries(replayCheckpointBoundaryRewrite);
    ASSERT_TRUE(replayCheckpointBoundaries.has_value()) << replayCheckpointBoundaries.error().what();
    const auto replayCheckpointFingerprints = computeReplayCheckpointFingerprints(optimizer->decomposePlacedPlan(originalPlan), *replayCheckpointBoundaries);
    ASSERT_TRUE(replayCheckpointFingerprints.has_value()) << replayCheckpointFingerprints.error().what();
    ASSERT_TRUE(replayCheckpointFingerprints->contains(worker));
    ASSERT_THAT(replayCheckpointFingerprints->at(worker), testing::SizeIs(1));
    ASSERT_TRUE(replayCheckpointFingerprints->at(worker).front().has_value());
    const auto replayRestoreFingerprint = *replayCheckpointFingerprints->at(worker).front();

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = originalPlan,
            .globalPlan = originalPlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {selectedRecording.recordingId},
            .networkExplanations = {},
            .queryPlanRewrite = QueryRecordingPlanRewrite{
                .queryId = queryId.getRawValue(),
                .basePlan = originalPlan,
                .insertions = {}},
            .replayBoundaries = {replayBoundary},
            .replayCheckpointRequirements = {ReplayCheckpointRequirement{
                .host = worker.getRawValue(),
                .replayRestoreFingerprint = replayRestoreFingerprint}}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = selectedRecording.recordingId,
            .node = selectedRecording.node,
            .filePath = recordingFilePath.string(),
            .structuralFingerprint = selectedRecording.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = selectedRecording.representation,
            .ownerQueries = {queryId},
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retainedStartWatermark = Timestamp(0),
            .retainedEndWatermark = Timestamp(70'000),
            .fillWatermark = Timestamp(70'000),
            .segmentCount = 3,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt});

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->stopRunningQueriesAfterWorkerStatusCallCount = 2;
    backendState->fallbackStopRunningQueriesAfterStatusCallCount = 20;
    backendState->workerStatusProvider = [recordingId = selectedRecording.recordingId, recordingFilePath](
                                             const TrackingLifecycleBackendState& backendState) -> std::expected<WorkerStatus, Exception>
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        status.replayMetrics.recordingStatuses.push_back(
            Replay::RecordingRuntimeStatus{
                .recordingId = recordingId.getRawValue(),
                .filePath = recordingFilePath.string(),
                .lifecycleState = Replay::RecordingLifecycleState::Ready,
                .retentionWindowMs = 60'000,
                .retainedStartWatermark = Timestamp(0),
                .retainedEndWatermark = Timestamp(70'000),
                .fillWatermark = Timestamp(70'000),
                .segmentCount = 3,
                .storageBytes = 4096,
                .successorRecordingId = std::nullopt});

        for (const auto& [localQueryId, localStatus] : backendState.statuses)
        {
            if (localStatus.state != QueryState::Running)
            {
                continue;
            }
            status.replayMetrics.replayQueryStatuses.push_back(
                ReplayQueryStatus{
                    .queryId = localQueryId,
                    .phase = backendState.workerStatusCallCount >= 2 ? ReplayQueryPhase::Emitting
                                                                     : ReplayQueryPhase::FastForwarding,
                    .lastOutputWatermarkMs = backendState.workerStatusCallCount >= 2 ? std::optional<uint64_t>(1'000)
                                                                                     : std::optional<uint64_t>(500)});
        }
        return status;
    };

    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 1'000, .intervalEndMs = 5'000});
    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_EQ(result->execution.state, ReplayExecutionState::Done);
    ASSERT_THAT(result->execution.internalQueryIds, testing::SizeIs(1));

    const auto internalQueryId = result->execution.internalQueryIds.front();
    ASSERT_TRUE(backendState->registeredReplayRuntimeControls.contains(internalQueryId));
    ASSERT_THAT(backendState->registeredReplayRuntimeControls.at(internalQueryId), testing::SizeIs(1));
    ASSERT_TRUE(backendState->registeredReplayRuntimeControls.at(internalQueryId).front().has_value());
    EXPECT_EQ(backendState->registeredReplayRuntimeControls.at(internalQueryId).front()->emitStartWatermarkMs, 1'000U);
    EXPECT_GE(backendState->workerStatusCallCount, 2U);

    std::error_code errorCode;
    std::filesystem::remove_all(recordingFilePath.parent_path(), errorCode);
}

TEST(QueryManagerMetricsTest, RegisterQueryRoutesReplayCheckpointRestoreAcrossMultipleLocalPlansOnSameHost)
{
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("replay-guard-query");

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); });

    const auto localPlanA = createReplayPlan(queryId, "REPLAY_SOURCE_A", "replay_sink_a");
    const auto localPlanB = createReplayPlan(queryId, "REPLAY_SOURCE_B", "replay_sink_b");
    const auto distributedPlan = DistributedLogicalPlan(
        DecomposedLogicalPlan<Host>{{{worker, {localPlanA, localPlanB}}}},
        localPlanA);

    const auto result = queryManager->registerQuery(
        distributedPlan,
        std::nullopt,
        QueryRegistrationOptions{
            .internal = true,
            .includeInReplayPlanning = false,
            .replayCheckpoints =
                {ReplayCheckpointReference{
                    .host = worker.getRawValue(),
                    .bundleName = "plan_replay-guard-query_a_replay_checkpoint_00000000000000000900",
                    .planFingerprint = "fingerprint-guard-a",
                    .replayRestoreFingerprint = "restore-a",
                    .checkpointWatermarkMs = 900},
                 ReplayCheckpointReference{
                     .host = worker.getRawValue(),
                     .bundleName = "plan_replay-guard-query_b_replay_checkpoint_00000000000000000900",
                     .planFingerprint = "fingerprint-guard-b",
                     .replayRestoreFingerprint = "restore-b",
                     .checkpointWatermarkMs = 900}},
            .replayRestoreFingerprintsByHost = {{worker, {std::optional<std::string>{"restore-a"}, std::optional<std::string>{"restore-b"}}}}});

    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_TRUE(queryManager->queries().empty());
    EXPECT_TRUE(queryManager->getQuery(*result).has_value());
    ASSERT_TRUE(backendState->registeredReplayCheckpoints.contains(*result));
    ASSERT_THAT(backendState->registeredReplayCheckpoints.at(*result), testing::SizeIs(2));
    ASSERT_TRUE(backendState->registeredReplayCheckpoints.at(*result)[0].has_value());
    ASSERT_TRUE(backendState->registeredReplayCheckpoints.at(*result)[1].has_value());
    EXPECT_EQ(
        backendState->registeredReplayCheckpoints.at(*result)[0]->restoreCheckpoint->bundleName,
        "plan_replay-guard-query_a_replay_checkpoint_00000000000000000900");
    EXPECT_EQ(
        backendState->registeredReplayCheckpoints.at(*result)[1]->restoreCheckpoint->bundleName,
        "plan_replay-guard-query_b_replay_checkpoint_00000000000000000900");
}

TEST(QueryManagerMetricsTest, ReplayStatementHandlerFailsWhenWorkerRejectsReplayCheckpointRegistration)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("replayable-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000};
    const auto recordingFilePath = createReplayRecordingFixturePath("replay-phase-7-registration-failure");
    initializeReplayRecordingFixture(
        recordingFilePath,
        {
            Replay::BinaryStoreManifestEntry{.segmentId = 11, .payloadOffset = 128, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 0, .maxWatermark = 1'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 12, .payloadOffset = 192, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 2'000, .maxWatermark = 3'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 13, .payloadOffset = 256, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 4'000, .maxWatermark = 5'999},
        });

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    const auto originalPlan = createPlacedWindowReplayPlan(
        queryId,
        worker,
        Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction("id")),
        "replay_sink");
    const auto sink = originalPlan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto window = sink.getChildren().front();
    ASSERT_TRUE(window.tryGetAs<WindowedAggregationLogicalOperator>().has_value());
    ASSERT_EQ(window.getChildren().size(), 1U);
    const auto assigner = window.getChildren().front();
    ASSERT_TRUE(assigner.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value());
    const auto selectedRecording = RecordingSelection{
        .recordingId = RecordingId("replayable-query-registration-failure-recording"),
        .node = worker,
        .filePath = recordingFilePath.string(),
        .structuralFingerprint = "replayable-window-registration-failure-boundary",
        .representation = RecordingRepresentation::BinaryStore,
        .retentionWindowMs = replaySpecification.retentionWindowMs,
        .beneficiaryQueries = {},
        .coversIncomingQuery = true};
    const auto replayBoundary = QueryRecordingPlanInsertion{
        .selection = selectedRecording,
        .materializationEdges = {RecordingRewriteEdge{.parentId = window.getId(), .childId = assigner.getId()}}};
    const auto replayCheckpointBoundaryRewrite = QueryRecordingPlanRewrite{
        .queryId = queryId.getRawValue(),
        .basePlan = originalPlan,
        .insertions = {replayBoundary}};
    const auto replayCheckpointBoundaries = buildReplayCheckpointBoundaries(replayCheckpointBoundaryRewrite);
    ASSERT_TRUE(replayCheckpointBoundaries.has_value()) << replayCheckpointBoundaries.error().what();
    const auto replayCheckpointFingerprints = computeReplayCheckpointFingerprints(optimizer->decomposePlacedPlan(originalPlan), *replayCheckpointBoundaries);
    ASSERT_TRUE(replayCheckpointFingerprints.has_value()) << replayCheckpointFingerprints.error().what();
    ASSERT_TRUE(replayCheckpointFingerprints->contains(worker));
    ASSERT_THAT(replayCheckpointFingerprints->at(worker), testing::SizeIs(1));
    ASSERT_TRUE(replayCheckpointFingerprints->at(worker).front().has_value());
    const auto replayRestoreFingerprint = *replayCheckpointFingerprints->at(worker).front();

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = originalPlan,
            .globalPlan = originalPlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {selectedRecording.recordingId},
            .networkExplanations = {},
            .queryPlanRewrite = QueryRecordingPlanRewrite{
                .queryId = queryId.getRawValue(),
                .basePlan = originalPlan,
                .insertions = {}},
            .replayBoundaries = {replayBoundary},
            .replayCheckpointRequirements = {ReplayCheckpointRequirement{
                .host = worker.getRawValue(),
                .replayRestoreFingerprint = replayRestoreFingerprint}}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = selectedRecording.recordingId,
            .node = selectedRecording.node,
            .filePath = recordingFilePath.string(),
            .structuralFingerprint = selectedRecording.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = selectedRecording.representation,
            .ownerQueries = {queryId},
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retainedStartWatermark = Timestamp(0),
            .retainedEndWatermark = Timestamp(70'000),
            .fillWatermark = Timestamp(70'000),
            .segmentCount = 3,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt});

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->replayCheckpointRegistrationFailureMessage = "incompatible replay checkpoint bundle";
    backendState->workerStatusResult = [](
                                         const QueryId& queryId,
                                         const RecordingId& recordingId,
                                         const std::filesystem::path& recordingFilePath,
                                         const std::string& replayRestoreFingerprint) -> std::expected<WorkerStatus, Exception>
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        status.replayMetrics.recordingStatuses.push_back(
            Replay::RecordingRuntimeStatus{
                .recordingId = recordingId.getRawValue(),
                .filePath = recordingFilePath.string(),
                .lifecycleState = Replay::RecordingLifecycleState::Ready,
                .retentionWindowMs = 60'000,
                .retainedStartWatermark = Timestamp(0),
                .retainedEndWatermark = Timestamp(70'000),
                .fillWatermark = Timestamp(70'000),
                .segmentCount = 3,
                .storageBytes = 4096,
                .successorRecordingId = std::nullopt});
        status.replayMetrics.replayCheckpoints.push_back(
            ReplayCheckpointStatus{
                .queryId = queryId,
                .bundleName = "plan_replayable-query_replay_checkpoint_00000000000000000500",
                .planFingerprint = "replay-checkpoint-fingerprint",
                .replayRestoreFingerprint = replayRestoreFingerprint,
                .checkpointWatermarkMs = 500});
        return status;
    }(localQueryId, selectedRecording.recordingId, recordingFilePath, replayRestoreFingerprint);
    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 1'000, .intervalEndMs = 5'000});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), QueryRegistrationFailed("unused").code());
    EXPECT_THAT(std::string(result.error().what()), testing::HasSubstr("incompatible replay checkpoint bundle"));

    ASSERT_THAT(queryManager->getReplayExecutions(), testing::SizeIs(1));
    const auto& replayExecution = queryManager->getReplayExecutions().begin()->second;
    EXPECT_EQ(replayExecution.state, ReplayExecutionState::Failed);
    EXPECT_TRUE(replayExecution.internalQueryIds.empty());
    EXPECT_THAT(
        replayExecution.pinnedSegments,
        testing::ElementsAre(
            ReplayPinnedSegment{.recordingId = selectedRecording.recordingId.getRawValue(), .segmentId = 11},
            ReplayPinnedSegment{.recordingId = selectedRecording.recordingId.getRawValue(), .segmentId = 12},
            ReplayPinnedSegment{.recordingId = selectedRecording.recordingId.getRawValue(), .segmentId = 13}));
    ASSERT_TRUE(replayExecution.failureReason.has_value());
    EXPECT_THAT(*replayExecution.failureReason, testing::HasSubstr("incompatible replay checkpoint bundle"));

    EXPECT_TRUE(backendState->registeredReplayCheckpoints.empty());
    EXPECT_TRUE(backendState->starts.empty());
    EXPECT_TRUE(backendState->unregisters.empty());
    ASSERT_THAT(backendState->replaySelections, testing::SizeIs(1));
    EXPECT_EQ(backendState->replaySelections.front().recordingFilePath, recordingFilePath.string());
    EXPECT_EQ(backendState->replaySelections.front().selection.scanStartMs, std::optional<uint64_t>(500));
    EXPECT_EQ(backendState->replaySelections.front().selection.scanEndMs, std::optional<uint64_t>(5'000));
    ASSERT_THAT(backendState->replayPins, testing::SizeIs(1));
    EXPECT_EQ(backendState->replayPins.front().recordingFilePath, recordingFilePath.string());
    EXPECT_THAT(backendState->replayPins.front().segmentIds, testing::ElementsAre(11, 12, 13));
    ASSERT_THAT(backendState->replayUnpins, testing::SizeIs(1));
    EXPECT_EQ(backendState->replayUnpins.front().recordingFilePath, recordingFilePath.string());
    EXPECT_THAT(backendState->replayUnpins.front().segmentIds, testing::ElementsAre(11, 12, 13));

    const auto manifest = Replay::readBinaryStoreManifest(recordingFilePath.string());
    EXPECT_THAT(manifest.segments | std::views::transform(&Replay::BinaryStoreManifestEntry::pinCount) | std::ranges::to<std::vector>(), testing::Each(0U));

    std::error_code errorCode;
    std::filesystem::remove_all(recordingFilePath.parent_path(), errorCode);
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

TEST(QueryManagerMetricsTest, ReplayStatementHandlerRejectsReplayWhenNoReadyRecordingCoversInterval)
{
    const ScopedTimeTravelReadAliasReset aliasReset;
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("replayable-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000};

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    addSourceAndSinkCatalogEntries(sourceCatalog, sinkCatalog, worker, "REPLAY_SOURCE", "replay_sink");

    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    const auto originalPlan = createReplayPlan(queryId, "REPLAY_SOURCE", "replay_sink");
    const auto optimizedPlan = optimizer->optimize(originalPlan, replaySpecification, RecordingCatalog{});
    ASSERT_EQ(optimizedPlan.getRecordingSelectionResult().selectedRecordings.size(), 1U);
    ASSERT_TRUE(optimizedPlan.getRecordingSelectionResult().incomingQueryPlanRewrite.has_value());
    ASSERT_EQ(optimizedPlan.getRecordingSelectionResult().incomingQueryReplayBoundaries.size(), 1U);
    const auto selectedRecording = optimizedPlan.getRecordingSelectionResult().selectedRecordings.front();

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = originalPlan,
            .globalPlan = optimizedPlan.getGlobalPlan(),
            .replaySpecification = replaySpecification,
            .selectedRecordings = {selectedRecording.recordingId},
            .networkExplanations = {},
            .queryPlanRewrite = QueryRecordingPlanRewrite{
                .queryId = queryId.getRawValue(),
                .basePlan = optimizedPlan.getRecordingSelectionResult().incomingQueryPlanRewrite->basePlan,
                .insertions = {}},
            .replayBoundaries = optimizedPlan.getRecordingSelectionResult().incomingQueryReplayBoundaries});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = selectedRecording.recordingId,
            .node = selectedRecording.node,
            .filePath = selectedRecording.filePath,
            .structuralFingerprint = selectedRecording.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = selectedRecording.representation,
            .ownerQueries = {queryId},
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retainedStartWatermark = Timestamp(2'000),
            .retainedEndWatermark = Timestamp(70'000),
            .fillWatermark = Timestamp(70'000),
            .segmentCount = 4,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt});

    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [](const WorkerConfig&) { return std::make_unique<FakeLifecycleBackend>(); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 1'000, .intervalEndMs = 5'000});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::PlacementFailure);
}

TEST(QueryManagerMetricsTest, ReplayStatementHandlerRejectsReplayExecutionWhenSuffixContainsUnsupportedStoreOperator)
{
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("unsupported-replay-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000};
    const auto recordingId = RecordingId("unsupported-replay-recording");
    const auto recordingFilePath = createReplayRecordingFixturePath("replay-unsupported-phase-5");
    initializeReplayRecordingFixture(
        recordingFilePath,
        {
            Replay::BinaryStoreManifestEntry{.segmentId = 21, .payloadOffset = 128, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 0, .maxWatermark = 5'999},
        });

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    addSourceAndSinkCatalogEntries(sourceCatalog, sinkCatalog, worker, "UNSUPPORTED_SOURCE", "unsupported_sink");

    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    const auto logicalSource = sourceCatalog->getLogicalSource("UNSUPPORTED_SOURCE");
    ASSERT_TRUE(logicalSource.has_value());
    const auto physicalSources = sourceCatalog->getPhysicalSources(*logicalSource);
    ASSERT_TRUE(physicalSources.has_value());
    ASSERT_THAT(*physicalSources, testing::SizeIs(1));
    const auto sourceOperator = LogicalOperator(SourceDescriptorLogicalOperator(*physicalSources->begin()).withTraitSet(TraitSet{PlacementTrait(worker)}));
    const auto storeConfig = StoreLogicalOperator::validateAndFormatConfig(
        {{"file_path", "/tmp/unsupported-replay-store.bin"}, {"append", "false"}, {"header", "true"}});
    const auto storeOperator = LogicalOperator(
        StoreLogicalOperator(storeConfig)
            .withTraitSet(TraitSet{PlacementTrait(worker)})
            .withInferredSchema({sourceOperator.getOutputSchema()})
            .withChildren({sourceOperator}));
    const auto basePlan = LogicalPlan(QueryId::createDistributed(queryId), {storeOperator});

    const QueryRecordingPlanInsertion replayBoundary{
        .selection =
            RecordingSelection{
                .recordingId = recordingId,
                .node = worker,
                .filePath = recordingFilePath.string(),
                .structuralFingerprint = "unsupported-store-boundary",
                .representation = RecordingRepresentation::BinaryStore,
                .retentionWindowMs = replaySpecification.retentionWindowMs,
                .beneficiaryQueries = {},
                .coversIncomingQuery = true},
        .materializationEdges =
            {RecordingRewriteEdge{.parentId = storeOperator.getId(), .childId = sourceOperator.getId()}}};

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = std::nullopt,
            .globalPlan = basePlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {recordingId},
            .networkExplanations = {},
            .queryPlanRewrite =
                QueryRecordingPlanRewrite{
                    .queryId = queryId.getRawValue(),
                    .basePlan = basePlan,
                    .insertions = {}},
            .replayBoundaries = {replayBoundary}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = worker,
            .filePath = recordingFilePath.string(),
            .structuralFingerprint = replayBoundary.selection.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {queryId},
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retainedStartWatermark = Timestamp(0),
            .retainedEndWatermark = Timestamp(70'000),
            .fillWatermark = Timestamp(70'000),
            .segmentCount = 1,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt});

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->workerStatusResult = [recordingId, recordingFilePath]() -> std::expected<WorkerStatus, Exception>
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        status.replayMetrics.recordingStatuses.push_back(
            Replay::RecordingRuntimeStatus{
                .recordingId = recordingId.getRawValue(),
                .filePath = recordingFilePath.string(),
                .lifecycleState = Replay::RecordingLifecycleState::Ready,
                .retentionWindowMs = 60'000,
                .retainedStartWatermark = Timestamp(0),
                .retainedEndWatermark = Timestamp(70'000),
                .fillWatermark = Timestamp(70'000),
                .segmentCount = 1,
                .storageBytes = 4096,
                .successorRecordingId = std::nullopt});
        return status;
    }();
    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 1'000, .intervalEndMs = 5'000});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::UnsupportedQuery);

    ASSERT_THAT(queryManager->getReplayExecutions(), testing::SizeIs(1));
    const auto& replayExecution = queryManager->getReplayExecutions().begin()->second;
    EXPECT_EQ(replayExecution.state, ReplayExecutionState::Failed);
    EXPECT_TRUE(replayExecution.internalQueryIds.empty());
    EXPECT_TRUE(replayExecution.pinnedSegments.empty());
    ASSERT_TRUE(replayExecution.failureReason.has_value());
    EXPECT_THAT(*replayExecution.failureReason, testing::HasSubstr("Store operator"));

    std::error_code errorCode;
    std::filesystem::remove_all(recordingFilePath.parent_path(), errorCode);
}

TEST(QueryManagerMetricsTest, ReplayStatementHandlerInjectsEventTimeGateForWindowReplaySuffix)
{
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("event-time-window-replay-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000};
    const auto recordingId = RecordingId("event-time-window-recording");
    const auto recordingFilePath = createReplayRecordingFixturePath("replay-phase-6-event-window");
    initializeReplayRecordingFixture(
        recordingFilePath,
        {
            Replay::BinaryStoreManifestEntry{.segmentId = 41, .payloadOffset = 128, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 0, .maxWatermark = 1'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 42, .payloadOffset = 192, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 2'000, .maxWatermark = 3'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 43, .payloadOffset = 256, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 4'000, .maxWatermark = 5'999},
        });

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);

    const auto basePlan = createPlacedWindowReplayPlan(
        queryId,
        worker,
        Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction("id")),
        "event_window_sink");
    const auto sink = basePlan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto window = sink.getChildren().front();
    ASSERT_TRUE(window.tryGetAs<WindowedAggregationLogicalOperator>().has_value());
    ASSERT_EQ(window.getChildren().size(), 1U);
    const auto assigner = window.getChildren().front();
    ASSERT_TRUE(assigner.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value());

    const QueryRecordingPlanInsertion replayBoundary{
        .selection =
            RecordingSelection{
                .recordingId = recordingId,
                .node = worker,
                .filePath = recordingFilePath.string(),
                .structuralFingerprint = "event-time-window-boundary",
                .representation = RecordingRepresentation::BinaryStore,
                .retentionWindowMs = replaySpecification.retentionWindowMs,
                .beneficiaryQueries = {},
                .coversIncomingQuery = true},
        .materializationEdges =
            {RecordingRewriteEdge{.parentId = window.getId(), .childId = assigner.getId()}}};

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = basePlan,
            .globalPlan = basePlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {recordingId},
            .networkExplanations = {},
            .queryPlanRewrite =
                QueryRecordingPlanRewrite{
                    .queryId = queryId.getRawValue(),
                    .basePlan = basePlan,
                    .insertions = {}},
            .replayBoundaries = {replayBoundary}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = worker,
            .filePath = recordingFilePath.string(),
            .structuralFingerprint = replayBoundary.selection.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {queryId},
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retainedStartWatermark = Timestamp(0),
            .retainedEndWatermark = Timestamp(70'000),
            .fillWatermark = Timestamp(70'000),
            .segmentCount = 3,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt});

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->autoStopRunningQueriesOnStatus = true;
    backendState->workerStatusResult = [recordingId, recordingFilePath]() -> std::expected<WorkerStatus, Exception>
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        status.replayMetrics.recordingStatuses.push_back(
            Replay::RecordingRuntimeStatus{
                .recordingId = recordingId.getRawValue(),
                .filePath = recordingFilePath.string(),
                .lifecycleState = Replay::RecordingLifecycleState::Ready,
                .retentionWindowMs = 60'000,
                .retainedStartWatermark = Timestamp(0),
                .retainedEndWatermark = Timestamp(70'000),
                .fillWatermark = Timestamp(70'000),
                .segmentCount = 3,
                .storageBytes = 4096,
                .successorRecordingId = std::nullopt});
        return status;
    }();
    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 3'000, .intervalEndMs = 5'000});
    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_THAT(result->execution.internalQueryIds, testing::SizeIs(1));

    const auto internalQueryId = result->execution.internalQueryIds.front();
    ASSERT_TRUE(backendState->registeredPlans.contains(internalQueryId));
    const auto& registeredReplayPlan = backendState->registeredPlans.at(internalQueryId);
    EXPECT_EQ(getOperatorByType<WindowedAggregationLogicalOperator>(registeredReplayPlan).size(), 1U);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(registeredReplayPlan).size(), 1U);
    ASSERT_EQ(getOperatorByType<SelectionLogicalOperator>(registeredReplayPlan).size(), 1U);

    const auto selections = getOperatorByType<SelectionLogicalOperator>(registeredReplayPlan);
    const auto selectionPredicate = selections.front()->getPredicate().explain(ExplainVerbosity::Debug);
    EXPECT_THAT(selectionPredicate, testing::HasSubstr("END"));
    EXPECT_THAT(selectionPredicate, testing::HasSubstr("3000"));
    EXPECT_THAT(selectionPredicate, testing::HasSubstr("5000"));

    const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(registeredReplayPlan);
    ASSERT_EQ(sources.size(), 1U);
    const auto sourceConfig = sources.front()->getSourceDescriptor().getConfig();
    EXPECT_EQ(std::get<uint64_t>(sourceConfig.at("scan_start_ms")), 0U);
    EXPECT_EQ(std::get<uint64_t>(sourceConfig.at("scan_end_ms")), 5'000U);
    EXPECT_EQ(std::get<std::string>(sourceConfig.at("segment_ids")), "41,42,43");
    EXPECT_EQ(std::get<std::string>(sourceConfig.at("replay_mode")), "interval");

    std::error_code errorCode;
    std::filesystem::remove_all(recordingFilePath.parent_path(), errorCode);
}

TEST(QueryManagerMetricsTest, ReplayStatementHandlerInjectsExactEventTimeTupleGateForStatelessReplaySuffix)
{
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("event-time-stateless-replay-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000};
    const auto recordingId = RecordingId("event-time-stateless-recording");
    const auto recordingFilePath = createReplayRecordingFixturePath("replay-phase-6-event-stateless");
    initializeReplayRecordingFixture(
        recordingFilePath,
        {
            Replay::BinaryStoreManifestEntry{.segmentId = 51, .payloadOffset = 128, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 0, .maxWatermark = 1'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 52, .payloadOffset = 192, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 2'000, .maxWatermark = 3'999},
            Replay::BinaryStoreManifestEntry{.segmentId = 53, .payloadOffset = 256, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 4'000, .maxWatermark = 5'999},
        });

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);

    const auto basePlan = createPlacedEventTimeReplayPlan(queryId, worker, "event_time_sink");
    const auto sink = basePlan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto assigner = sink.getChildren().front();
    ASSERT_TRUE(assigner.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value());

    const QueryRecordingPlanInsertion replayBoundary{
        .selection =
            RecordingSelection{
                .recordingId = recordingId,
                .node = worker,
                .filePath = recordingFilePath.string(),
                .structuralFingerprint = "event-time-stateless-boundary",
                .representation = RecordingRepresentation::BinaryStore,
                .retentionWindowMs = replaySpecification.retentionWindowMs,
                .beneficiaryQueries = {},
                .coversIncomingQuery = true},
        .materializationEdges =
            {RecordingRewriteEdge{.parentId = sink.getId(), .childId = assigner.getId()}}};

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = basePlan,
            .globalPlan = basePlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {recordingId},
            .networkExplanations = {},
            .queryPlanRewrite =
                QueryRecordingPlanRewrite{
                    .queryId = queryId.getRawValue(),
                    .basePlan = basePlan,
                    .insertions = {}},
            .replayBoundaries = {replayBoundary}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = worker,
            .filePath = recordingFilePath.string(),
            .structuralFingerprint = replayBoundary.selection.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {queryId},
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retainedStartWatermark = Timestamp(0),
            .retainedEndWatermark = Timestamp(70'000),
            .fillWatermark = Timestamp(70'000),
            .segmentCount = 3,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt});

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->autoStopRunningQueriesOnStatus = true;
    backendState->workerStatusResult = [recordingId, recordingFilePath]() -> std::expected<WorkerStatus, Exception>
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        status.replayMetrics.recordingStatuses.push_back(
            Replay::RecordingRuntimeStatus{
                .recordingId = recordingId.getRawValue(),
                .filePath = recordingFilePath.string(),
                .lifecycleState = Replay::RecordingLifecycleState::Ready,
                .retentionWindowMs = 60'000,
                .retainedStartWatermark = Timestamp(0),
                .retainedEndWatermark = Timestamp(70'000),
                .fillWatermark = Timestamp(70'000),
                .segmentCount = 3,
                .storageBytes = 4096,
                .successorRecordingId = std::nullopt});
        return status;
    }();
    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 3'000, .intervalEndMs = 5'000});
    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_THAT(result->execution.internalQueryIds, testing::SizeIs(1));

    const auto internalQueryId = result->execution.internalQueryIds.front();
    ASSERT_TRUE(backendState->registeredPlans.contains(internalQueryId));
    const auto& registeredReplayPlan = backendState->registeredPlans.at(internalQueryId);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(registeredReplayPlan).size(), 1U);
    ASSERT_EQ(getOperatorByType<SelectionLogicalOperator>(registeredReplayPlan).size(), 1U);

    const auto selections = getOperatorByType<SelectionLogicalOperator>(registeredReplayPlan);
    const auto selectionPredicate = selections.front()->getPredicate().explain(ExplainVerbosity::Debug);
    EXPECT_THAT(selectionPredicate, testing::HasSubstr("3000"));
    EXPECT_THAT(selectionPredicate, testing::HasSubstr("5000"));

    const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(registeredReplayPlan);
    ASSERT_EQ(sources.size(), 1U);
    const auto sourceConfig = sources.front()->getSourceDescriptor().getConfig();
    EXPECT_EQ(std::get<uint64_t>(sourceConfig.at("scan_start_ms")), 3'000U);
    EXPECT_EQ(std::get<uint64_t>(sourceConfig.at("scan_end_ms")), 5'000U);
    EXPECT_EQ(std::get<uint64_t>(sourceConfig.at("emit_start_ms")), 3'000U);
    EXPECT_EQ(std::get<uint64_t>(sourceConfig.at("emit_end_ms")), 5'000U);
    EXPECT_EQ(std::get<std::string>(sourceConfig.at("event_time_field")), "id");
    EXPECT_EQ(std::get<uint64_t>(sourceConfig.at("event_time_unit_ms_multiplier")), 1U);

    std::error_code errorCode;
    std::filesystem::remove_all(recordingFilePath.parent_path(), errorCode);
}

TEST(QueryManagerMetricsTest, ReplayStatementHandlerRejectsWindowReplayWithoutPayloadDerivedEventTime)
{
    const auto worker = Host("worker-1:8080");
    const auto queryId = DistributedQueryId("ingestion-time-window-replay-query");
    const auto localQueryId = QueryId::create(
        LocalQueryId(std::string("00000000-0000-0000-0000-000000000001")), queryId);
    const auto replaySpecification = ReplaySpecification{.retentionWindowMs = 60'000, .replayLatencyLimitMs = 5'000};
    const auto recordingId = RecordingId("ingestion-time-window-recording");
    const auto recordingFilePath = createReplayRecordingFixturePath("replay-phase-6-ingestion-window");
    initializeReplayRecordingFixture(
        recordingFilePath,
        {
            Replay::BinaryStoreManifestEntry{.segmentId = 51, .payloadOffset = 128, .storedSizeBytes = 64, .logicalSizeBytes = 64, .minWatermark = 0, .maxWatermark = 5'999},
        });

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(worker, "worker-1-data", INFINITE_CAPACITY, {}));

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);

    const auto basePlan = createPlacedWindowReplayPlan(
        queryId,
        worker,
        Windowing::TimeCharacteristic::createIngestionTime(),
        "ingestion_window_sink");
    const auto sink = basePlan.getRootOperators().front();
    ASSERT_EQ(sink.getChildren().size(), 1U);
    const auto window = sink.getChildren().front();
    ASSERT_TRUE(window.tryGetAs<WindowedAggregationLogicalOperator>().has_value());
    ASSERT_EQ(window.getChildren().size(), 1U);
    const auto assigner = window.getChildren().front();
    ASSERT_TRUE(assigner.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>().has_value());

    const QueryRecordingPlanInsertion replayBoundary{
        .selection =
            RecordingSelection{
                .recordingId = recordingId,
                .node = worker,
                .filePath = recordingFilePath.string(),
                .structuralFingerprint = "ingestion-time-window-boundary",
                .representation = RecordingRepresentation::BinaryStore,
                .retentionWindowMs = replaySpecification.retentionWindowMs,
                .beneficiaryQueries = {},
                .coversIncomingQuery = true},
        .materializationEdges =
            {RecordingRewriteEdge{.parentId = window.getId(), .childId = assigner.getId()}}};

    QueryManagerState state;
    state.queries.emplace(queryId, DistributedQuery({{worker, {localQueryId}}}));
    state.recordingCatalog.upsertQueryMetadata(
        queryId,
        ReplayableQueryMetadata{
            .originalPlan = basePlan,
            .globalPlan = basePlan,
            .replaySpecification = replaySpecification,
            .selectedRecordings = {recordingId},
            .networkExplanations = {},
            .queryPlanRewrite =
                QueryRecordingPlanRewrite{
                    .queryId = queryId.getRawValue(),
                    .basePlan = basePlan,
                    .insertions = {}},
            .replayBoundaries = {replayBoundary}});
    state.recordingCatalog.upsertRecording(
        RecordingEntry{
            .id = recordingId,
            .node = worker,
            .filePath = recordingFilePath.string(),
            .structuralFingerprint = replayBoundary.selection.structuralFingerprint,
            .retentionWindowMs = replaySpecification.retentionWindowMs,
            .representation = RecordingRepresentation::BinaryStore,
            .ownerQueries = {queryId},
            .lifecycleState = Replay::RecordingLifecycleState::Ready,
            .retainedStartWatermark = Timestamp(0),
            .retainedEndWatermark = Timestamp(70'000),
            .fillWatermark = Timestamp(70'000),
            .segmentCount = 1,
            .storageBytes = 4096,
            .successorRecordingId = std::nullopt});

    auto backendState = std::make_shared<TrackingLifecycleBackendState>();
    backendState->workerStatusResult = [recordingId, recordingFilePath]() -> std::expected<WorkerStatus, Exception>
    {
        WorkerStatus status;
        status.until = std::chrono::system_clock::now();
        status.replayMetrics.recordingStatuses.push_back(
            Replay::RecordingRuntimeStatus{
                .recordingId = recordingId.getRawValue(),
                .filePath = recordingFilePath.string(),
                .lifecycleState = Replay::RecordingLifecycleState::Ready,
                .retentionWindowMs = 60'000,
                .retainedStartWatermark = Timestamp(0),
                .retainedEndWatermark = Timestamp(70'000),
                .fillWatermark = Timestamp(70'000),
                .segmentCount = 1,
                .storageBytes = 4096,
                .successorRecordingId = std::nullopt});
        return status;
    }();
    auto queryManager = std::make_shared<QueryManager>(
        workerCatalog,
        [backendState](const WorkerConfig&) { return std::make_unique<TrackingLifecycleBackend>(backendState); },
        std::move(state));
    QueryStatementHandler handler(queryManager, optimizer, sourceCatalog);

    const auto result = handler(ReplayStatement{.queryId = queryId, .intervalStartMs = 1'000, .intervalEndMs = 5'000});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::UnsupportedQuery);
    EXPECT_TRUE(backendState->registeredPlans.empty());

    ASSERT_THAT(queryManager->getReplayExecutions(), testing::SizeIs(1));
    const auto& replayExecution = queryManager->getReplayExecutions().begin()->second;
    EXPECT_EQ(replayExecution.state, ReplayExecutionState::Failed);
    EXPECT_TRUE(replayExecution.internalQueryIds.empty());
    EXPECT_TRUE(replayExecution.pinnedSegments.empty());
    ASSERT_TRUE(replayExecution.failureReason.has_value());
    EXPECT_THAT(*replayExecution.failureReason, testing::HasSubstr("payload-derived event-time semantics"));

    std::error_code errorCode;
    std::filesystem::remove_all(recordingFilePath.parent_path(), errorCode);
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
