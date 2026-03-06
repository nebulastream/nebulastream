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
#include <expected>
#include <gtest/gtest.h>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
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

}
