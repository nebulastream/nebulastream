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

#include <WorkerCatalog.hpp>

#include <chrono>
#include <gtest/gtest.h>

namespace NES
{

TEST(WorkerCatalogTest, UpdateWorkerRuntimeMetricsStoresSnapshotWithoutChangingTopologyVersion)
{
    WorkerCatalog catalog;
    ASSERT_TRUE(catalog.addWorker(Host("worker-1:8080"), "worker-1-data", INFINITE_CAPACITY, {}));
    const auto version = catalog.getVersion();
    const auto observedAt = std::chrono::system_clock::now();

    EXPECT_TRUE(catalog.updateWorkerRuntimeMetrics(
        Host("worker-1:8080"),
        WorkerRuntimeMetrics{
            .observedAt = observedAt,
            .recordingStorageBytes = 42,
            .recordingFileCount = 3,
            .activeQueryCount = 1}));
    EXPECT_EQ(catalog.getVersion(), version);

    const auto metrics = catalog.getWorkerRuntimeMetrics(Host("worker-1:8080"));
    ASSERT_TRUE(metrics.has_value());
    EXPECT_EQ(
        metrics.value(),
        (WorkerRuntimeMetrics{
            .observedAt = observedAt,
            .recordingStorageBytes = 42,
            .recordingFileCount = 3,
            .activeQueryCount = 1}));
}

TEST(WorkerCatalogTest, UpdateWorkerRuntimeMetricsRejectsUnknownWorker)
{
    WorkerCatalog catalog;
    EXPECT_FALSE(catalog.updateWorkerRuntimeMetrics(Host("missing-worker:8080"), WorkerRuntimeMetrics{}));
    EXPECT_FALSE(catalog.getWorkerRuntimeMetrics(Host("missing-worker:8080")).has_value());
}

TEST(WorkerCatalogTest, UpdateWorkerRuntimeMetricsDerivesRecordingWriteRateFromSuccessiveSnapshots)
{
    WorkerCatalog catalog;
    ASSERT_TRUE(catalog.addWorker(Host("worker-1:8080"), "worker-1-data", INFINITE_CAPACITY, {}));

    ASSERT_TRUE(catalog.updateWorkerRuntimeMetrics(
        Host("worker-1:8080"),
        WorkerRuntimeMetrics{
            .observedAt = std::chrono::system_clock::time_point(std::chrono::seconds(1)),
            .recordingStorageBytes = 1024,
            .recordingFileCount = 1,
            .activeQueryCount = 1}));
    ASSERT_TRUE(catalog.updateWorkerRuntimeMetrics(
        Host("worker-1:8080"),
        WorkerRuntimeMetrics{
            .observedAt = std::chrono::system_clock::time_point(std::chrono::seconds(3)),
            .recordingStorageBytes = 5120,
            .recordingFileCount = 2,
            .activeQueryCount = 2}));

    const auto metrics = catalog.getWorkerRuntimeMetrics(Host("worker-1:8080"));
    ASSERT_TRUE(metrics.has_value());
    EXPECT_EQ(metrics->recordingWriteBytesPerSecond, 2048);
}

}
