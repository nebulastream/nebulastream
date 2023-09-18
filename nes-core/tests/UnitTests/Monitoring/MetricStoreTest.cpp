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

#include <BaseIntegrationTest.hpp>
#include <gtest/gtest.h>

#include <Monitoring/MonitoringCatalog.hpp>

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>

#include <Monitoring/MetricCollectors/NetworkCollector.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Storage/AllEntriesMetricStore.hpp>
#include <Monitoring/Storage/LatestEntriesMetricStore.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

namespace NES {
using namespace Configurations;
using namespace Runtime;

class MetricStoreTest : public Testing::BaseUnitTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    uint64_t bufferSize = 0;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MetricStoreTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("ResourcesReaderTest: Setup MetricStoreTest test class.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("MetricStoreTest: Setup MetricStoreTest test case.");

        unsigned int numCPU = std::thread::hardware_concurrency();
        bufferSize = (numCPU + 1) * sizeof(Monitoring::CpuMetrics) + sizeof(Monitoring::CpuMetricsWrapper);
        bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, 10);
    }
};

TEST_F(MetricStoreTest, testNewestEntryMetricStore) {
    uint64_t nodeId = 0;
    auto metricStore = std::make_shared<Monitoring::LatestEntriesMetricStore>();
    auto networkCollector = Monitoring::NetworkCollector();

    uint64_t myInt = 12345;
    std::string myString = "testString";
    Monitoring::MetricPtr networkMetrics = networkCollector.readMetric();

    metricStore->addMetrics(nodeId, networkMetrics);
    metricStore->addMetrics(nodeId, std::make_shared<Monitoring::Metric>(myInt));
    metricStore->addMetrics(nodeId, std::make_shared<Monitoring::Metric>(myString));

    Monitoring::StoredNodeMetricsPtr storedMetrics = metricStore->getAllMetrics(nodeId);
    NES_INFO("MetricStoreTest: Stored metrics {}", Monitoring::MetricUtils::toJson(storedMetrics).dump());
    ASSERT_EQ(storedMetrics->size(), 2);
    ASSERT_EQ(storedMetrics->at(Monitoring::MetricType::UnknownMetric)->size(), 1);

    metricStore->removeMetrics(nodeId);
    ASSERT_FALSE(metricStore->hasMetrics(nodeId));
}

TEST_F(MetricStoreTest, testAllEntriesMetricStore) {
    uint64_t nodeId = 0;
    auto metricStore = std::make_shared<Monitoring::AllEntriesMetricStore>();
    auto networkCollector = Monitoring::NetworkCollector();

    uint64_t myInt = 12345;
    std::string myString = "testString";
    Monitoring::MetricPtr networkMetrics = networkCollector.readMetric();

    metricStore->addMetrics(nodeId, std::make_shared<Monitoring::Metric>(myInt));
    metricStore->addMetrics(nodeId, std::make_shared<Monitoring::Metric>(myString));
    metricStore->addMetrics(nodeId, networkMetrics);

    auto storedMetrics = metricStore->getAllMetrics(nodeId);
    NES_INFO("MetricStoreTest: Stored metrics {}", Monitoring::MetricUtils::toJson(storedMetrics).dump());
    ASSERT_EQ(storedMetrics->size(), 2);
    ASSERT_EQ(storedMetrics->at(Monitoring::MetricType::UnknownMetric)->size(), 2);

    metricStore->removeMetrics(nodeId);
    ASSERT_FALSE(metricStore->hasMetrics(nodeId));
}

}// namespace NES