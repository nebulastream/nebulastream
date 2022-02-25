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

#include <NesBaseTest.hpp>
#include <gtest/gtest.h>

#include <Monitoring/MonitoringCatalog.hpp>
#include <Monitoring/MonitoringPlan.hpp>

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/MetricCollectors/NetworkCollector.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Storage/MetricStore.hpp>

namespace NES {
using namespace Configurations;
using namespace Runtime;

class MetricStoreTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    uint64_t bufferSize = 0;

    static void SetUpTestCase() {
        NES::setupLogging("MetricStoreTest.log", NES::LOG_DEBUG);
        NES_INFO("ResourcesReaderTest: Setup MetricStoreTest test class.");
    }

    static void TearDownTestCase() { std::cout << "MetricStoreTest: Tear down MetricStoreTest class." << std::endl; }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        std::cout << "MetricStoreTest: Setup MetricStoreTest test case." << std::endl;

        unsigned int numCPU = std::thread::hardware_concurrency();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        bufferSize = 4096 + (numCPU + 1) * sizeof(CpuMetrics) + sizeof(CpuMetricsWrapper);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "MetricStoreTest: Tear down MetricStoreTest test case." << std::endl; }
};

TEST_F(MetricStoreTest, testMetricStore) {
    uint64_t nodeId = 0;
    auto metricStore = std::make_shared<MetricStore>(MetricStoreStrategy::NEWEST);
    auto networkCollector = NetworkCollector();

    uint64_t myInt = 12345;
    std::string myString = "testString";
    MetricPtr networkMetrics = std::make_shared<Metric>(networkCollector.readMetric());

    metricStore->addMetrics(nodeId, std::make_shared<Metric>(myInt));
    metricStore->addMetrics(nodeId, std::make_shared<Metric>(myString));
    metricStore->addMetrics(nodeId, networkMetrics);

    auto storedMetrics = metricStore->getNewestMetrics(nodeId);
    ASSERT_EQ(storedMetrics.size(), 2);

    metricStore->removeMetrics(nodeId);
    ASSERT_FALSE(metricStore->hasMetrics(nodeId));
}

}// namespace NES