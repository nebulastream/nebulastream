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
#include <Util/Logger/Logger.hpp>

#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Storage/NewestEntryMetricStore.hpp>

namespace NES {
using namespace Configurations;
using namespace Runtime;

class ContinuousMonitoringTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    uint64_t bufferSize = 0;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("ContinuousMonitoringTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("ResourcesReaderTest: Setup ContinuousMonitoringTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "ContinuousMonitoringTest: Tear down ContinuousMonitoringTest class." << std::endl;
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        std::cout << "ContinuousMonitoringTest: Setup ContinuousMonitoringTest test case." << std::endl;

        unsigned int numCPU = std::thread::hardware_concurrency();
        bufferSize = (numCPU + 1) * sizeof(CpuMetrics) + sizeof(CpuMetricsWrapper);
        bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, 10);
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        std::cout << "ContinuousMonitoringTest: Tear down ContinuousMonitoringTest test case." << std::endl;
    }
};

TEST_F(ContinuousMonitoringTest, testRuntimeConcepts) {
    web::json::value metricsJson{};
    std::vector<Metric> metrics;

    uint64_t myInt = 12345;
    metrics.emplace_back(myInt);
    std::string myString = "testString";
    metrics.emplace_back(myString);

    for (unsigned int i = 0; i < metrics.size(); i++) {
        metricsJson[i] = asJson(metrics[i]);
    }

    NES_DEBUG("ContinuousMonitoringTest: Json Concepts: " << metricsJson);
}

}// namespace NES