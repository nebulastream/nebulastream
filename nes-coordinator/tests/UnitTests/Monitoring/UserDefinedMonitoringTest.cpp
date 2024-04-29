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
#include <Monitoring/MonitoringPlan.hpp>

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>

#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Storage/LatestEntriesMetricStore.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {
using namespace Configurations;
using namespace Runtime;

class UserDefinedMonitoringTest : public Testing::BaseUnitTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    uint64_t bufferSize = 0;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("UserDefinedMonitoringTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("ResourcesReaderTest: Setup UserDefinedMonitoringTest test class.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("UserDefinedMonitoringTest: Setup UserDefinedMonitoringTest test case.");

        unsigned int numCPU = std::thread::hardware_concurrency();
        bufferSize = (numCPU + 1) * sizeof(Monitoring::CpuMetrics) + sizeof(Monitoring::CpuMetricsWrapper);
        bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, 10);
    }
};

TEST_F(UserDefinedMonitoringTest, testRuntimeConcepts) {
    nlohmann::json metricsJson{};
    std::vector<Monitoring::Metric> metrics;

    uint64_t myInt = 12345;
    metrics.emplace_back(myInt);
    std::string myString = "testString";
    metrics.emplace_back(myString);

    for (unsigned int i = 0; i < metrics.size(); i++) {
        metricsJson[i] = asJson(metrics[i]);
    }

    NES_DEBUG("UserDefinedMonitoringTest: Json Concepts: {}", metricsJson);
}

TEST_F(UserDefinedMonitoringTest, testJsonRuntimeConcepts) {
    auto monitoringPlan = Monitoring::MonitoringPlan::defaultPlan();
    auto monitoringCatalog = Monitoring::MonitoringCatalog::defaultCatalog();
    nlohmann::json metricsJson{};

    for (auto type : monitoringPlan->getMetricTypes()) {
        auto collector = monitoringCatalog->getMetricCollector(type);
        Monitoring::MetricPtr metric = collector->readMetric();
        metricsJson[std::string(magic_enum::enum_name(metric->getMetricType()))] = asJson(metric);
    }
    NES_DEBUG("UserDefinedMonitoringTest: Json Concepts: {}", metricsJson);
}

}// namespace NES
