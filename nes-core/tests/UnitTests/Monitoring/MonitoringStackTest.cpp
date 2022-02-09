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

#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
#include "../../../tests/util/MetricValidator.hpp"

#include "Monitoring/Metrics/Gauge/CpuMetricsWrapper.hpp"
#include "Monitoring/Metrics/Gauge/DiskMetrics.hpp"
#include "Monitoring/Metrics/Gauge/MemoryMetrics.hpp"
#include "Monitoring/Metrics/Gauge/NetworkMetricsWrapper.hpp"
#include "Monitoring/MonitoringPlan.hpp"
#include <Monitoring/Util/MetricUtils.hpp>

#include "Monitoring/Metrics/Gauge/RuntimeNesMetrics.hpp"
#include "Monitoring/Metrics/Gauge/StaticNesMetrics.hpp"
#include "Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp"
#include <Util/Logger.hpp>
#include <cpprest/json.h>

namespace NES {

class MonitoringStackTest : public testing::Test {
  public:
    AbstractSystemResourcesReaderPtr resourcesReader;
    static void SetUpTestCase() {
        NES::setupLogging("MonitoringStackTest.log", NES::LOG_DEBUG);
        NES_INFO("MonitoringStackTest: Setup MonitoringStackTest test class.");
    }

    static void TearDownTestCase() { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest class." << std::endl; }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        resourcesReader = MetricUtils::getSystemResourcesReader();
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl; }
};

TEST_F(MonitoringStackTest, testMetric) {
    CpuMetricsWrapper cpuStats = MetricUtils::cpuStats();
    Gauge<NetworkMetricsWrapper> networkStats = MetricUtils::networkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::diskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::memoryStats();

    auto metrics = std::vector<Metric>();
    auto metricsMap = std::unordered_map<std::string, Metric>();

    // test with simple data types
    metrics.emplace_back(1);
    Metric m0 = metrics[0];
    EXPECT_TRUE(getMetricType(m0) == MetricType::UnknownType);
    int valueInt = m0.getValue<int>();
    EXPECT_TRUE(valueInt == 1);
    metricsMap.insert({"sdf", Metric{1}});

    metrics.emplace_back(std::string("test"));
    Metric m1 = metrics[1];
    std::string valueString = m1.getValue<std::string>();
    EXPECT_TRUE(valueString == "test");

    // test cpu stats
    metrics.emplace_back(cpuStats);
    Metric m2 = metrics[2];
    EXPECT_TRUE(getMetricType(m2) == MetricType::GaugeType);
    Gauge<CpuMetricsWrapper> cpuMetrics = m2.getValue<Gauge<CpuMetrics>>();
    EXPECT_TRUE(cpuStats.measure().getNumCores() == cpuMetrics.measure().getNumCores());

    // test network stats
    metrics.emplace_back(networkStats);
    auto networkMetrics = metrics[3].getValue<Gauge<NetworkMetrics>>();
    EXPECT_TRUE(networkStats.measure().size() == networkMetrics.measure().size());

    // test disk stats
    metrics.emplace_back(diskStats);
    auto diskMetrics = metrics[4].getValue<Gauge<DiskMetrics>>();
    EXPECT_TRUE(diskMetrics.measure().fBavail >= 0);

    // test mem stats
    metrics.emplace_back(memStats);
    auto memMetrics = metrics[5].getValue<Gauge<MemoryMetrics>>();
    EXPECT_TRUE(memStats.measure().TOTAL_RAM == memMetrics.measure().TOTAL_RAM);
}

TEST_F(MonitoringStackTest, testMetricGroup) {
    MetricGroupPtr metricGroup = MetricGroup::create();

    Gauge<CpuMetricsWrapper> cpuStats = MetricUtils::cpuStats();
    Gauge<NetworkMetricsWrapper> networkStats = MetricUtils::networkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::diskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::memoryStats();

    // test with simple data types
    const auto* intS = "simpleInt";
    metricGroup->add(intS, Metric{1});
    int valueInt = metricGroup->getAs<int>(intS);
    EXPECT_TRUE(valueInt == 1);

    const auto* stringS = "simpleString";
    metricGroup->add(stringS, Metric{std::string("test")});
    std::string valueString = metricGroup->getAs<std::string>(stringS);
    EXPECT_TRUE(valueString == "test");

    // test cpu stats
    const auto* cpuS = "cpuStats";
    metricGroup->add(cpuS, Metric{cpuStats});
    Gauge<CpuMetricsWrapper> cpuMetrics = metricGroup->getAs<Gauge<CpuMetrics>>(cpuS);
    EXPECT_TRUE(cpuStats.measure().getNumCores() == cpuMetrics.measure().getNumCores());

    // test network stats
    const auto* networkS = "networkStats";
    metricGroup->add(networkS, Metric{networkStats});
    auto networkMetrics = metricGroup->getAs<Gauge<NetworkMetrics>>(networkS);
    EXPECT_TRUE(networkStats.measure().size() == networkMetrics.measure().size());

    // test disk stats
    const auto* diskS = "diskStats";
    metricGroup->add(diskS, Metric{diskStats});
    auto diskMetrics = metricGroup->getAs<Gauge<DiskMetrics>>(diskS);
    EXPECT_TRUE(diskMetrics.measure().fBavail >= 0);

    // test mem stats
    const auto* memS = "memStats";
    metricGroup->add(memS, Metric{memStats});
    auto memMetrics = metricGroup->getAs<Gauge<MemoryMetrics>>(memS);
    EXPECT_TRUE(memStats.measure().TOTAL_RAM == memMetrics.measure().TOTAL_RAM);
}

}// namespace NES