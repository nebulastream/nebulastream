/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include "../../../tests/util/MetricValidator.hpp"

#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
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

TEST_F(MonitoringStackTest, testAbstractSystemResourcesReader){
    auto resourcesReader = std::make_shared<AbstractSystemResourcesReader>();
    EXPECT_TRUE(resourcesReader->readRuntimeNesMetrics() == RuntimeNesMetrics {});
    EXPECT_TRUE(resourcesReader->readStaticNesMetrics() == StaticNesMetrics {});
    EXPECT_TRUE(resourcesReader->readCpuStats() == CpuMetrics {});
    EXPECT_TRUE(resourcesReader->readNetworkStats() == NetworkMetrics {});
    EXPECT_TRUE(resourcesReader->readMemoryStats() == MemoryMetrics {});
    EXPECT_TRUE(resourcesReader->readDiskStats() == DiskMetrics {});
    EXPECT_TRUE(resourcesReader->getWallTimeInNs() == 0);
}

TEST_F(MonitoringStackTest, testRuntimeNesMetrics) {
    auto runtimeMetrics = resourcesReader->readRuntimeNesMetrics();
    NES_DEBUG("MonitoringStackTest: Runtime metrics=" << runtimeMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, runtimeMetrics));
}

TEST_F(MonitoringStackTest, testStaticNesMetrics) {
    auto staticMetrics = resourcesReader->readStaticNesMetrics();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << staticMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, staticMetrics));
}

TEST_F(MonitoringStackTest, testCPUStats) {
    auto cpuMetrics = resourcesReader->readCpuStats();
    NES_DEBUG("MonitoringStackTest: CPU metrics=" << cpuMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, cpuMetrics));
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memMetrics = resourcesReader->readMemoryStats();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << memMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, memMetrics));
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskMetrics = resourcesReader->readDiskStats();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << diskMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, diskMetrics));
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkMetrics = resourcesReader->readNetworkStats();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << networkMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, networkMetrics));
}

TEST_F(MonitoringStackTest, testMetric) {
    Gauge<CpuMetrics> cpuStats = MetricUtils::cpuStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::networkStats();
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
    Gauge<CpuMetrics> cpuMetrics = m2.getValue<Gauge<CpuMetrics>>();
    EXPECT_TRUE(cpuStats.measure().getNumCores() == cpuMetrics.measure().getNumCores());

    // test network stats
    metrics.emplace_back(networkStats);
    auto networkMetrics = metrics[3].getValue<Gauge<NetworkMetrics>>();
    EXPECT_TRUE(networkStats.measure().getInterfaceNum() == networkMetrics.measure().getInterfaceNum());

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

    Gauge<CpuMetrics> cpuStats = MetricUtils::cpuStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::networkStats();
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
    Gauge<CpuMetrics> cpuMetrics = metricGroup->getAs<Gauge<CpuMetrics>>(cpuS);
    EXPECT_TRUE(cpuStats.measure().getNumCores() == cpuMetrics.measure().getNumCores());

    // test network stats
    const auto* networkS = "networkStats";
    metricGroup->add(networkS, Metric{networkStats});
    auto networkMetrics = metricGroup->getAs<Gauge<NetworkMetrics>>(networkS);
    EXPECT_TRUE(networkStats.measure().getInterfaceNum() == networkMetrics.measure().getInterfaceNum());

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