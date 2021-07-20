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

#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <Monitoring/Util/SystemResourcesReader.hpp>
#include <Services/MonitoringService.hpp>
#include <Util/Logger.hpp>

namespace NES {

class MonitoringStackTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MonitoringStackTest.log", NES::LOG_DEBUG);
        NES_INFO("MonitoringStackTest: Setup MonitoringStackTest test class.");
    }

    static void TearDownTestCase() { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest class." << std::endl; }

    /* Will be called before a  test is executed. */
    void SetUp() override { std::cout << "MonitoringStackTest: Setup MonitoringStackTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl; }
};

TEST_F(MonitoringStackTest, testRuntimeNesMetrics) {
    auto runtimeMetrics = SystemResourcesReader::ReadRuntimeNesMetrics();
    NES_DEBUG("MonitoringStackTest: Runtime metrics=" << runtimeMetrics.toJson());

    EXPECT_TRUE(runtimeMetrics.wallTimeNs <= SystemResourcesReader::getWallTimeInNs());
    EXPECT_TRUE(runtimeMetrics.blkioBytesWritten >= 0);
    EXPECT_TRUE(runtimeMetrics.blkioBytesRead >= 0);
    EXPECT_TRUE(runtimeMetrics.memoryUsageInBytes > 0);
    EXPECT_TRUE(runtimeMetrics.cpuLoadInJiffies > 0);
    EXPECT_TRUE(runtimeMetrics.longCoord >= 0);
    EXPECT_TRUE(runtimeMetrics.latCoord >= 0);
    EXPECT_TRUE(runtimeMetrics.batteryStatus >= 0);
}

TEST_F(MonitoringStackTest, testStaticNesMetrics) {
    auto staticMetrics = SystemResourcesReader::ReadStaticNesMetrics();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << staticMetrics.toJson());

    EXPECT_TRUE(staticMetrics.cpuQuotaUS >= -1);
    EXPECT_TRUE(staticMetrics.cpuPeriodUS >= -1);
    EXPECT_TRUE(staticMetrics.totalCPUJiffies > 0);
    EXPECT_TRUE(staticMetrics.cpuCoreNum > 0);
    EXPECT_TRUE(staticMetrics.totalMemoryBytes > 0 );
    EXPECT_TRUE(staticMetrics.hasBattery == false);
    EXPECT_TRUE(staticMetrics.isMoving == false);
}

TEST_F(MonitoringStackTest, testCPUStats) {
    auto cpuStats = MetricUtils::CPUStats();
    CpuMetrics cpuMetrics = cpuStats.measure();
    EXPECT_TRUE(cpuMetrics.getNumCores() > 0);
    for (int i = 0; i < cpuMetrics.getNumCores(); i++) {
        EXPECT_TRUE(cpuMetrics.getValues(i).user > 0);
    }
    EXPECT_TRUE(cpuMetrics.getTotal().user > 0);

    auto cpuIdle = MetricUtils::CPUIdle(0);
    NES_INFO("MonitoringStackTest: Idle " << cpuIdle.measure());
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memStats = MetricUtils::MemoryStats();
    auto memMetrics = memStats.measure();
    EXPECT_TRUE(memMetrics.FREE_RAM > 0);

    NES_INFO("MonitoringStackTest: Total ram " << memMetrics.TOTAL_RAM / (1024 * 1024) << "gb");
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskStats = MetricUtils::DiskStats();
    auto diskMetrics = diskStats.measure();
    EXPECT_TRUE(diskMetrics.fBavail >= 0);
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkStats = MetricUtils::NetworkStats();
    auto networkMetrics = networkStats.measure();
    EXPECT_TRUE(!networkMetrics.getInterfaceNames().empty());

    for (const std::string& intfs : networkMetrics.getInterfaceNames()) {
        NES_INFO("MonitoringStackTest: Received metrics for interface " << intfs);
    }
}

TEST_F(MonitoringStackTest, testMetric) {
    Gauge<CpuMetrics> cpuStats = MetricUtils::CPUStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::NetworkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::DiskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::MemoryStats();

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
    EXPECT_TRUE(diskStats.measure().fBavail == diskMetrics.measure().fBavail);

    // test mem stats
    metrics.emplace_back(memStats);
    auto memMetrics = metrics[5].getValue<Gauge<MemoryMetrics>>();
    EXPECT_TRUE(memStats.measure().TOTAL_RAM == memMetrics.measure().TOTAL_RAM);
}

TEST_F(MonitoringStackTest, testMetricGroup) {
    MetricGroupPtr metricGroup = MetricGroup::create();

    Gauge<CpuMetrics> cpuStats = MetricUtils::CPUStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::NetworkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::DiskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::MemoryStats();

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
    EXPECT_TRUE(diskStats.measure().fBavail == diskMetrics.measure().fBavail);

    // test mem stats
    const auto* memS = "memStats";
    metricGroup->add(memS, Metric{memStats});
    auto memMetrics = metricGroup->getAs<Gauge<MemoryMetrics>>(memS);
    EXPECT_TRUE(memStats.measure().TOTAL_RAM == memMetrics.measure().TOTAL_RAM);
}

}// namespace NES