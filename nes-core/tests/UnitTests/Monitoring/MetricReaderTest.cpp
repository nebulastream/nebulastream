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

#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>

#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <cpprest/json.h>

namespace NES {

class MetricReaderTest : public testing::Test {
  public:
    AbstractSystemResourcesReaderPtr resourcesReader;

    static void SetUpTestCase() {
        NES::setupLogging("MetricReaderTest.log", NES::LOG_DEBUG);
        NES_INFO("MetricReaderTest: Setup MetricReaderTest test class.");
    }

    static void TearDownTestCase() { std::cout << "MetricReaderTest: Tear down MetricReaderTest class." << std::endl; }

    /* Will be called before a  test is executed. */
    void SetUp() override { resourcesReader = SystemResourcesReaderFactory::getSystemResourcesReader(); }
};

TEST_F(MetricReaderTest, testAbstractSystemResourcesReader) {
    auto resourcesReader = std::make_shared<AbstractSystemResourcesReader>();
    EXPECT_TRUE(resourcesReader->readRuntimeNesMetrics() == RuntimeMetrics{});
    EXPECT_TRUE(resourcesReader->readRegistrationMetrics() == RegistrationMetrics{});
    EXPECT_TRUE(resourcesReader->readCpuStats() == CpuMetricsWrapper{});
    EXPECT_TRUE(resourcesReader->readNetworkStats() == NetworkMetricsWrapper{});
    EXPECT_TRUE(resourcesReader->readMemoryStats() == MemoryMetrics{});
    EXPECT_TRUE(resourcesReader->readDiskStats() == DiskMetrics{});
    EXPECT_TRUE(resourcesReader->getWallTimeInNs() == 0);
}

TEST_F(MetricReaderTest, testRuntimeNesMetrics) {
    auto runtimeMetrics = resourcesReader->readRuntimeNesMetrics();
    NES_DEBUG("MonitoringStackTest: Runtime metrics=" << runtimeMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, runtimeMetrics));
}

TEST_F(MetricReaderTest, testStaticNesMetrics) {
    auto staticMetrics = resourcesReader->readRegistrationMetrics();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << staticMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, staticMetrics));
}

TEST_F(MetricReaderTest, testCPUStats) {
    auto cpuMetrics = resourcesReader->readCpuStats();
    NES_DEBUG("MonitoringStackTest: CPU metrics=" << cpuMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, cpuMetrics));
}

TEST_F(MetricReaderTest, testMemoryStats) {
    auto memMetrics = resourcesReader->readMemoryStats();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << memMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, memMetrics));
}

TEST_F(MetricReaderTest, testDiskStats) {
    auto diskMetrics = resourcesReader->readDiskStats();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << diskMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, diskMetrics));
}

TEST_F(MetricReaderTest, testNetworkStats) {
    auto networkMetrics = resourcesReader->readNetworkStats();
    NES_DEBUG("MonitoringStackTest: Static metrics=" << networkMetrics.toJson());
    EXPECT_TRUE(MetricValidator::isValid(resourcesReader, networkMetrics));
}

}// namespace NES