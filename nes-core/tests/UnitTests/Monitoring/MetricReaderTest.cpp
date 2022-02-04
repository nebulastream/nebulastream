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

#include <Monitoring/Util/SystemsReader/Linux/LinuxReader.hpp>
#include <Monitoring/Util/SystemsReader/SystemsReaderFactory.hpp>

#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeNesMetrics.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Gauge/StaticNesMetrics.hpp>
#include <cpprest/json.h>

namespace NES {

    class MetricReaderTest : public testing::Test {
      public:
        LinuxReaderPtr resourcesReader;

        static void SetUpTestCase() {
            NES::setupLogging("MetricReaderTest.log", NES::LOG_DEBUG);
            NES_INFO("MetricReaderTest: Setup MetricReaderTest test class.");
        }

        static void TearDownTestCase() { std::cout << "MetricReaderTest: Tear down MetricReaderTest class." << std::endl; }

        /* Will be called before a  test is executed. */
        void SetUp() override { resourcesReader = SystemsReaderFactory::getSystemsReader(); }
    };

    TEST_F(MetricReaderTest, testReadingDiskStats) {
        DiskMetrics metrics = resourcesReader->readDiskStats();
        NES_DEBUG("MonitoringStackTest: Disk metrics=" << metrics.toJson());
        EXPECT_TRUE(MetricValidator::isValid(metrics));
    }

    TEST_F(MetricReaderTest, testRuntimeNesMetrics) {
        RuntimeNesMetrics runtimeMetrics = resourcesReader->readRuntimeNesMetrics();
        NES_DEBUG("MonitoringStackTest: Runtime metrics=" << runtimeMetrics.toJson());
        EXPECT_TRUE(MetricValidator::isValid(runtimeMetrics));
    }

    TEST_F(MetricReaderTest, testStaticNesMetrics) {
        auto staticMetrics = resourcesReader->readStaticNesMetrics();
        NES_DEBUG("MonitoringStackTest: Static metrics=" << staticMetrics.toJson());
        EXPECT_TRUE(MetricValidator::isValid(staticMetrics));
    }

    TEST_F(MetricReaderTest, testCPUStats) {
        auto cpuMetrics = resourcesReader->readCpuStats();
        NES_DEBUG("MonitoringStackTest: Cpu metrics=" << cpuMetrics.toJson());
        EXPECT_TRUE(MetricValidator::isValid(cpuMetrics));
    }

    TEST_F(MetricReaderTest, testMemoryStats) {
        auto memMetrics = resourcesReader->readMemoryStats();
        NES_DEBUG("MonitoringStackTest: Memory metrics=" << memMetrics.toJson());
        EXPECT_TRUE(MetricValidator::isValid(memMetrics));
    }

    TEST_F(MetricReaderTest, testNetworkStats) {
        auto networkMetrics = resourcesReader->readNetworkStats();
        NES_DEBUG("MonitoringStackTest: Network metrics=" << networkMetrics.toJson());
        EXPECT_TRUE(MetricValidator::isValid(networkMetrics));
    }

}// namespace NES