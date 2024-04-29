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

#include <Util/MetricValidator.hpp>

#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>

#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Util/Logger/Logger.hpp>

#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>

namespace NES {

class ResourcesReaderTest : public Testing::BaseUnitTest {
  public:
    Monitoring::AbstractSystemResourcesReaderPtr resourcesReader;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("ResourcesReaderTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("ResourcesReaderTest: Setup ResourcesReaderTest test class.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        resourcesReader = Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader();
    }
};

TEST_F(ResourcesReaderTest, testAbstractSystemResourcesReader) {
    auto resourcesReader = std::make_shared<Monitoring::AbstractSystemResourcesReader>();
    ASSERT_TRUE(resourcesReader->readRuntimeNesMetrics() == Monitoring::RuntimeMetrics{});
    ASSERT_TRUE(resourcesReader->readRegistrationMetrics() == Monitoring::RegistrationMetrics{});
    ASSERT_TRUE(resourcesReader->readCpuStats().size() == 1);
    ASSERT_TRUE(resourcesReader->readNetworkStats().size() == 1);
    ASSERT_TRUE(resourcesReader->readMemoryStats() == Monitoring::MemoryMetrics{});
    ASSERT_TRUE(resourcesReader->readDiskStats() == Monitoring::DiskMetrics{});
    ASSERT_TRUE(resourcesReader->getWallTimeInNs() == 0);
}

TEST_F(ResourcesReaderTest, testRuntimeNesMetrics) {
    auto runtimeMetrics = resourcesReader->readRuntimeNesMetrics();
    NES_DEBUG("ResourcesReaderTest: Runtime metrics={}", runtimeMetrics.toJson().dump());
    ASSERT_TRUE(MetricValidator::isValid(resourcesReader, runtimeMetrics));
}

TEST_F(ResourcesReaderTest, testStaticNesMetrics) {
    auto staticMetrics = resourcesReader->readRegistrationMetrics();
    NES_DEBUG("ResourcesReaderTest: Static metrics={}", staticMetrics.toJson().dump());
    ASSERT_TRUE(MetricValidator::isValid(resourcesReader, staticMetrics));
}

TEST_F(ResourcesReaderTest, testCPUStats) {
    auto cpuMetrics = resourcesReader->readCpuStats();
    NES_DEBUG("ResourcesReaderTest: CPU metrics={}", cpuMetrics.toJson().dump());
    ASSERT_TRUE(MetricValidator::isValid(resourcesReader, cpuMetrics));
}

TEST_F(ResourcesReaderTest, testMemoryStats) {
    auto memMetrics = resourcesReader->readMemoryStats();
    NES_DEBUG("ResourcesReaderTest: Static metrics={}", memMetrics.toJson().dump());
    ASSERT_TRUE(MetricValidator::isValid(resourcesReader, memMetrics));
}

TEST_F(ResourcesReaderTest, testDiskStats) {
    auto diskMetrics = resourcesReader->readDiskStats();
    NES_DEBUG("ResourcesReaderTest: Static metrics={}", diskMetrics.toJson().dump());
    ASSERT_TRUE(MetricValidator::isValid(resourcesReader, diskMetrics));
}

TEST_F(ResourcesReaderTest, testNetworkStats) {
    auto networkMetrics = resourcesReader->readNetworkStats();
    NES_DEBUG("ResourcesReaderTest: Static metrics={}", networkMetrics.toJson().dump());
    ASSERT_TRUE(MetricValidator::isValid(resourcesReader, networkMetrics));
}

}// namespace NES
