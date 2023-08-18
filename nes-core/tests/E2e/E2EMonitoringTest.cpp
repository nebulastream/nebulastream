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

#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Util/MetricValidator.hpp>

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Runtime/BufferManager.hpp>

#include <Util/Logger/Logger.hpp>
#include <memory>
#include <nlohmann/json.hpp>

using std::cout;
using std::endl;
namespace NES {

uint16_t timeout = 15;

class E2EMonitoringTest : public Testing::BaseIntegrationTest {
  public:
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("E2EMonitoringTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup E2EMonitoringTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
    }
};

TEST_F(E2EMonitoringTest, DISABLED_requestStoredRegistrationMetrics) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n{}", jsons.dump());
    ASSERT_EQ(jsons.size(), noWorkers + 1);
}

TEST_F(E2EMonitoringTest, requestAllMetricsViaRest) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("metrics", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n{}", jsons.dump());

    ASSERT_EQ(jsons.size(), noWorkers + 1);
    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID {}", i);
        auto json = jsons[std::to_string(i)];
        NES_DEBUG("E2EMonitoringTest: JSON for node {}:\n{}", i, json.dump());
        auto jsonString = json.dump();
        nlohmann::json jsonLohmann = nlohmann::json::parse(jsonString);
        ASSERT_TRUE(
            MetricValidator::isValidAll(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), jsonLohmann));
        ASSERT_TRUE(MetricValidator::checkNodeIds(jsonLohmann, i));
    }
}

TEST_F(E2EMonitoringTest, requestStoredMetricsViaRest) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n{}", jsons.dump());

    ASSERT_EQ(jsons.size(), noWorkers + 1);

    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID {}", i);
        auto json = jsons[std::to_string(i)];
        NES_DEBUG("E2EMonitoringTest: JSON for node {}:\n{}", i, json.dump());
        auto jsonRegistration = json["RegistrationMetric"][0]["value"];
        auto jsonString = jsonRegistration.dump();
        nlohmann::json jsonRegistrationLohmann = nlohmann::json::parse(jsonString);
        ASSERT_TRUE(
            MetricValidator::isValidRegistrationMetrics(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(),
                                                        jsonRegistrationLohmann));
        ASSERT_EQ(jsonRegistrationLohmann["NODE_ID"], i);
    }
}

TEST_F(E2EMonitoringTest, requestAllMetricsFromMonitoringStreams) {
    auto expectedMonitoringStreams = Monitoring::MonitoringPlan::defaultPlan()->getMetricTypes();
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsonStart = TestUtils::makeMonitoringRestCall("start", std::to_string(*restPort));
    NES_INFO("E2EMonitoringTest: Started monitoring streams {}", jsonStart.dump());
    ASSERT_EQ(jsonStart.size(), expectedMonitoringStreams.size());

    ASSERT_TRUE(MetricValidator::waitForMonitoringStreamsOrTimeout(expectedMonitoringStreams, 100, *restPort));
    auto jsonMetrics = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));

    // test network metrics
    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID {}", i);
        auto json = jsonMetrics[std::to_string(i)];
        NES_DEBUG("E2EMonitoringTest: JSON for node {}:\n{}", i, json.dump());
        auto jsonString = json.dump();
        nlohmann::json jsonLohmann = nlohmann::json::parse(jsonString);
        ASSERT_TRUE(MetricValidator::isValidAllStorage(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(),
                                                       jsonLohmann));
        ASSERT_TRUE(MetricValidator::checkNodeIdsStorage(jsonLohmann, i));
    }
}

}// namespace NES