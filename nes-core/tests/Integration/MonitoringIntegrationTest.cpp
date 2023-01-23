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

#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Util/MetricValidator.hpp>

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Runtime/BufferManager.hpp>

#include <Services/MonitoringService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpprest/json.h>
#include <cstdint>
#include <memory>

using std::cout;
    using std::endl;
    namespace NES {

    uint16_t timeout = 15;

    class MonitoringIntegrationTest : public Testing::NESBaseTest {
      public:
        Runtime::BufferManagerPtr bufferManager;

        static void SetUpTestCase() {
            NES::Logger::setupLogging("MonitoringIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup MonitoringIntegrationTest test class.");
        }

        void SetUp() override {
            Testing::NESBaseTest::SetUp();
            bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
            NES_INFO("MonitoringIntegrationTest: Setting up test with rpc port " << rpcCoordinatorPort << ", rest port " << restPort);
        }
    };

    TEST_F(MonitoringIntegrationTest, requestStoredRegistrationMetrics) {
        uint64_t noWorkers = 2;
        auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

        auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test2"),
                                               TestUtils::workerHealthCheckWaitTime(1)});

        auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test1"),
                                               TestUtils::workerHealthCheckWaitTime(1)});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

        auto jsons = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
        NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());
        //TODO: This should be addressed by issue 2803
        ASSERT_EQ(jsons.size(), noWorkers + 1);
    }

    TEST_F(MonitoringIntegrationTest, requestAllMetricsViaRest) {
        uint64_t noWorkers = 2;
        auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                        TestUtils::restPort(*restPort),
                                                        TestUtils::enableMonitoring(),
                                                        TestUtils::enableDebug()});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

        auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test2"),
                                               TestUtils::workerHealthCheckWaitTime(1),
                                               TestUtils::enableMonitoring()});

        auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test1"),
                                               TestUtils::workerHealthCheckWaitTime(1),
                                               TestUtils::enableMonitoring()});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

        auto jsons = TestUtils::makeMonitoringRestCall("metrics", std::to_string(*restPort));
        NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

        ASSERT_EQ(jsons.size(), noWorkers + 1);

        for (uint64_t i = 1; i <= noWorkers + 1; i++) {
            NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID " << i);
            auto json = jsons[std::to_string(i)];
            NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
            ASSERT_TRUE(MetricValidator::isValidAll(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), json));
            ASSERT_TRUE(MetricValidator::checkNodeIds(json, i));
        }
    }

    TEST_F(MonitoringIntegrationTest, DISABLED_requestStoredMetricsViaRest) {
        uint64_t noWorkers = 2;
        auto coordinator = TestUtils::startCoordinator(
            {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::enableMonitoring()});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

        auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test2"),
                                               TestUtils::workerHealthCheckWaitTime(1),
                                               TestUtils::enableMonitoring()});

        auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test1"),
                                               TestUtils::workerHealthCheckWaitTime(1),
                                               TestUtils::enableMonitoring()});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

        auto jsons = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
        NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

        ASSERT_EQ(jsons.size(), noWorkers + 1);

        for (uint64_t i = 1; i <= noWorkers + 1; i++) {
            NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID " << i);
            auto json = jsons[std::to_string(i)];
            NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
            auto jsonRegistration = json["registration"][0]["value"];
            ASSERT_TRUE(
                MetricValidator::isValidRegistrationMetrics(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(),
                                                            jsonRegistration));
            ASSERT_EQ(jsonRegistration["NODE_ID"], i);
        }
    }

    TEST_F(MonitoringIntegrationTest, DISABLED_requestAllMetricsFromMonitoringStreams) {
        uint64_t noWorkers = 2;
        uint64_t localBuffers = 64;
        uint64_t globalBuffers = 1024 * 128;
        std::set<std::string> expectedMonitoringStreams{"wrapped_network", "wrapped_cpu", "memory", "disk"};

        auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                        TestUtils::restPort(*restPort),
                                                        TestUtils::enableMonitoring(),
                                                        TestUtils::enableDebug(),
                                                        TestUtils::enableMonitoring(true),
                                                        TestUtils::bufferSizeInBytes(32768, true),
                                                        TestUtils::numberOfSlots(50, true),
                                                        TestUtils::numLocalBuffers(localBuffers, true),
                                                        TestUtils::numGlobalBuffers(globalBuffers, true)});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

        auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test2"),
                                               TestUtils::workerHealthCheckWaitTime(1),
                                               TestUtils::enableMonitoring(),
                                               TestUtils::enableDebug(),
                                               TestUtils::numberOfSlots(50),
                                               TestUtils::numLocalBuffers(localBuffers),
                                               TestUtils::numGlobalBuffers(globalBuffers),
                                               TestUtils::bufferSizeInBytes(32768)});

        auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test1"),
                                               TestUtils::workerHealthCheckWaitTime(1),
                                               TestUtils::enableMonitoring(),
                                               TestUtils::enableDebug(),
                                               TestUtils::numberOfSlots(50),
                                               TestUtils::numLocalBuffers(localBuffers),
                                               TestUtils::numGlobalBuffers(globalBuffers),
                                               TestUtils::bufferSizeInBytes(32768)});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

        auto jsonStart = TestUtils::makeMonitoringRestCall("start", std::to_string(*restPort));
        NES_INFO("MonitoringIntegrationTest: Started monitoring streams " << jsonStart);
        ASSERT_EQ(jsonStart.size(), expectedMonitoringStreams.size());

        ASSERT_TRUE(MetricValidator::waitForMonitoringStreamsOrTimeout(expectedMonitoringStreams, 100, *restPort));
        auto jsonMetrics = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));

        // test network metrics
        for (uint64_t i = 1; i <= noWorkers + 1; i++) {
            NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID " << i);
            auto json = jsonMetrics[std::to_string(i)];
            NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
            ASSERT_TRUE(
                MetricValidator::isValidAllStorage(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), json));
            ASSERT_TRUE(MetricValidator::checkNodeIdsStorage(json, i));
        }
    }

    TEST_F(MonitoringIntegrationTest, requestAllMetricsViaRestConfiguration) {
        uint64_t noWorkers = 1;
        std::string configMonitoring01 = " - cpu: attributes: \"nice, user, system\""
                                         " - disk: attributes: \"F_BSIZE, F_BLOCKS, F_FRSIZE\""
                                         " - memory: attributes: \"FREE_RAM, TOTAL_RAM\""
                                         " - network: attributes: \"rBytes, rFifo, tPackets\"";

//        std::string configMonitoring02 = " - cpu: attributes: \"iowait, irq, steal\""
//                                         " - memory: attributes: \"FREE_SWAP, TOTAL_SWAP\""
//                                         " - disk: attributes: \"F_BFREE, F_BAVAIL, F_FRSIZE\"";

        auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                        TestUtils::restPort(*restPort),
                                                        TestUtils::enableMonitoring(),
                                                        TestUtils::enableDebug()});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

        auto worker1 = TestUtils::startWorker(
            {TestUtils::rpcPort(0),
             TestUtils::dataPort(0),
             TestUtils::coordinatorPort(*rpcCoordinatorPort),
             TestUtils::sourceType("DefaultSource"),
             TestUtils::logicalSourceName("default_logical"),
             TestUtils::physicalSourceName("test2"),
             TestUtils::workerHealthCheckWaitTime(1),
             TestUtils::enableMonitoring(),
             TestUtils::monitoringConfiguration(configMonitoring01)
            });

//        auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
//                                               TestUtils::dataPort(0),
//                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
//                                               TestUtils::sourceType("DefaultSource"),
//                                               TestUtils::logicalSourceName("default_logical"),
//                                               TestUtils::physicalSourceName("test1"),
//                                               TestUtils::workerHealthCheckWaitTime(1),
//                                               TestUtils::enableMonitoring(),
//                                               TestUtils::monitoringConfiguration(configMonitoring02)
//        });
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

        NES_DEBUG("Now comes the RestCall!");
        auto jsons = TestUtils::makeMonitoringRestCall("metrics", std::to_string(*restPort));
        NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

        ASSERT_EQ(jsons.size(), noWorkers + 1);

        for (uint64_t i = 1; i <= noWorkers + 1; i++) {
            NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID " << i);
            auto json = jsons[std::to_string(i)];
            NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
            ASSERT_TRUE(MetricValidator::checkNodeIds(json, i));
            if (i == 2) {
                ASSERT_TRUE(json.has_field("disk"));
                ASSERT_TRUE(json.has_field("memory"));
                ASSERT_TRUE(json.has_field("wrapped_cpu"));
                ASSERT_TRUE(json.has_field("wrapped_network"));

            }
//            else if (i == 3) {
//                ASSERT_TRUE(json.has_field("disk"));
//                ASSERT_TRUE(json.has_field("memory"));
//                ASSERT_TRUE(json.has_field("wrapped_cpu"));
//                ASSERT_TRUE(json.has_field("wrapped_network"));
//            }
        }

    }

    TEST_F(MonitoringIntegrationTest, requestAllMetricsFromMonitoringStreamsConfiguration) {
        uint64_t noWorkers = 1;
        uint64_t localBuffers = 64;
        uint64_t globalBuffers = 1024 * 128;

        std::set<std::string> expectedMonitoringStreams {"wrapped_network", "wrapped_cpu", "memory", "disk"};
        std::string configMonitoring01 = " - cpu: attributes: \"user, nice, system, idle, iowait, irq, softirq, steal, guest, guestnice\" "
                                         "cores: \"0, 2, 3, 6\""
                                         " - disk: attributes: \"F_BSIZE, F_FRSIZE, F_BLOCKS, F_BFREE, F_BAVAIL\" "
                                         " - memory: attributes: \"TOTAL_RAM, TOTAL_SWAP, FREE_RAM, SHARED_RAM, BUFFER_RAM, "
                                         "FREE_SWAP, TOTAL_HIGH, FREE_HIGH, PROCS, MEM_UNIT, LOADS_1MIN, LOADS_5MIN, LOADS_15MIN\" "
                                         " - network: attributes: \"rBytes, rPackets, rErrs, rDrop, rFifo, rFrame, rCompressed, "
                                         "rMulticast, tBytes, tPackets, tErrs, tDrop, tFifo, tCools, tCarrier, tCompressed\" ";
        //    std::set<std::string> expectedMonitoringStreams {"wrapped_cpu, disk"};
//        std::string configMonitoring02 = " - cpu: attributes: \"user, nice, system, idle, iowait, irq, softirq, steal, guest, guestnice\" "
//                                         " - disk: attributes: \"F_BSIZE, F_FRSIZE, F_BLOCKS, F_BFREE, F_BAVAIL\" ";

        auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                        TestUtils::restPort(*restPort),
                                                        TestUtils::enableMonitoring(),
                                                        TestUtils::enableDebug(),
                                                        TestUtils::enableMonitoring(true),
                                                        TestUtils::bufferSizeInBytes(32768, true),
                                                        TestUtils::numberOfSlots(50, true),
                                                        TestUtils::numLocalBuffers(localBuffers, true),
                                                        TestUtils::numGlobalBuffers(globalBuffers, true)});
        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

        auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                               TestUtils::dataPort(0),
                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                               TestUtils::sourceType("DefaultSource"),
                                               TestUtils::logicalSourceName("default_logical"),
                                               TestUtils::physicalSourceName("test2"),
                                               TestUtils::workerHealthCheckWaitTime(1),
                                               TestUtils::enableMonitoring(),
                                               TestUtils::enableDebug(),
                                               TestUtils::numberOfSlots(50),
                                               TestUtils::numLocalBuffers(localBuffers),
                                               TestUtils::numGlobalBuffers(globalBuffers),
                                               TestUtils::bufferSizeInBytes(32768),
                                               TestUtils::monitoringConfiguration(configMonitoring01)});

//        auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
//                                               TestUtils::dataPort(0),
//                                               TestUtils::coordinatorPort(*rpcCoordinatorPort),
//                                               TestUtils::sourceType("DefaultSource"),
//                                               TestUtils::logicalSourceName("default_logical"),
//                                               TestUtils::physicalSourceName("test1"),
//                                               TestUtils::workerHealthCheckWaitTime(1),
//                                               TestUtils::enableMonitoring(),
//                                               TestUtils::enableDebug(),
//                                               TestUtils::numberOfSlots(50),
//                                               TestUtils::numLocalBuffers(localBuffers),
//                                               TestUtils::numGlobalBuffers(globalBuffers),
//                                               TestUtils::bufferSizeInBytes(32768),
//                                               TestUtils::monitoringConfiguration(configMonitoring02)});

        EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

//        auto jsonStart = TestUtils::makeMonitoringRestCall("start", std::to_string(*restPort));
//        NES_INFO("MonitoringIntegrationTest: Started monitoring streams " << jsonStart);
//        //    ASSERT_EQ(jsonStart.size(), expectedMonitoringStreams.size() * 2);
//
//        //    ASSERT_TRUE(MetricValidator::waitForMonitoringStreamsOrTimeout(expectedMonitoringStreams, 100, *restPort));
//        sleep(30);
//        auto jsonMetrics = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
//
//        auto json = jsonMetrics[std::to_string(1)];
//        NES_DEBUG("MonitoringIntegrationTest: JSON for node " << 1 << ":\n" << json);
//        sleep(30);
//        jsonMetrics = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
//        json = jsonMetrics[std::to_string(2)];
//        NES_DEBUG("MonitoringIntegrationTest: JSON for node " << 2 << ":\n" << json);
//        sleep(30);
//        jsonMetrics = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
//        json = jsonMetrics[std::to_string(3)];
//        NES_DEBUG("MonitoringIntegrationTest: JSON for node " << 3 << ":\n" << json);

        auto jsonStart = TestUtils::makeMonitoringRestCall("start", std::to_string(*restPort));
        NES_INFO("MonitoringIntegrationTest: Started monitoring streams " << jsonStart);
//        ASSERT_EQ(jsonStart.size(), expectedMonitoringStreams.size());

        ASSERT_TRUE(MetricValidator::waitForMonitoringStreamsOrTimeout(expectedMonitoringStreams, 100, *restPort));
        auto jsonMetrics = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));

        // test network metrics
        for (uint64_t i = 1; i <= noWorkers + 1; i++) {
            NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID " << i);
            auto json = jsonMetrics[std::to_string(i)];
            NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
//            ASSERT_TRUE(MetricValidator::isValidAllStorage(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), json));
            ASSERT_TRUE(MetricValidator::checkNodeIdsStorage(json, i));
        }
    }
}// namespace NES