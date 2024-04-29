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
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>

namespace NES {

class NesCoordinatorTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("CoordinatorTest.log", NES::LogLevel::LOG_DEBUG); }
};

// Test that the worker configuration from the coordinator configuration is passed to the internal worker.
TEST_F(NesCoordinatorTest, internalWorkerUsesConfigurationFromCoordinatorConfiguration) {
    // given
    auto configuration = CoordinatorConfiguration::createDefault();
    configuration->rpcPort = *rpcCoordinatorPort;
    configuration->restPort = *restPort;
    configuration->worker.numWorkerThreads = 3;
    // when
    auto coordinator = std::make_shared<NesCoordinator>(configuration);
    NES_DEBUG("Starting coordinator.")
    coordinator->startCoordinator(false);
    // then
    ASSERT_EQ(3, coordinator->getNesWorker()->getWorkerConfiguration()->numWorkerThreads.getValue());
    // Stop coordinator.
    NES_DEBUG("Stopping coordinator.")
    EXPECT_TRUE(coordinator->stopCoordinator(true));
}

// Test that IP and port of the internal worker are consistent with IP and port from coordinator
TEST_F(NesCoordinatorTest, internalWorkerUsesIpAndPortFromCoordinator) {
    // given: Set up the coordinator IP and ports, and enable monitoring
    auto coordinatorIp = "127.0.0.1";
    auto coordinatorWorkerIp = "127.0.0.3";
    auto configuration = CoordinatorConfiguration::createDefault();
    configuration->rpcPort = *rpcCoordinatorPort;
    configuration->restPort = *restPort;
    configuration->coordinatorIp = coordinatorIp;
    configuration->enableMonitoring = true;
    // given: Configure the worker with nonsensical IP and port, and disable monitoring
    configuration->worker.coordinatorPort = 111;// This port won't be assigned by the line above because it is below 1024.
    configuration->worker.coordinatorIp = "127.0.0.2";
    configuration->worker.localWorkerIp = coordinatorWorkerIp;
    configuration->worker.enableMonitoring = false;
    // when
    auto coordinator = std::make_shared<NesCoordinator>(configuration);
    NES_DEBUG("Starting coordinator.")
    coordinator->startCoordinator(false);
    // then: the IP and port in the worker configuration are overwritten, and monitoring is enabled
    auto workerConfiguration = coordinator->getNesWorker()->getWorkerConfiguration();
    EXPECT_EQ(*rpcCoordinatorPort, workerConfiguration->coordinatorPort.getValue());
    EXPECT_EQ(coordinatorIp, workerConfiguration->coordinatorIp.getValue());
    EXPECT_EQ(coordinatorWorkerIp, workerConfiguration->localWorkerIp.getValue());
    EXPECT_EQ(true, workerConfiguration->enableMonitoring.getValue());
    // Stop coordinator.
    NES_DEBUG("Stopping coordinator.")
    EXPECT_TRUE(coordinator->stopCoordinator(true));
}

}// namespace NES
