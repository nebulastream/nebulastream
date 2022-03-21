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
#include <NesBaseTest.hpp>

#include "../../tests/util/MetricValidator.hpp"
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
//#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Monitoring/MonitoringPlan.hpp>

#include <Runtime/BufferManager.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>

#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/MonitoringService.hpp>
#include <cpprest/json.h>
#include <cstdint>
#include <memory>

using std::cout;
using std::endl;
namespace NES {

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

TEST_F(MonitoringIntegrationTest, requestRuntimeMetricsEnabled) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->enableMonitoring = (true);
    coordinatorConfig->restPort = *restPort;
    wrkConf1->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf1->enableMonitoring = (true);
    wrkConf2->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf2->enableMonitoring = (true);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::set<MetricType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    auto const nodeNumber = static_cast<std::size_t>(3U);
    ASSERT_TRUE(crd->getMonitoringService()->isMonitoringEnabled());
    auto jsons = crd->getMonitoringService()->requestMonitoringDataFromAllNodesAsJson();
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

    ASSERT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("ResourcesReaderTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];
        NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
        ASSERT_TRUE(MetricValidator::isValid(SystemResourcesReaderFactory::getSystemResourcesReader(), json));
    }

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(MonitoringIntegrationTest, requestStoredRegistrationMetricsEnabled) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->enableMonitoring = (true);
    coordinatorConfig->restPort = *restPort;
    wrkConf1->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf1->enableMonitoring = (true);
    wrkConf2->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf2->enableMonitoring = (true);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::set<MetricType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    auto const nodeNumber = static_cast<std::size_t>(3U);
    ASSERT_TRUE(crd->getMonitoringService()->isMonitoringEnabled());
    auto jsons = crd->getMonitoringService()->requestNewestMonitoringDataFromMetricStoreAsJson();
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

    ASSERT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("ResourcesReaderTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];
        ASSERT_TRUE(json.has_field("registration"));
        json = json["registration"];
        ASSERT_TRUE(MetricValidator::isValidRegistrationMetrics(SystemResourcesReaderFactory::getSystemResourcesReader(), json));
    }

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(MonitoringIntegrationTest, requestStoredRegistrationMetricsDisabled) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    bool monitoring = false;

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->enableMonitoring = (monitoring);
    wrkConf1->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf1->enableMonitoring = (monitoring);
    wrkConf2->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf2->enableMonitoring = (monitoring);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::set<MetricType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    auto const nodeNumber = static_cast<std::size_t>(3U);

    ASSERT_FALSE(crd->getMonitoringService()->isMonitoringEnabled());

    auto jsons = crd->getMonitoringService()->requestNewestMonitoringDataFromMetricStoreAsJson();
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

    ASSERT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("ResourcesReaderTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];
        ASSERT_TRUE(json.has_field("registration"));
        json = json["registration"];
        ASSERT_TRUE(MetricValidator::isValidRegistrationMetrics(SystemResourcesReaderFactory::getSystemResourcesReader(), json));
    }

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

}// namespace NES