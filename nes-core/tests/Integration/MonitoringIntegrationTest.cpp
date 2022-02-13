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
#include "../util/NesBaseTest.hpp"

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>

#include <Runtime/BufferManager.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>

#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Monitoring/MetricValues/GroupedMetricValues.hpp>
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
        NES::setupLogging("MonitoringIntegrationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MonitoringIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        NES_INFO("MonitoringIntegrationTest: Setting up test with rpc port " << rpcCoordinatorPort << ", rest port " << restPort);
    }
};

TEST_F(MonitoringIntegrationTest, requestMonitoringDataFromServiceAsJson) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->enableMonitoring=(true);
    coordinatorConfig->restPort = *restPort;
    wrkConf1->coordinatorPort=(*rpcCoordinatorPort);
    wrkConf1->enableMonitoring=(true);
    wrkConf2->coordinatorPort=(*rpcCoordinatorPort);
    wrkConf2->enableMonitoring=(true);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);
    auto schema = plan->createSchema();
    EXPECT_TRUE(schema->getSize() > 1);

    auto const nodeNumber = static_cast<std::size_t>(3U);
    auto jsons = crd->getMonitoringService()->requestMonitoringDataFromAllNodesAsJson(bufferManager);
    NES_INFO("MonitoringStackTest: Jsons received: \n" + jsons.serialize());

    EXPECT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("MonitoringStackTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];

        EXPECT_TRUE(json.has_field("disk"));
        EXPECT_EQ(json["disk"].size(), 5U);

        EXPECT_TRUE(json.has_field("cpu"));
        auto numCores = json["cpu"]["NUM_CORES"].as_integer();
        EXPECT_TRUE(numCores > 0);
        EXPECT_EQ(json["cpu"].size(), static_cast<std::size_t>(numCores) + 2U);

        EXPECT_TRUE(json.has_field("network"));
        EXPECT_TRUE(json["network"].size() > 0);

        EXPECT_TRUE(json.has_field("memory"));
        EXPECT_EQ(json["memory"].size(), 13U);
    }

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MonitoringIntegrationTest, requestLocalMonitoringDataFromServiceAsJsonEnabled) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->enableMonitoring=(true);
    coordinatorConfig->restPort = *restPort;
    wrkConf1->coordinatorPort=(*rpcCoordinatorPort);
    wrkConf1->enableMonitoring=(true);
    wrkConf2->coordinatorPort=(*rpcCoordinatorPort);
    wrkConf2->enableMonitoring=(true);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);
    auto schema = plan->createSchema();
    EXPECT_TRUE(schema->getSize() > 1);

    auto const nodeNumber = static_cast<std::size_t>(3U);
    auto jsons = crd->getMonitoringService()->requestNewestMonitoringDataFromMetricStoreAsJson();
    NES_INFO("MonitoringStackTest: Jsons received: \n" + jsons.serialize());

    EXPECT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("MonitoringStackTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];
        EXPECT_TRUE(json.has_field("staticNesMetrics"));
        json = json["staticNesMetrics"];

        EXPECT_TRUE(json.has_field("TotalMemory"));
        EXPECT_TRUE(json["TotalMemory"].as_double() > 1);

        EXPECT_TRUE(json.has_field("CpuCoreNum"));
        EXPECT_TRUE(json["CpuCoreNum"].as_double() >= 1);

        EXPECT_TRUE(json.has_field("TotalCPUJiffies"));
        EXPECT_TRUE(json["TotalCPUJiffies"].as_double() >= 1);

        EXPECT_TRUE(json.has_field("CpuPeriodUS"));
        EXPECT_TRUE(json.has_field("CpuQuotaUS"));
        EXPECT_TRUE(json.has_field("IsMoving"));
        EXPECT_TRUE(json.has_field("HasBattery"));
    }

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MonitoringIntegrationTest, requestLocalMonitoringDataFromServiceAsJsonDisabled) {
    // TODO Refactor this once #2239 is solved.
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    bool monitoring = false;

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->enableMonitoring=(monitoring);
    wrkConf1->coordinatorPort=(*rpcCoordinatorPort);
    wrkConf1->enableMonitoring=(monitoring);
    wrkConf2->coordinatorPort=(*rpcCoordinatorPort);
    wrkConf2->enableMonitoring=(monitoring);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);
    auto schema = plan->createSchema();
    EXPECT_TRUE(schema->getSize() > 1);

    auto const nodeNumber = static_cast<std::size_t>(3U);
    auto jsons = crd->getMonitoringService()->requestNewestMonitoringDataFromMetricStoreAsJson();
    NES_INFO("MonitoringStackTest: Jsons received: \n" + jsons.serialize());

    EXPECT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);
    auto emptyStaticNesMetrics = StaticNesMetrics{};

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("MonitoringStackTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];
        EXPECT_TRUE(json.has_field("staticNesMetrics"));
        json = json["staticNesMetrics"];

        EXPECT_TRUE(json.has_field("TotalMemory"));
        EXPECT_TRUE(json["TotalMemory"].as_double() == emptyStaticNesMetrics.totalMemoryBytes);

        EXPECT_TRUE(json.has_field("CpuCoreNum"));
        EXPECT_TRUE(json["CpuCoreNum"].as_double() == emptyStaticNesMetrics.cpuCoreNum);

        EXPECT_TRUE(json.has_field("TotalCPUJiffies"));
        EXPECT_TRUE(json["TotalCPUJiffies"].as_double() == emptyStaticNesMetrics.totalCPUJiffies);

        EXPECT_TRUE(json.has_field("CpuPeriodUS"));
        EXPECT_TRUE(json.has_field("CpuQuotaUS"));
        EXPECT_TRUE(json.has_field("IsMoving"));
        EXPECT_TRUE(json.has_field("HasBattery"));
    }

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES