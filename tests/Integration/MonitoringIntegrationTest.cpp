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

#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
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

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
// TODO use grpc async queue to fix this issue - I am currently increasing the rpc port by 30 on every test! this is very bad!
uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class MonitoringIntegrationTest : public testing::Test {
  public:
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::setupLogging("MonitoringIntegrationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MonitoringIntegrationTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        NES_INFO("MonitoringIntegrationTest: Setting up test with rpc port " << rpcPort << ", rest port " << restPort);
    }

    void TearDown() override { std::cout << "Tear down MonitoringIntegrationTest class." << std::endl; }
};

TEST_F(MonitoringIntegrationTest, requestMonitoringDataFromServiceAsJson) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
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

}// namespace NES