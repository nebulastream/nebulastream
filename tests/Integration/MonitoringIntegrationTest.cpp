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
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/IntCounter.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>

#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Components/NesWorker.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Monitoring/MetricValues/GroupedValues.hpp>
#include <Services/MonitoringService.hpp>
#include <memory>

#define private public
#include <Components/NesCoordinator.hpp>

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 5000;

class MonitoringIntegrationTest : public testing::Test {
  public:
    NodeEngine::BufferManagerPtr bufferManager;
    std::string ipAddress = "127.0.0.1";
    uint64_t restPort = 8081;
    CoordinatorConfigPtr crdConf;
    WorkerConfigPtr wrkConf;

    static void SetUpTestCase() {
        NES::setupLogging("MonitoringStackTest.log", NES::LOG_DEBUG);
        NES_INFO("MonitoringStackTest: Setup MonitoringStackTest test class.");
    }

    static void TearDownTestCase() { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest class." << std::endl; }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        crdConf = CoordinatorConfig::create();
        wrkConf = WorkerConfig::create();
        crdConf->resetCoordinatorOptions();
        wrkConf->resetWorkerOptions();
        std::cout << "MonitoringStackTest: Setup MonitoringStackTest test case." << std::endl;
        bufferManager = std::make_shared<NodeEngine::BufferManager>(4096, 10);
        rpcPort = rpcPort + 30;
        crdConf->setRpcPort(rpcPort);
        wrkConf->setCoordinatorPort(rpcPort);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl; }
};

TEST_F(MonitoringIntegrationTest, DISABLED_requestMonitoringDataFromGrpcClient) {
    crdConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();

    NES_INFO("MonitoringStackTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0);
    NES_INFO("MonitoringStackTest: Coordinator started successfully");

    NES_INFO("MonitoringStackTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
    bool retStart1 = wrk1->start(false, false);
    EXPECT_TRUE(retStart1);
    NES_INFO("MonitoringStackTest: Worker1 started successfully");

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    NES_INFO("MonitoringStackTest: Worker 1 connected ");

    // requesting the monitoring data
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);
    auto schema = plan->createSchema();

    std::string destAddress = "127.0.0.1:" + std::to_string(port + 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto success = crd->workerRpcClient->requestMonitoringData(destAddress, tupleBuffer, schema->getSchemaSizeInBytes());

    NES_INFO("MonitoringStackTest: Coordinator requested monitoring data from worker 127.0.0.1:" + std::to_string(port + 10));
    EXPECT_TRUE(success);
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));

    NES_INFO("MonitoringStackTest: Stopping worker");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MonitoringStackTest: stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MonitoringIntegrationTest, requestMonitoringDataFromServiceAsJson) {
    crdConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();

    NES_INFO("MonitoringStackTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0);
    NES_INFO("MonitoringStackTest: Coordinator started successfully");

    NES_INFO("MonitoringStackTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
    bool retStart1 = wrk1->start(false, false);
    EXPECT_TRUE(retStart1);
    NES_INFO("MonitoringStackTest: Worker1 started successfully");

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    NES_INFO("MonitoringStackTest: Worker 1 connected ");

    // requesting the monitoring data
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);
    auto schema = plan->createSchema();
    EXPECT_TRUE(schema->getSize() > 1);

    auto nodeNumber = 2;
    auto jsons = crd->getMonitoringService()->requestMonitoringDataFromAllNodesAsJson();
    NES_INFO("MonitoringStackTest: Jsons received: \n" + jsons.serialize());

    EXPECT_EQ(jsons.size(), nodeNumber);
    for (int i = 1; i <= nodeNumber; i++) {
        NES_INFO("MonitoringStackTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                     + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];

        EXPECT_TRUE(json.has_field("disk"));
        EXPECT_EQ(json["disk"].size(), 5);

        EXPECT_TRUE(json.has_field("cpu"));
        auto numCores = json["cpu"]["NUM_CORES"].as_integer();
        EXPECT_EQ(json["cpu"].size(), numCores + 2);

        EXPECT_TRUE(json.has_field("network"));
        EXPECT_TRUE(json["network"].size() > 0);

        EXPECT_TRUE(json.has_field("memory"));
        EXPECT_EQ(json["memory"].size(), 13);
    }

    NES_INFO("MonitoringStackTest: Stopping worker");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MonitoringStackTest: stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES