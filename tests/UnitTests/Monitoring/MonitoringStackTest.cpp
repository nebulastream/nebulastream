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

class MonitoringStackTest : public testing::Test {
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

TEST_F(MonitoringStackTest, testCPUStats) {
    auto cpuStats = MetricUtils::CPUStats();
    CpuMetrics cpuMetrics = cpuStats.measure();
    EXPECT_TRUE(cpuMetrics.getNumCores() > 0);
    for (int i = 0; i < cpuMetrics.getNumCores(); i++) {
        EXPECT_TRUE(cpuMetrics.getValues(i).user > 0);
    }
    EXPECT_TRUE(cpuMetrics.getTotal().user > 0);

    auto cpuIdle = MetricUtils::CPUIdle(0);
    NES_INFO("MonitoringStackTest: Idle " << cpuIdle.measure());
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memStats = MetricUtils::MemoryStats();
    auto memMetrics = memStats.measure();
    EXPECT_TRUE(memMetrics.FREE_RAM > 0);

    NES_INFO("MonitoringStackTest: Total ram " << memMetrics.TOTAL_RAM / (1024 * 1024) << "gb");
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskStats = MetricUtils::DiskStats();
    auto diskMetrics = diskStats.measure();
    EXPECT_TRUE(diskMetrics.fBavail > 0);
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkStats = MetricUtils::NetworkStats();
    auto networkMetrics = networkStats.measure();
    EXPECT_TRUE(!networkMetrics.getInterfaceNames().empty());

    for (std::string intfs : networkMetrics.getInterfaceNames()) {
        NES_INFO("MonitoringStackTest: Received metrics for interface " << intfs);
    }
}

TEST_F(MonitoringStackTest, testMetric) {
    Gauge<CpuMetrics> cpuStats = MetricUtils::CPUStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::NetworkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::DiskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::MemoryStats();

    auto metrics = std::vector<Metric>();
    auto metricsMap = std::unordered_map<std::string, Metric>();

    // test with simple data types
    metrics.emplace_back(1);
    Metric m0 = metrics[0];
    EXPECT_TRUE(getMetricType(m0) == MetricType::UnknownType);
    int valueInt = m0.getValue<int>();
    EXPECT_TRUE(valueInt == 1);
    metricsMap.insert({"sdf", 1});

    metrics.emplace_back(std::string("test"));
    Metric m1 = metrics[1];
    std::string valueString = m1.getValue<std::string>();
    EXPECT_TRUE(valueString == "test");

    // test cpu stats
    metrics.emplace_back(cpuStats);
    Metric m2 = metrics[2];
    EXPECT_TRUE(getMetricType(m2) == MetricType::GaugeType);
    Gauge<CpuMetrics> cpuMetrics = m2.getValue<Gauge<CpuMetrics>>();
    EXPECT_TRUE(cpuStats.measure().getNumCores() == cpuMetrics.measure().getNumCores());

    // test network stats
    metrics.emplace_back(networkStats);
    auto networkMetrics = metrics[3].getValue<Gauge<NetworkMetrics>>();
    EXPECT_TRUE(networkStats.measure().getInterfaceNum() == networkMetrics.measure().getInterfaceNum());

    // test disk stats
    metrics.emplace_back(diskStats);
    auto diskMetrics = metrics[4].getValue<Gauge<DiskMetrics>>();
    EXPECT_TRUE(diskStats.measure().fBavail == diskMetrics.measure().fBavail);

    // test mem stats
    metrics.emplace_back(memStats);
    auto memMetrics = metrics[5].getValue<Gauge<MemoryMetrics>>();
    EXPECT_TRUE(memStats.measure().TOTAL_RAM == memMetrics.measure().TOTAL_RAM);
}

TEST_F(MonitoringStackTest, testMetricGroup) {
    MetricGroupPtr metricGroup = MetricGroup::create();

    Gauge<CpuMetrics> cpuStats = MetricUtils::CPUStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::NetworkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::DiskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::MemoryStats();

    // test with simple data types
    auto intS = "simpleInt";
    metricGroup->add(intS, 1);
    int valueInt = metricGroup->getAs<int>(intS);
    EXPECT_TRUE(valueInt == 1);

    auto stringS = "simpleString";
    metricGroup->add(stringS, std::string("test"));
    std::string valueString = metricGroup->getAs<std::string>(stringS);
    EXPECT_TRUE(valueString == "test");

    // test cpu stats
    auto cpuS = "cpuStats";
    metricGroup->add(cpuS, cpuStats);
    Gauge<CpuMetrics> cpuMetrics = metricGroup->getAs<Gauge<CpuMetrics>>(cpuS);
    EXPECT_TRUE(cpuStats.measure().getNumCores() == cpuMetrics.measure().getNumCores());

    // test network stats
    auto networkS = "networkStats";
    metricGroup->add(networkS, networkStats);
    auto networkMetrics = metricGroup->getAs<Gauge<NetworkMetrics>>(networkS);
    EXPECT_TRUE(networkStats.measure().getInterfaceNum() == networkMetrics.measure().getInterfaceNum());

    // test disk stats
    auto diskS = "diskStats";
    metricGroup->add(diskS, diskStats);
    auto diskMetrics = metricGroup->getAs<Gauge<DiskMetrics>>(diskS);
    EXPECT_TRUE(diskStats.measure().fBavail == diskMetrics.measure().fBavail);

    // test mem stats
    auto memS = "memStats";
    metricGroup->add(memS, memStats);
    auto memMetrics = metricGroup->getAs<Gauge<MemoryMetrics>>(memS);
    EXPECT_TRUE(memStats.measure().TOTAL_RAM == memMetrics.measure().TOTAL_RAM);
}

/**
 * @brief Tests that the schema from the sample method and the metric group are the same, so that no inconsistencies occur
 */
TEST_F(MonitoringStackTest, testIndependentSamplingAndGrouping) {
    MetricGroupPtr metricGroup = MetricGroup::create();

    Gauge<CpuMetrics> cpuStats = MetricUtils::CPUStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::NetworkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::DiskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::MemoryStats();

    // add with simple data types
    metricGroup->add("simpleInt_", 1);

    // add cpu stats
    metricGroup->add(MonitoringPlan::CPU_METRICS_DESC, cpuStats);

    // add network stats
    metricGroup->add(MonitoringPlan::NETWORK_METRICS_DESC, networkStats);

    // add disk stats
    metricGroup->add(MonitoringPlan::DISK_METRICS_DESC, diskStats);

    // add mem stats
    metricGroup->add(MonitoringPlan::MEMORY_METRICS_DESC, memStats);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto schema = Schema::create();
    metricGroup->getSample(tupleBuffer);

    //TODO: add assert to test that the schema is correct
    NES_INFO(metricGroup->createSchema()->toString());
}

TEST_F(MonitoringStackTest, requestMonitoringDataFromGrpcClient) {
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

    std::string destAddress = "127.0.0.1:" + std::to_string(port + 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto schema = crd->workerRpcClient->requestMonitoringData(destAddress, plan, tupleBuffer);

    NES_INFO("MonitoringStackTest: Coordinator requested monitoring data from worker 127.0.0.1:" + std::to_string(port + 10));
    EXPECT_TRUE(schema->getSize() > 1);
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));

    NES_INFO("MonitoringStackTest: Stopping worker");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MonitoringStackTest: stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MonitoringStackTest, requestMonitoringDataFromServiceAsBuffer) {
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

    auto iterations = 50;

    for (int i = 0; i <= iterations; i++) {
        auto tupleBuffer = bufferManager->getBufferBlocking();
        NES_INFO("MonitoringStackTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        crd->getMonitoringService()->requestMonitoringData("127.0.0.1", port + 10, plan, tupleBuffer);

        NES_INFO(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));

        EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
        tupleBuffer.release();
    }

    NES_INFO("MonitoringStackTest: Stopping worker");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MonitoringStackTest: stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MonitoringStackTest, requestMonitoringDataFromServiceAsJson) {
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

    auto iterations = 1;

    for (int i = 0; i <= iterations; i++) {
        NES_INFO("MonitoringStackTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = crd->getMonitoringService()->requestMonitoringDataAsJson("127.0.0.1", port + 10, plan);

        NES_INFO(json.to_string());
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