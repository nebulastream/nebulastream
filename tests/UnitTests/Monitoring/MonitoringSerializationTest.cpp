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

class MonitoringSerializationTest : public testing::Test {
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

TEST_F(MonitoringSerializationTest, testSerializationMetricsCpu) {
    auto cpuStats = MetricUtils::CPUStats();
    auto tupleBuffer = bufferManager->getBufferBlocking();

    CpuMetrics measuredVal = cpuStats.measure();
    auto schema = getSchema(measuredVal, "");
    writeToBuffer(measuredVal, tupleBuffer, 0);

    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    CpuMetrics deserVal = CpuMetrics::fromBuffer(schema, tupleBuffer, "");
    EXPECT_EQ(measuredVal, deserVal);
}

TEST_F(MonitoringSerializationTest, testSerializationMetricsNetworkValue) {
    auto networkStats = MetricUtils::NetworkStats();
    auto tupleBuffer = bufferManager->getBufferBlocking();

    NetworkValues measuredVal = networkStats.measure().getNetworkValue(0);
    writeToBuffer(measuredVal, tupleBuffer, 0);

    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, measuredVal.getSchema("")));
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    NetworkValues deserNw = NetworkValues::fromBuffer(measuredVal.getSchema(""), tupleBuffer, "");
    EXPECT_EQ(measuredVal, deserNw);
}

TEST_F(MonitoringSerializationTest, testSerializationMetricsNetworkMetrics) {
    auto networkStats = MetricUtils::NetworkStats();
    auto tupleBuffer = bufferManager->getBufferBlocking();

    NetworkMetrics measuredVal = networkStats.measure();
    auto schema = getSchema(measuredVal, "");
    writeToBuffer(measuredVal, tupleBuffer, 0);

    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    NetworkMetrics deserNw = NetworkMetrics::fromBuffer(schema, tupleBuffer, "");
    EXPECT_EQ(measuredVal, deserNw);
}

TEST_F(MonitoringSerializationTest, testSerializationGroups) {
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
    metricGroup->getSample(tupleBuffer);

    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, metricGroup->createSchema()));
}

TEST_F(MonitoringSerializationTest, testDeserializationMetricValues) {
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
    metricGroup->getSample(tupleBuffer);

    auto schema = metricGroup->createSchema();
    auto deserMem = MemoryMetrics::fromBuffer(schema, tupleBuffer, MonitoringPlan::MEMORY_METRICS_DESC);
    EXPECT_TRUE(deserMem != MemoryMetrics{});
    EXPECT_TRUE(deserMem.TOTAL_RAM == memStats.measure().TOTAL_RAM);

    auto deserCpu = CpuMetrics::fromBuffer(schema, tupleBuffer, MonitoringPlan::CPU_METRICS_DESC);
    EXPECT_TRUE(deserCpu.getNumCores() == cpuStats.measure().getNumCores());
    EXPECT_TRUE(deserCpu.getValues(1).user > 0);

    auto deserNw = NetworkMetrics::fromBuffer(schema, tupleBuffer, MonitoringPlan::NETWORK_METRICS_DESC);
    EXPECT_TRUE(deserNw.getInterfaceNum() == networkStats.measure().getInterfaceNum());

    auto deserDisk = DiskMetrics::fromBuffer(schema, tupleBuffer, MonitoringPlan::DISK_METRICS_DESC);
    EXPECT_TRUE(deserDisk.fBlocks == diskStats.measure().fBlocks);
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
}

TEST_F(MonitoringSerializationTest, testSerDeserMetricGroup) {
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    //worker side
    MetricGroupPtr metricGroup = plan->createMetricGroup(MetricCatalog::NesMetrics());
    auto tupleBuffer = bufferManager->getBufferBlocking();
    metricGroup->getSample(tupleBuffer);

    // coordinator side
    auto schema = metricGroup->createSchema();
    GroupedValues parsedValues = plan->fromBuffer(schema, tupleBuffer);

    EXPECT_TRUE(parsedValues.cpuMetrics.value()->getTotal().user > 0);
    EXPECT_TRUE(parsedValues.memoryMetrics.value()->FREE_RAM > 0);
    EXPECT_TRUE(parsedValues.diskMetrics.value()->fBavail > 0);
}

}// namespace NES