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
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Runtime/BufferManager.hpp>

#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Monitoring/MetricStore.hpp>
#include <Monitoring/MetricValues/GroupedMetricValues.hpp>
#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <memory>

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 5000;

class MonitoringSerializationTest : public testing::Test {
  public:
    Runtime::BufferManagerPtr bufferManager;
    std::string ipAddress = "127.0.0.1";
    uint64_t restPort = 8081;
    CoordinatorConfigPtr crdConf;
    WorkerConfigPtr wrkConf;
    uint64_t bufferSize = 0;

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

        unsigned int numCPU = std::thread::hardware_concurrency();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        bufferSize = 4096 + (numCPU + 1) * sizeof(CpuValues) + sizeof(CpuMetrics);

        rpcPort = rpcPort + 30;
        crdConf->setRpcPort(rpcPort);
        wrkConf->setCoordinatorPort(rpcPort);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl; }
};

TEST_F(MonitoringSerializationTest, testSerializationRuntimeNesMetrics) {
    auto rutimeStats = MetricUtils::runtimeNesStats();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();

    RuntimeNesMetrics measuredVal = rutimeStats.measure();
    auto schema = getSchema(measuredVal, "");
    writeToBuffer(measuredVal, tupleBuffer, 0);

    NES_DEBUG(Util::prettyPrintTupleBuffer(tupleBuffer, schema));
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    RuntimeNesMetrics deserVal = RuntimeNesMetrics::fromBuffer(schema, tupleBuffer, "");

    EXPECT_EQ(measuredVal, deserVal);
}

TEST_F(MonitoringSerializationTest, testSerializationStaticNesMetrics) {
    auto staticStats = MetricUtils::staticNesStats();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();

    StaticNesMetrics measuredVal = staticStats.measure();
    auto schema = getSchema(measuredVal, "");
    writeToBuffer(measuredVal, tupleBuffer, 0);

    NES_DEBUG(measuredVal.toJson());
    NES_DEBUG(Util::prettyPrintTupleBuffer(tupleBuffer, schema));
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    StaticNesMetrics deserVal = StaticNesMetrics::fromBuffer(schema, tupleBuffer, "");

    EXPECT_EQ(measuredVal, deserVal);
}

TEST_F(MonitoringSerializationTest, testSerializationMetricsCpu) {
    auto cpuStats = MetricUtils::cpuStats();
    CpuMetrics measuredVal = cpuStats.measure();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();

    auto schema = getSchema(measuredVal, "");
    writeToBuffer(measuredVal, tupleBuffer, 0);

    NES_DEBUG(Util::prettyPrintTupleBuffer(tupleBuffer, schema));
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    CpuMetrics deserVal = CpuMetrics::fromBuffer(schema, tupleBuffer, "");
    EXPECT_EQ(measuredVal, deserVal);
}

TEST_F(MonitoringSerializationTest, testSerializationMetricsNetworkValue) {
    auto networkStats = MetricUtils::networkStats();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();

    NetworkValues measuredVal = networkStats.measure().getNetworkValue(0);
    writeToBuffer(measuredVal, tupleBuffer, 0);

    NES_DEBUG(Util::prettyPrintTupleBuffer(tupleBuffer, measuredVal.getSchema("")));
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    NetworkValues deserNw = NetworkValues::fromBuffer(NES::NetworkValues::getSchema(""), tupleBuffer, "");
    EXPECT_EQ(measuredVal, deserNw);
}

TEST_F(MonitoringSerializationTest, testSerializationMetricsNetworkMetrics) {
    auto networkStats = MetricUtils::networkStats();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();

    NetworkMetrics measuredVal = networkStats.measure();
    auto schema = getSchema(measuredVal, "");
    writeToBuffer(measuredVal, tupleBuffer, 0);

    NES_DEBUG(Util::prettyPrintTupleBuffer(tupleBuffer, schema));
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    NetworkMetrics deserNw = NetworkMetrics::fromBuffer(schema, tupleBuffer, "");
    EXPECT_EQ(measuredVal, deserNw);
}

TEST_F(MonitoringSerializationTest, testSerializationGroups) {
    MetricGroupPtr metricGroup = MetricGroup::create();

    Gauge<CpuMetrics> cpuStats = MetricUtils::cpuStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::networkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::diskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::memoryStats();

    // add with simple data types
    metricGroup->add("simpleInt_", Metric{1});

    // add cpu stats
    metricGroup->add(MonitoringPlan::CPU_METRICS_DESC, Metric{cpuStats});

    // add network stats
    metricGroup->add(MonitoringPlan::NETWORK_METRICS_DESC, Metric{networkStats});

    // add disk stats
    metricGroup->add(MonitoringPlan::DISK_METRICS_DESC, Metric{diskStats});

    // add mem stats
    metricGroup->add(MonitoringPlan::MEMORY_METRICS_DESC, Metric{memStats});

    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    metricGroup->getSample(tupleBuffer);

    NES_DEBUG(Util::prettyPrintTupleBuffer(tupleBuffer, metricGroup->createSchema()));
}

TEST_F(MonitoringSerializationTest, testDeserializationMetricValues) {
    MetricGroupPtr metricGroup = MetricGroup::create();

    Gauge<CpuMetrics> cpuStats = MetricUtils::cpuStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::networkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::diskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::memoryStats();

    // add with simple data types
    metricGroup->add("simpleInt_", Metric{1});

    // add cpu stats
    metricGroup->add(MonitoringPlan::CPU_METRICS_DESC, Metric{cpuStats});

    // add network stats
    metricGroup->add(MonitoringPlan::NETWORK_METRICS_DESC, Metric{networkStats});

    // add disk stats
    metricGroup->add(MonitoringPlan::DISK_METRICS_DESC, Metric{diskStats});

    // add mem stats
    metricGroup->add(MonitoringPlan::MEMORY_METRICS_DESC, Metric{memStats});

    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
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
    NES_DEBUG(Util::prettyPrintTupleBuffer(tupleBuffer, schema));
}

TEST_F(MonitoringSerializationTest, testSerDeserMetricGroup) {
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    //worker side
    MetricGroupPtr metricGroup = plan->createMetricGroup(MetricCatalog::NesMetrics());
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    metricGroup->getSample(tupleBuffer);

    // coordinator side
    auto schema = metricGroup->createSchema();
    GroupedMetricValues parsedValues = plan->fromBuffer(schema, tupleBuffer);

    EXPECT_TRUE(parsedValues.cpuMetrics.value()->getTotal().user > 0);
    EXPECT_TRUE(parsedValues.memoryMetrics.value()->FREE_RAM > 0);
    EXPECT_TRUE(parsedValues.diskMetrics.value()->fBavail > 0);
}

TEST_F(MonitoringSerializationTest, testMetricStore) {
    auto metricStoreAlways = std::make_shared<MetricStore>(MetricStoreStrategy::ALWAYS);
    auto metricStoreNewest = std::make_shared<MetricStore>(MetricStoreStrategy::NEWEST);

    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    //worker side
    MetricGroupPtr metricGroup = plan->createMetricGroup(MetricCatalog::NesMetrics());
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    metricGroup->getSample(tupleBuffer);

    // coordinator side
    auto schema = metricGroup->createSchema();
    GroupedMetricValuesPtr parsedValues = std::make_shared<GroupedMetricValues>(plan->fromBuffer(schema, tupleBuffer));

    const uint64_t noMetrics = 10;
    for (uint64_t i=0; i<noMetrics; i++) {
        metricStoreAlways->addMetric(i, parsedValues);
        metricStoreAlways->addMetric(i, parsedValues);

        metricStoreNewest->addMetric(i, parsedValues);
        metricStoreNewest->addMetric(i, parsedValues);
    }

    for (uint64_t i=0; i<noMetrics; i++) {
        EXPECT_EQ(int(metricStoreAlways->getMetrics(i).size()), 2);
        EXPECT_EQ(int(metricStoreNewest->getMetrics(i).size()), 1);
    }
}

}// namespace NES