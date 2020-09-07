#include <gtest/gtest.h>

#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Monitoring/Protocols/SamplingProtocol.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/IntCounter.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>

#include <Util/UtilityFunctions.hpp>
#include <Util/Logger.hpp>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Monitoring/MetricValues/GroupedValues.hpp>
#include <memory>

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 5000;

class MonitoringStackTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;
    std::string ipAddress = "127.0.0.1";
    uint64_t restPort = 8081;

    static void SetUpTestCase() {
        NES::setupLogging("MonitoringStackTest.log", NES::LOG_DEBUG);
        NES_INFO("MonitoringStackTest: Setup MonitoringStackTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "MonitoringStackTest: Tear down MonitoringStackTest class." << std::endl;
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        std::cout << "MonitoringStackTest: Setup MonitoringStackTest test case." << std::endl;
        bufferManager = std::make_shared<BufferManager>(4096, 10);
        rpcPort = rpcPort + 30;
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl;
    }
};

TEST_F(MonitoringStackTest, testCPUStats) {
    auto cpuStats = MetricUtils::CPUStats();
    CpuMetrics cpuMetrics = cpuStats.measure();
    ASSERT_TRUE(cpuMetrics.getNumCores() > 0);
    for (int i=0; i< cpuMetrics.getNumCores(); i++){
        ASSERT_TRUE(cpuMetrics.getValues(i).USER > 0);
    }
    ASSERT_TRUE(cpuMetrics.getTotal().USER > 0);

    auto cpuIdle = MetricUtils::CPUIdle(0);
    NES_INFO("MonitoringStackTest: Idle " << cpuIdle.measure());
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memStats = MetricUtils::MemoryStats();
    auto memMetrics = memStats.measure();
    ASSERT_TRUE(memMetrics.FREE_RAM > 0);

    NES_INFO("MonitoringStackTest: Total ram " << memMetrics.TOTAL_RAM/(1024*1024) << "gb");
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskStats = MetricUtils::DiskStats();
    auto diskMetrics = diskStats.measure();
    ASSERT_TRUE(diskMetrics.fBavail > 0);
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkStats = MetricUtils::NetworkStats();
    auto networkMetrics = networkStats.measure();
    ASSERT_TRUE(!networkMetrics.getInterfaceNames().empty());

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
    ASSERT_TRUE(getMetricType(m0)==MetricType::UnknownType);
    int valueInt = m0.getValue<int>();
    ASSERT_TRUE(valueInt == 1);
    metricsMap.insert({"sdf", 1});

    metrics.emplace_back(std::string("test"));
    Metric m1 = metrics[1];
    std::string valueString = m1.getValue<std::string>();
    ASSERT_TRUE(valueString == "test");

    // test cpu stats
    metrics.emplace_back(cpuStats);
    Metric m2 = metrics[2];
    ASSERT_TRUE(getMetricType(m2) == MetricType::GaugeType);
    Gauge<CpuMetrics> cpuMetrics = m2.getValue<Gauge<CpuMetrics>>();
    ASSERT_TRUE(cpuStats.measure().getNumCores() == cpuMetrics.measure().getNumCores());

    // test network stats
    metrics.emplace_back(networkStats);
    auto networkMetrics = metrics[3].getValue<Gauge<NetworkMetrics>>();
    ASSERT_TRUE(networkStats.measure().getInterfaceNum() == networkMetrics.measure().getInterfaceNum());

    // test disk stats
    metrics.emplace_back(diskStats);
    auto diskMetrics = metrics[4].getValue<Gauge<DiskMetrics>>();
    ASSERT_TRUE(diskStats.measure().fBavail == diskMetrics.measure().fBavail);

    // test mem stats
    metrics.emplace_back(memStats);
    auto memMetrics = metrics[5].getValue<Gauge<MemoryMetrics>>();
    ASSERT_TRUE(memStats.measure().TOTAL_RAM == memMetrics.measure().TOTAL_RAM);
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
    ASSERT_TRUE(valueInt == 1);

    auto stringS = "simpleString";
    metricGroup->add(stringS, std::string("test"));
    std::string valueString = metricGroup->getAs<std::string>(stringS);
    ASSERT_TRUE(valueString == "test");

    // test cpu stats
    auto cpuS = "cpuStats";
    metricGroup->add(cpuS, cpuStats);
    Gauge<CpuMetrics> cpuMetrics = metricGroup->getAs<Gauge<CpuMetrics>>(cpuS);
    ASSERT_TRUE(cpuStats.measure().getNumCores() == cpuMetrics.measure().getNumCores());

    // test network stats
    auto networkS = "networkStats";
    metricGroup->add(networkS, networkStats);
    auto networkMetrics = metricGroup->getAs<Gauge<NetworkMetrics>>(networkS);
    ASSERT_TRUE(networkStats.measure().getInterfaceNum() == networkMetrics.measure().getInterfaceNum());

    // test disk stats
    auto diskS = "diskStats";
    metricGroup->add(diskS, diskStats);
    auto diskMetrics = metricGroup->getAs<Gauge<DiskMetrics>>(diskS);
    ASSERT_TRUE(diskStats.measure().fBavail == diskMetrics.measure().fBavail);

    // test mem stats
    auto memS = "memStats";
    metricGroup->add(memS, memStats);
    auto memMetrics = metricGroup->getAs<Gauge<MemoryMetrics>>(memS);
    ASSERT_TRUE(memStats.measure().TOTAL_RAM == memMetrics.measure().TOTAL_RAM);
}

TEST_F(MonitoringStackTest, testSerializationMetricsSingle) {
    auto cpuStats = MetricUtils::CPUStats();

    //test serialize method to append to existing schema and buffer
    auto schema = Schema::create();
    auto tupleBuffer = bufferManager->getBufferBlocking();
    serialize(cpuStats.measure().getTotal(), schema, tupleBuffer, "TOTAL_");
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
}

TEST_F(MonitoringStackTest, testSerializationMetricsNested) {
    NES_INFO("Starting test");
    auto schema = Schema::create();
    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto cpuStats = MetricUtils::CPUStats();
    serialize(cpuStats.measure(), schema, tupleBuffer, "");
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
}

TEST_F(MonitoringStackTest, testSerializationGroups) {
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
    metricGroup->getSample(schema, tupleBuffer);
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
}

TEST_F(MonitoringStackTest, testDeserializationMetricValues) {
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

    metricGroup->getSample(schema, tupleBuffer);

    auto deserMem = MemoryMetrics::fromBuffer(schema, tupleBuffer, MonitoringPlan::MEMORY_METRICS_DESC);
    ASSERT_TRUE(deserMem != MemoryMetrics{});
    ASSERT_TRUE(deserMem.TOTAL_RAM == memStats.measure().TOTAL_RAM);

    auto deserCpu = CpuMetrics::fromBuffer(schema, tupleBuffer, MonitoringPlan::CPU_METRICS_DESC);
    ASSERT_TRUE(deserCpu.getNumCores() == cpuStats.measure().getNumCores());
    ASSERT_TRUE(deserCpu.getValues(1).USER > 0);

    auto deserNw = NetworkMetrics::fromBuffer(schema, tupleBuffer, MonitoringPlan::NETWORK_METRICS_DESC);
    ASSERT_TRUE(deserNw.getInterfaceNum() == networkStats.measure().getInterfaceNum());

    auto deserDisk = DiskMetrics::fromBuffer(schema, tupleBuffer, MonitoringPlan::DISK_METRICS_DESC);
    ASSERT_TRUE(deserDisk.fBlocks == diskStats.measure().fBlocks);
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
}

TEST_F(MonitoringStackTest, testDeserializationMetricGroup) {
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    MonitoringPlan plan = MonitoringPlan(metrics);

    //worker side
    MetricGroupPtr metricGroup = plan.createMetricGroup(MetricCatalog::NesMetrics());
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto schema = Schema::create();
    metricGroup->getSample(schema, tupleBuffer);

    // coordinator side
    GroupedValues parsedValues = plan.fromBuffer(schema, tupleBuffer);

    ASSERT_TRUE(parsedValues.cpuMetrics.value()->getTotal().USER > 0);
    ASSERT_TRUE(parsedValues.memoryMetrics.value()->FREE_RAM > 0);
    ASSERT_TRUE(parsedValues.diskMetrics.value()->fBavail > 0);
}

TEST_F(MonitoringStackTest, testSamplingProtocol) {
    auto cpuStats = MetricUtils::CPUStats();
    int systemMetrics = 22;

    //static sampling func
    auto samplingProtocolPtr = std::make_shared<SamplingProtocol>([systemMetrics]() {
      sleep(1);
      return MetricGroup::create();
    });

    auto output = samplingProtocolPtr->getSample();
}

TEST_F(MonitoringStackTest, requestMonitoringData) {
    NES_INFO("MonitoringStackTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0);
    NES_INFO("MonitoringStackTest: Coordinator started successfully");

    NES_INFO("MonitoringStackTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1",
                                                    port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(false,false);
    EXPECT_TRUE(retStart1);
    NES_INFO("MonitoringStackTest: Worker1 started successfully");

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    NES_INFO("MonitoringStackTest: Worker 1 connected ");

    // requesting the monitoring data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan(metrics);

    auto schema = crd->requestMonitoringData("127.0.0.1", port + 10, plan, tupleBuffer);
    NES_INFO("MonitoringStackTest: Coordinator requested monitoring data from worker 127.0.0.1:" + std::to_string(port+10));
    ASSERT_TRUE(schema->getSize()>1);
    ASSERT_TRUE(tupleBuffer.getNumberOfTuples()==1);
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));

    NES_INFO("MonitoringStackTest: Stopping worker");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MonitoringStackTest: stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}