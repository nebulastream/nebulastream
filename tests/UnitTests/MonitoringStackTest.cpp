#include <gtest/gtest.h>

#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Monitoring/Protocols/SamplingProtocol.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <Monitoring/Metrics/IntCounter.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>

#include <Util/UtilityFunctions.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/Metrics/MetricDefinition.hpp>
#include <memory>

namespace NES {
class MonitoringStackTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

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
    MetricDefinition def{};
    serialize(cpuStats.measure().getTotal(), schema, tupleBuffer, def, "TOTAL_");
    ASSERT_TRUE(def.cpuMetrics.find("TOTAL_") != def.cpuMetrics.end());
    ASSERT_TRUE(!def.cpuValues.empty());
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
}

TEST_F(MonitoringStackTest, testSerializationMetricsNested) {
    NES_INFO("Starting test");
    auto schema = Schema::create();
    auto tupleBuffer = bufferManager->getBufferBlocking();
    MetricDefinition def{};

    auto cpuStats = MetricUtils::CPUStats();
    serialize(cpuStats.measure(), schema, tupleBuffer, def, "");
    ASSERT_TRUE(def.cpuMetrics.find("TOTAL_") != def.cpuMetrics.end());
    ASSERT_TRUE(!def.cpuValues.empty());
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
}

TEST_F(MonitoringStackTest, testSerializationGroups) {
    MetricGroupPtr metricGroup = MetricGroup::create();

    Gauge<CpuMetrics> cpuStats = MetricUtils::CPUStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::NetworkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::DiskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::MemoryStats();

    // add with simple data types
    //auto intS = "simpleInt";
    //metricGroup->add(intS, 1);

    // add cpu stats
    //auto cpuS = "cpuStats";
    //metricGroup->add(cpuS, cpuStats);

    // add network stats
    auto networkS = "networkStats";
    metricGroup->add(networkS, networkStats);

    // add disk stats
    //auto diskS = "diskStats";
    //metricGroup->add(diskS, diskStats);

    // add mem stats
    //auto memS = "memStats";
    //metricGroup->add(memS, memStats);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto schema = Schema::create();
    auto def = metricGroup->getSample(schema, tupleBuffer);
    NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));

    //ASSERT_TRUE(!def.cpuMetrics.empty());
    //ASSERT_TRUE(!def.cpuValues.empty());
    ASSERT_TRUE(!def.networkMetrics.empty());
    ASSERT_TRUE(!def.networkValues.empty());
    //ASSERT_TRUE(!def.diskMetrics.empty());
    //ASSERT_TRUE(!def.memoryMetrics.empty());
    //ASSERT_TRUE(!def.simpleTypedMetrics.empty());

    auto deserNw = NetworkMetrics::fromBuffer(schema, tupleBuffer, "networkStats_");
    //ASSERT_TRUE(deserNw.getInterfaceNum() == networkStats.measure().getInterfaceNum());
}

TEST_F(MonitoringStackTest, testDeserializationMetricValues) {
    MetricGroupPtr metricGroup = MetricGroup::create();

    Gauge<CpuMetrics> cpuStats = MetricUtils::CPUStats();
    Gauge<NetworkMetrics> networkStats = MetricUtils::NetworkStats();
    Gauge<DiskMetrics> diskStats = MetricUtils::DiskStats();
    Gauge<MemoryMetrics> memStats = MetricUtils::MemoryStats();

    // add with simple data types
    auto intS = "simpleInt";
    metricGroup->add(intS, 1);

    // add cpu stats
    auto cpuS = "cpuStats";
    metricGroup->add(cpuS, cpuStats);

    // add network stats
    auto networkS = "networkStats";
    metricGroup->add(networkS, networkStats);

    // add disk stats
    auto diskS = "diskStats";
    metricGroup->add(diskS, diskStats);

    // add mem stats
    auto memS = "memStats";
    metricGroup->add(memS, memStats);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto schema = Schema::create();
    auto def = metricGroup->getSample(schema, tupleBuffer);

    auto deserMem = MemoryMetrics::fromBuffer(schema, tupleBuffer, "memStats_");
    ASSERT_TRUE(deserMem != MemoryMetrics{});
    ASSERT_TRUE(deserMem.TOTAL_RAM == memStats.measure().TOTAL_RAM);

    auto deserCpu = CpuMetrics::fromBuffer(schema, tupleBuffer, "cpuStats_");
    ASSERT_TRUE(deserCpu.getNumCores() == cpuStats.measure().getNumCores());

    auto deserNw = NetworkMetrics::fromBuffer(schema, tupleBuffer, "networkStats_");
    ASSERT_TRUE(deserCpu.getNumCores() == cpuStats.measure().getNumCores());
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

}