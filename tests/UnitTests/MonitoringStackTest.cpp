#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <memory>
#include <Monitoring/Protocols/SamplingProtocol.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <Monitoring/Metrics/IntCounter.hpp>

namespace NES {
class MonitoringStackTest : public testing::Test {
  public:

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
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl;
    }
};

TEST_F(MonitoringStackTest, testCPUStats) {
    auto cpuStats = MetricUtils::CPUStats();
    CpuMetrics cpuMetrics = cpuStats.getValue();
    ASSERT_TRUE(cpuMetrics.size() > 0);
    for (int i=0; i<cpuMetrics.size(); i++){
        ASSERT_TRUE(cpuMetrics.getValues(i).USER > 0);
    }
    ASSERT_TRUE(cpuMetrics.getTotal().USER > 0);

    auto cpuIdle = MetricUtils::CPUIdle(0);
    NES_INFO("MonitoringStackTest: Idle " << cpuIdle.getValue());
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memStats = MetricUtils::MemoryStats();
    auto memMetrics = memStats.getValue();
    ASSERT_TRUE(memMetrics.FREE_RAM > 0);

    NES_INFO("MonitoringStackTest: Total ram " << memMetrics.TOTAL_RAM/(1024*1024) << "gb");
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskStats = MetricUtils::DiskStats();
    auto diskMetrics = diskStats.getValue();
    ASSERT_TRUE(diskMetrics.F_BAVAIL > 0);
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkStats = MetricUtils::NetworkStats();
    auto networkMetrics = networkStats.getValue();
    ASSERT_TRUE(!networkMetrics.getInterfaceNames().empty());

    for (std::string intfs : networkMetrics.getInterfaceNames()) {
        NES_INFO("MonitoringStackTest: Received metrics for interface " << intfs);
    }
}

TEST_F(MonitoringStackTest, testMetricGroup) {
    auto cpuStats = MetricUtils::CPUStats();
    auto networkStats = MetricUtils::NetworkStats();
    auto diskStats = MetricUtils::DiskStats();
    auto memStats = MetricUtils::MemoryStats();

    auto metrics = std::vector<Metric>();

    // test with simple data types
    metrics.emplace_back(1);
    Metric m0 = metrics[0];
    int valueInt = m0.getValue<int>();
    ASSERT_TRUE(valueInt == 1);

    metrics.emplace_back(std::string("test"));
    Metric m1 = metrics[1];
    std::string valueString = m1.getValue<std::string>();
    ASSERT_TRUE(valueString == "test");

    // test cpu stats
    metrics.emplace_back(cpuStats, MetricType::GaugeType);
    Metric m2 = metrics[2];
    auto cpuMetrics = m2.getValue<Gauge<CpuMetrics>>();
    ASSERT_TRUE(cpuStats.getValue().size() == cpuMetrics.getValue().size());
    //ASSERT_TRUE(m2.getType() == MetricType::GaugeType);

    /**
    // test network stats
    metrics.emplace_back(networkStats);
    auto networkMetrics = metrics[3].getValue<Gauge<NetworkMetrics>>();
    ASSERT_TRUE(networkStats.readValue().size() == networkMetrics.readValue().size());

    // test disk stats
    metrics.emplace_back(diskStats);
    auto diskMetrics = metrics[4].getValue<Gauge<DiskMetrics>>();
    ASSERT_TRUE(diskStats.readValue().F_BAVAIL == diskMetrics.readValue().F_BAVAIL);

    // test mem stats
    metrics.emplace_back(memStats);
    auto memMetrics = metrics[5].getValue<Gauge<MemoryMetrics>>();
    ASSERT_TRUE(memStats.readValue().TOTAL_RAM == memMetrics.readValue().TOTAL_RAM);
     **/
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