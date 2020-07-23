#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Gauge.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <memory>
#include <Monitoring/Protocols/SamplingProtocol.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/CpuValues.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/NetworkValues.hpp>


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
    CpuMetrics cpuMetrics = cpuStats.readValue();
    ASSERT_TRUE(cpuMetrics.size() > 0);
    for (int i=0; i<cpuMetrics.size(); i++){
        ASSERT_TRUE(cpuMetrics[i].USER > 0);
    }

    auto cpuIdle = MetricUtils::CPUIdle(0);
    NES_INFO("MonitoringStackTest: Idle " << cpuIdle.readValue());
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memStats = MetricUtils::MemoryStats();
    auto memMetrics = memStats.readValue();
    ASSERT_TRUE(memMetrics.FREE_RAM > 0);

    NES_INFO("MonitoringStackTest: Total ram " << memMetrics.TOTAL_RAM/(1024*1024) << "gb");
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskStats = MetricUtils::DiskStats();
    auto diskMetrics = diskStats.readValue();
    ASSERT_TRUE(diskMetrics.F_BAVAIL > 0);
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkStats = MetricUtils::NetworkStats();
    auto networkMetrics = networkStats.readValue();
    ASSERT_TRUE(!networkMetrics.getInterfaceNames().empty());

    for (std::string intfs : networkMetrics.getInterfaceNames()) {
        NES_INFO("MonitoringStackTest: Received metrics for interface " << intfs);
    }
}

TEST_F(MonitoringStackTest, testMetricCollector) {
    auto cpuStats = MetricUtils::CPUStats();
    auto networkStats = MetricUtils::NetworkStats();
    auto diskStats = MetricUtils::DiskStats();
    auto memStats = MetricUtils::MemoryStats();

    auto systemMetrics = MetricGroup();
    //systemMetrics.add("cpu", cpuStats);
    systemMetrics.add("network", &networkStats);
    systemMetrics.add("disk", &diskStats);
    systemMetrics.add("mem", &memStats);

    //static sampling func
    auto samplingProtocolPtr = std::make_shared<SamplingProtocol>([&systemMetrics]() {
      sleep(5);
      return systemMetrics;
    });

    auto output = samplingProtocolPtr->getSample();
}

}