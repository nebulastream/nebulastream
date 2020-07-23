#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Gauge.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <memory>
#include <Monitoring/Protocols/SamplingProtocol.hpp>
#include <Monitoring/MetricValues/CPU.hpp>
#include <Monitoring/MetricValues/CpuStats.hpp>

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
    CPU metrics = cpuStats.readValue();
    ASSERT_TRUE(metrics.size() > 0);
    ASSERT_TRUE(metrics[0].USER > 0);

    auto cpuIdle = MetricUtils::CPUIdle(0);
    NES_INFO("MonitoringStackTest: Idle " << cpuIdle.readValue());
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memStats = MetricUtils::MemoryStats();
    std::unordered_map<std::string, uint64_t> metrics = memStats.readValue();
    ASSERT_TRUE(metrics.size() == 13);

    NES_INFO("MonitoringStackTest: Total ram " << metrics["totalram"]/(1024*1024) << "gb");
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskStats = MetricUtils::DiskStats();
    std::unordered_map<std::string, uint64_t> metrics = diskStats.readValue();
    ASSERT_TRUE(metrics.size() == 5);
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkStats = MetricUtils::NetworkStats();
    auto metrics = networkStats.readValue();
    ASSERT_TRUE(!metrics.empty());

    for (auto const& intfs : metrics) {
        NES_INFO("MonitoringStackTest: Received metrics for interface " << intfs.first);
        ASSERT_TRUE(intfs.second.size() == 16);
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