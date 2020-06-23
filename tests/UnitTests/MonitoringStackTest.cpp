#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Gauge.hpp>
#include <Monitoring/Metrics/NesMetrics.hpp>


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
    auto cpuStats = NesMetrics::CPUStats();
    std::unordered_map<std::string, uint64_t> metrics = cpuStats.readValue();
    ASSERT_TRUE(metrics["cpuCount"] > 0);
    ASSERT_TRUE(metrics.size()>=11);

    auto cpuIdle = NesMetrics::CPUIdle("cpu1");
    NES_INFO("MonitoringStackTest: Idle " << cpuIdle.readValue());
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memStats = NesMetrics::MemoryStats();
    std::unordered_map<std::string, uint64_t> metrics = memStats.readValue();
    ASSERT_TRUE(metrics.size()==13);

    NES_INFO("MonitoringStackTest: Total ram " << metrics["totalram"]/(1024*1024) << "gb");
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskStats = NesMetrics::DiskStats();
    std::unordered_map<std::string, uint64_t> metrics = diskStats.readValue();
    ASSERT_TRUE(metrics.size()==5);
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkStats = NesMetrics::NetworkStats();
    auto metrics = networkStats.readValue();
    ASSERT_TRUE(!metrics.empty());

    for (auto const& intfs : metrics) {
        NES_INFO("MonitoringStackTest: Received metrics for interface " << intfs.first);
        ASSERT_TRUE(intfs.second.size()==16);
    }
}

}