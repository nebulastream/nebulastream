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
    CpuMetrics cpuMetrics = cpuStats->readValue();
    ASSERT_TRUE(cpuMetrics.size() > 0);
    for (int i=0; i<cpuMetrics.size(); i++){
        ASSERT_TRUE(cpuMetrics.getValues(i).USER > 0);
    }
    ASSERT_TRUE(cpuMetrics.getTotal().USER > 0);

    auto cpuIdle = MetricUtils::CPUIdle(0);
    NES_INFO("MonitoringStackTest: Idle " << cpuIdle->readValue());
}

TEST_F(MonitoringStackTest, testMemoryStats) {
    auto memStats = MetricUtils::MemoryStats();
    auto memMetrics = memStats->readValue();
    ASSERT_TRUE(memMetrics.FREE_RAM > 0);

    NES_INFO("MonitoringStackTest: Total ram " << memMetrics.TOTAL_RAM/(1024*1024) << "gb");
}

TEST_F(MonitoringStackTest, testDiskStats) {
    auto diskStats = MetricUtils::DiskStats();
    auto diskMetrics = diskStats->readValue();
    ASSERT_TRUE(diskMetrics.F_BAVAIL > 0);
}

TEST_F(MonitoringStackTest, testNetworkStats) {
    auto networkStats = MetricUtils::NetworkStats();
    auto networkMetrics = networkStats->readValue();
    ASSERT_TRUE(!networkMetrics.getInterfaceNames().empty());

    for (std::string intfs : networkMetrics.getInterfaceNames()) {
        NES_INFO("MonitoringStackTest: Received metrics for interface " << intfs);
    }
}

TEST_F(MonitoringStackTest, testMetricCollector) {
    //TODO: Will be solved by issue #886
    auto cpuStats = MetricUtils::CPUStats();
    auto networkStats = MetricUtils::NetworkStats();
    auto diskStats = MetricUtils::DiskStats();
    auto memStats = MetricUtils::MemoryStats();

    auto systemMetrics = MetricGroup::create();

    //TODO: change that no shared_ptr are used here

    auto metrics = std::vector<Metric>();
    metrics.emplace_back(1);
    auto cpuValues = cpuStats->readValue();
    //metrics.emplace_back(cpuValues);

    Metric m = metrics[0];
    int value = m.getValue<int>();
    NES_INFO(value);

    //Metric cpu = metrics[2];
    //std::shared_ptr<Gauge<CpuMetrics>> v = cpu.getValue<std::shared_ptr<Gauge<CpuMetrics>>>();
    //NES_INFO(v->readValue().size());

    //metricMap.emplace_back(cpuStats);
    //systemMetrics.addGauge("network", &networkStats);
    //systemMetrics.addGauge("disk", &diskStats);
    //systemMetrics.addGauge("mem", &memStats);

    //static sampling func
    auto samplingProtocolPtr = std::make_shared<SamplingProtocol>([systemMetrics]() {
      sleep(1);
      return systemMetrics;
    });

    auto output = samplingProtocolPtr->getSample();
}

}