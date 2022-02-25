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
#include <NesBaseTest.hpp>
#include <gtest/gtest.h>

#include <Monitoring/MonitoringCatalog.hpp>
#include <Monitoring/MonitoringPlan.hpp>

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/MetricCollectors/MemoryCollector.hpp>
#include <Monitoring/MetricCollectors/NetworkCollector.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Monitoring/Storage/MetricStore.hpp>

namespace NES {

using namespace Configurations;
using namespace Runtime;

class MonitoringSerializationTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    uint64_t bufferSize = 0;

    static void SetUpTestCase() {
        NES::setupLogging("MonitoringStackTest.log", NES::LOG_DEBUG);
        NES_INFO("MonitoringStackTest: Setup MonitoringStackTest test class.");
    }

    static void TearDownTestCase() { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest class." << std::endl; }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        std::cout << "MonitoringStackTest: Setup MonitoringStackTest test case." << std::endl;

        unsigned int numCPU = std::thread::hardware_concurrency();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        bufferSize = 4096 + (numCPU + 1) * sizeof(CpuMetrics) + sizeof(CpuMetricsWrapper);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl; }
};

TEST_F(MonitoringSerializationTest, testNetworkCollectorWrappedMetrics) {
    auto networkCollector = NetworkCollector();
    Metric networkMetric = networkCollector.readMetric();
    EXPECT_EQ(networkMetric.getMetricType(), MetricType::WrappedNetworkMetrics);

    NetworkMetricsWrapper wrappedMetric = networkMetric.getValue<NetworkMetricsWrapper>();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    wrappedMetric.writeToBuffer(tupleBuffer, 0);
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());

    NetworkMetricsWrapper parsedMetric{};
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    EXPECT_EQ(wrappedMetric, parsedMetric);
}

TEST_F(MonitoringSerializationTest, testNetworkCollectorSingleMetrics) {
    auto networkCollector = NetworkCollector();
    Metric networkMetric = networkCollector.readMetric();
    EXPECT_EQ(networkMetric.getMetricType(), MetricType::WrappedNetworkMetrics);

    NetworkMetricsWrapper wrappedMetric = networkMetric.getValue<NetworkMetricsWrapper>();
    NetworkMetrics totalMetrics = wrappedMetric.getNetworkValue(0);

    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    totalMetrics.writeToBuffer(tupleBuffer, 0);
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    NetworkMetrics parsedMetric{};
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    EXPECT_EQ(totalMetrics, parsedMetric);
}

TEST_F(MonitoringSerializationTest, testCpuCollectorWrappedMetrics) {
    auto cpuCollector = CpuCollector();
    Metric cpuMetric = cpuCollector.readMetric();
    EXPECT_EQ(cpuMetric.getMetricType(), MetricType::WrappedCpuMetrics);

    CpuMetricsWrapper wrappedMetric = cpuMetric.getValue<CpuMetricsWrapper>();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    wrappedMetric.writeToBuffer(tupleBuffer, 0);
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());

    CpuMetricsWrapper parsedMetric{};
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    EXPECT_EQ(wrappedMetric, parsedMetric);
}

TEST_F(MonitoringSerializationTest, testCpuCollectorSingleMetrics) {
    auto cpuCollector = CpuCollector();
    Metric cpuMetric = cpuCollector.readMetric();
    EXPECT_EQ(cpuMetric.getMetricType(), MetricType::WrappedCpuMetrics);

    CpuMetricsWrapper wrappedMetric = cpuMetric.getValue<CpuMetricsWrapper>();
    CpuMetrics totalMetrics = wrappedMetric.getValue(0);

    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    totalMetrics.writeToBuffer(tupleBuffer, 0);
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    CpuMetrics parsedMetric;
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    EXPECT_EQ(totalMetrics, parsedMetric);
}

TEST_F(MonitoringSerializationTest, testDiskCollector) {
    auto diskCollector = DiskCollector();
    Metric diskMetric = diskCollector.readMetric();
    DiskMetrics typedMetric = diskMetric.getValue<DiskMetrics>();
    EXPECT_EQ(diskMetric.getMetricType(), MetricType::DiskMetric);

    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    auto success = diskCollector.fillBuffer(tupleBuffer);

    EXPECT_TRUE(success);
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    DiskMetrics parsedMetric{};
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    EXPECT_EQ(typedMetric, parsedMetric);
}

TEST_F(MonitoringSerializationTest, testMemoryCollector) {
    auto memoryCollector = MemoryCollector();
    Metric memoryMetric = memoryCollector.readMetric();
    MemoryMetrics typedMetric = memoryMetric.getValue<MemoryMetrics>();
    EXPECT_EQ(memoryMetric.getMetricType(), MetricType::MemoryMetric);

    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    auto success = memoryCollector.fillBuffer(tupleBuffer);

    EXPECT_TRUE(success);
    EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    MemoryMetrics parsedMetric{};
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    EXPECT_EQ(typedMetric, parsedMetric);
}

TEST_F(MonitoringSerializationTest, testRuntimeConcepts) {
    web::json::value metricsJson{};
    std::vector<Metric> metrics;

    uint64_t myInt = 12345;
    metrics.emplace_back(myInt);
    std::string myString = "testString";
    metrics.emplace_back(myString);

    for (unsigned int i = 0; i < metrics.size(); i++) {
        metricsJson[i] = asJson(metrics[i]);
    }

    NES_DEBUG("MonitoringSerializationTest: Json Concepts: " << metricsJson);
}

TEST_F(MonitoringSerializationTest, testJsonRuntimeConcepts) {
    auto monitoringPlan = MonitoringPlan::createDefaultPlan();
    auto monitoringCatalog = MonitoringCatalog::defaultCatalog();
    web::json::value metricsJson{};

    for (auto type : monitoringPlan->getMetricTypes()) {
        auto collector = monitoringCatalog->getMetricCollector(type);
        Metric metric = collector->readMetric();
        metricsJson[toString(metric.getMetricType())] = asJson(metric);
    }
    NES_DEBUG("MonitoringSerializationTest: Json Concepts: " << metricsJson);
}

TEST_F(MonitoringSerializationTest, testMetricStore) {
    uint64_t nodeId = 0;
    auto metricStore = std::make_shared<MetricStore>(MetricStoreStrategy::NEWEST);
    auto networkCollector = NetworkCollector();

    uint64_t myInt = 12345;
    std::string myString = "testString";
    MetricPtr networkMetrics = std::make_shared<Metric>(networkCollector.readMetric());

    metricStore->addMetrics(nodeId, std::make_shared<Metric>(myInt));
    metricStore->addMetrics(nodeId, std::make_shared<Metric>(myString));
    metricStore->addMetrics(nodeId, networkMetrics);

    auto storedMetrics = metricStore->getNewestMetrics(nodeId);
    ASSERT_EQ(storedMetrics.size(), 2);

    metricStore->removeMetrics(nodeId);
    ASSERT_FALSE(metricStore->hasMetrics(nodeId));
}

}// namespace NES
