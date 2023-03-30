/*
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

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>

#include <Monitoring/MetricCollectors/NetworkCollector.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Storage/AllEntriesMetricStore.hpp>
#include <Monitoring/Storage/LatestEntriesMetricStore.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/TestHarness/TestHarness.hpp>

namespace NES {
using namespace Configurations;
using namespace Runtime;

class MetricStoreTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    uint64_t bufferSize = 0;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MetricStoreTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("ResourcesReaderTest: Setup MetricStoreTest test class.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_DEBUG("MetricStoreTest: Setup MetricStoreTest test case.");

        unsigned int numCPU = std::thread::hardware_concurrency();
        bufferSize = (numCPU + 1) * sizeof(Monitoring::CpuMetrics) + sizeof(Monitoring::CpuMetricsWrapper);
        bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, 10);
    }
};

TEST_F(MetricStoreTest, testNewestEntryMetricStore) {
    uint64_t nodeId = 0;
    auto metricStore = std::make_shared<Monitoring::LatestEntriesMetricStore>();
    auto networkCollector = Monitoring::NetworkCollector();

    uint64_t myInt = 12345;
    std::string myString = "testString";
    Monitoring::MetricPtr networkMetrics = networkCollector.readMetric();

    metricStore->addMetrics(nodeId, networkMetrics);
    metricStore->addMetrics(nodeId, std::make_shared<Monitoring::Metric>(myInt));
    metricStore->addMetrics(nodeId, std::make_shared<Monitoring::Metric>(myString));

    Monitoring::StoredNodeMetricsPtr storedMetrics = metricStore->getAllMetrics(nodeId);
    NES_INFO("MetricStoreTest: Stored metrics" << Monitoring::MetricUtils::toJson(storedMetrics));
    ASSERT_EQ(storedMetrics->size(), 2);
    ASSERT_EQ(storedMetrics->at(Monitoring::MetricType::UnknownMetric)->size(), 1);

    metricStore->removeMetrics(nodeId);
    ASSERT_FALSE(metricStore->hasMetrics(nodeId));
}

TEST_F(MetricStoreTest, testAllEntriesMetricStore) {
    uint64_t nodeId = 0;
    auto metricStore = std::make_shared<Monitoring::AllEntriesMetricStore>();
    auto networkCollector = Monitoring::NetworkCollector();

    uint64_t myInt = 12345;
    std::string myString = "testString";
    Monitoring::MetricPtr networkMetrics = networkCollector.readMetric();

    metricStore->addMetrics(nodeId, std::make_shared<Monitoring::Metric>(myInt));
    metricStore->addMetrics(nodeId, std::make_shared<Monitoring::Metric>(myString));
    metricStore->addMetrics(nodeId, networkMetrics);

    auto storedMetrics = metricStore->getAllMetrics(nodeId);
    NES_INFO("MetricStoreTest: Stored metrics" << Monitoring::MetricUtils::toJson(storedMetrics));
    ASSERT_EQ(storedMetrics->size(), 2);
    ASSERT_EQ(storedMetrics->at(Monitoring::MetricType::UnknownMetric)->size(), 2);

    metricStore->removeMetrics(nodeId);
    ASSERT_FALSE(metricStore->hasMetrics(nodeId));
}

TEST_F(MetricStoreTest, testNodeEngineStatisticsMetricStore) {
    //ContinousSourceTest.cpp
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    NES_INFO("MetricStoreTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("MetricStoreTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "exdra.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource1 = PhysicalSource::create("exdra", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString =
        R"(Query::from("exdra").sink(PrintSinkDescriptor::create()); )";
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    int buffersToASSERT = 1;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToASSERT, true));

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    auto stats = crd->getNodeEngine()->getQueryStatistics(queryId);

    auto metricStore = std::make_shared<Monitoring::AllEntriesMetricStore>();
    //ASSERT_EQ(metricStore->statistics, stats);

    // Collector für stats
}

}// namespace NES