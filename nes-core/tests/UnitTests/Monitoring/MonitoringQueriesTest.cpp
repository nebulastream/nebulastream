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

#include <BaseIntegrationTest.hpp>
#include <gtest/gtest.h>

#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Util/MetricValidator.hpp>

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
//#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Storage/AbstractMetricStore.hpp>
#include <Util/MetricCollectorType.hpp>

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Runtime/BufferManager.hpp>

#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MonitoringSourceType.hpp>
#include <Services/MonitoringService.hpp>
#include <Services/QueryService.hpp>
#include <cstdint>
#include <memory>
#include <regex>

using std::cout;
using std::endl;
namespace NES {

uint16_t timeout = 15;

class MonitoringQueriesTest : public Testing::BaseIntegrationTest {
  public:
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MonitoringQueriesTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MonitoringQueriesTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        std::stringstream rpcCoordinatorPortAsString;
        rpcCoordinatorPortAsString << rpcCoordinatorPort;
        std::stringstream restPortAsString;
        rpcCoordinatorPortAsString << restPort;
        NES_INFO("MonitoringQueriesTest: Setting up test with rpc port {}, rest port {}",
                 rpcCoordinatorPortAsString.str(),
                 restPortAsString.str());
    }

    NesCoordinatorPtr createCoordinator() {
        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->enableMonitoring = true;
        coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

        return std::make_shared<NesCoordinator>(coordinatorConfig);
    }

    NesWorkerPtr createWorker(PhysicalSourceTypePtr phSource) {
        WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
        workerConfig->coordinatorPort = *rpcCoordinatorPort;
        workerConfig->numberOfSlots = (12);
        workerConfig->enableMonitoring = (true);
        workerConfig->physicalSourceTypes.add(phSource);
        workerConfig->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
        return std::make_shared<NesWorker>(std::move(workerConfig));
    }

    std::string createQueryString(std::string logicalStream, std::string metricCollectorString) {
        std::string query =
            R"(Query::from("%LOGS%").sink(MonitoringSinkDescriptor::create(Monitoring::MetricCollectorType::%COLLECTOR%));)";
        query = std::regex_replace(query, std::regex("%LOGS%"), logicalStream);
        query = std::regex_replace(query, std::regex("%COLLECTOR%"), metricCollectorString);
        return query;
    }

    void runMetricsQueryTest(uint64_t workerCnt,
                             Monitoring::MetricCollectorType collectorType,
                             SchemaPtr schema,
                             Monitoring::MetricType expectedType) {
        std::vector<NesWorkerPtr> workers;
        Monitoring::MetricType retMetricType =
            Monitoring::MetricUtils::createMetricFromCollectorType(collectorType)->getMetricType();
        ASSERT_EQ(retMetricType, expectedType);
        MonitoringSourceTypePtr sourceType =
            MonitoringSourceType::create("logTestMetricStream", "physMetricSource", collectorType);
        std::string metricCollectorStr = std::string(magic_enum::enum_name(collectorType));

        NesCoordinatorPtr crd = createCoordinator();
        NES_INFO("MonitoringQueriesTest: Start coordinator");
        uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
        EXPECT_NE(port, 0UL);
        //register logical schema
        crd->getSourceCatalogService()->registerLogicalSource("logTestMetricStream", schema);
        NES_DEBUG("MonitoringQueriesTest: Coordinator started successfully");

        for (uint64_t i = 0; i < workerCnt; i++) {
            NES_DEBUG("MonitoringQueriesTest: Start worker 1");
            NesWorkerPtr wrk = createWorker(sourceType);
            bool retStart1 = wrk->start(/**blocking**/ false, /**withConnect**/ true);
            EXPECT_TRUE(retStart1);
            NES_INFO("MonitoringQueriesTest: Worker1 started successfully");
            workers.emplace_back(wrk);
        }

        QueryServicePtr queryService = crd->getQueryService();
        QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService(); /*register logical schema qnv*/

        NES_INFO("MonitoringQueriesTest: Submit query");
        auto query = createQueryString("logTestMetricStream", metricCollectorStr);
        QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                        Optimizer::PlacementStrategy::BottomUp,
                                                                        FaultToleranceType::NONE,
                                                                        LineageType::IN_MEMORY);

        GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
        EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
        for (auto wrk : workers) {
            EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk, queryId, globalQueryPlan, 2));
        }
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

        NES_DEBUG("MonitoringQueriesTest: Remove query");
        ASSERT_TRUE(queryService->validateAndQueueStopQueryRequest(queryId));
        NES_DEBUG("MonitoringQueriesTest: Stop query");
        ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

        auto cnt = 1;
        for (auto wrk : workers) {
            NES_DEBUG("MonitoringQueriesTest: Stop worker {}", cnt);
            bool retStopWrk3 = wrk->stop(true);
            EXPECT_TRUE(retStopWrk3);
        }

        NES_DEBUG("MonitoringQueriesTest: Stop Coordinator");
        //TODO: If this gets removed we receive random folly exceptions
        std::this_thread::sleep_for(std::chrono::seconds(1));
        bool retStopCord = crd->stopCoordinator(false);
        EXPECT_TRUE(retStopCord);
        NES_DEBUG("MonitoringQueriesTest: Test finished");

        auto metricStore = crd->getMonitoringService()->getMonitoringManager()->getMetricStore();

        // test metrics
        for (uint64_t nodeId = 2; nodeId <= workerCnt + 1; nodeId++) {
            Monitoring::StoredNodeMetricsPtr storedMetrics = metricStore->getAllMetrics(nodeId);
            ASSERT_TRUE(MetricValidator::isValid(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(),
                                                 storedMetrics,
                                                 expectedType,
                                                 nodeId,
                                                 2));
        }
    }
};

TEST_F(MonitoringQueriesTest, testDiskMetricsQueryWithStorage) {
    NES_INFO("MonitoringQueryTest: Testing disk metrics query");
    uint64_t workerCnt = 3;
    Monitoring::MetricCollectorType collectorType = Monitoring::MetricCollectorType::DISK_COLLECTOR;
    SchemaPtr schema = Monitoring::DiskMetrics::getSchema("");
    Monitoring::MetricType expectedType = Monitoring::MetricType::DiskMetric;
    runMetricsQueryTest(workerCnt, collectorType, schema, expectedType);
}

TEST_F(MonitoringQueriesTest, testCpuMetricsQueryWithStorage) {
    NES_INFO("MonitoringQueryTest: Testing cpu metrics query");
    uint64_t workerCnt = 3;
    Monitoring::MetricCollectorType collectorType = Monitoring::MetricCollectorType::CPU_COLLECTOR;
    SchemaPtr schema = Monitoring::CpuMetrics::getSchema("");
    Monitoring::MetricType expectedType = Monitoring::MetricType::WrappedCpuMetrics;
    runMetricsQueryTest(workerCnt, collectorType, schema, expectedType);
}

TEST_F(MonitoringQueriesTest, testMemoryMetricsQueryWithStorage) {
    NES_INFO("MonitoringQueryTest: Testing memory metrics query");
    uint64_t workerCnt = 3;
    Monitoring::MetricCollectorType collectorType = Monitoring::MetricCollectorType::MEMORY_COLLECTOR;
    SchemaPtr schema = Monitoring::MemoryMetrics::getSchema("");
    Monitoring::MetricType expectedType = Monitoring::MetricType::MemoryMetric;
    runMetricsQueryTest(workerCnt, collectorType, schema, expectedType);
}

TEST_F(MonitoringQueriesTest, testNetworkMetricsQueryWithStorage) {
    NES_INFO("MonitoringQueryTest: Testing network metrics query");
    uint64_t workerCnt = 3;
    Monitoring::MetricCollectorType collectorType = Monitoring::MetricCollectorType::NETWORK_COLLECTOR;
    SchemaPtr schema = Monitoring::NetworkMetrics::getSchema("");
    Monitoring::MetricType expectedType = Monitoring::MetricType::WrappedNetworkMetrics;
    runMetricsQueryTest(workerCnt, collectorType, schema, expectedType);
}

}// namespace NES