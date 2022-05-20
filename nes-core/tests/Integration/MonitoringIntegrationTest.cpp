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

#include "../../tests/util/MetricValidator.hpp"
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
//#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Storage/AbstractMetricStore.hpp>

#include <Runtime/BufferManager.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>

#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MonitoringSourceType.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/MonitoringService.hpp>
#include <Services/QueryService.hpp>
#include <cpprest/json.h>
#include <cstdint>
#include <memory>
#include <regex>

using std::cout;
using std::endl;
namespace NES {

class MonitoringIntegrationTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MonitoringIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MonitoringIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        NES_INFO("MonitoringIntegrationTest: Setting up test with rpc port " << rpcCoordinatorPort << ", rest port " << restPort);
    }
};

TEST_F(MonitoringIntegrationTest, requestRuntimeMetricsEnabled) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->enableMonitoring = (true);
    coordinatorConfig->restPort = *restPort;
    wrkConf1->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf1->enableMonitoring = (true);
    wrkConf2->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf2->enableMonitoring = (true);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::set<MetricType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    auto const nodeNumber = static_cast<std::size_t>(3U);
    ASSERT_TRUE(crd->getMonitoringService()->isMonitoringEnabled());
    auto jsons = crd->getMonitoringService()->requestMonitoringDataFromAllNodesAsJson();
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

    ASSERT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("ResourcesReaderTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];
        NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
        ASSERT_TRUE(MetricValidator::isValid(SystemResourcesReaderFactory::getSystemResourcesReader(), json));
    }

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(MonitoringIntegrationTest, requestStoredRegistrationMetricsEnabled) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->enableMonitoring = (true);
    coordinatorConfig->restPort = *restPort;
    wrkConf1->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf1->enableMonitoring = (true);
    wrkConf2->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf2->enableMonitoring = (true);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::set<MetricType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    auto const nodeNumber = static_cast<std::size_t>(3U);
    ASSERT_TRUE(crd->getMonitoringService()->isMonitoringEnabled());
    auto jsons = crd->getMonitoringService()->requestNewestMonitoringDataFromMetricStoreAsJson();
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

    ASSERT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("ResourcesReaderTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];
        ASSERT_TRUE(json.has_field("registration"));
        json = json["registration"];

        for (int j = 0; j < (int) json.size(); j++) {
            auto data = json.at(j)["value"];
            ASSERT_TRUE(
                MetricValidator::isValidRegistrationMetrics(SystemResourcesReaderFactory::getSystemResourcesReader(), data));
        }
    }

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(MonitoringIntegrationTest, requestStoredRegistrationMetricsDisabled) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    bool monitoring = false;

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->enableMonitoring = (monitoring);
    wrkConf1->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf1->enableMonitoring = (monitoring);
    wrkConf2->coordinatorPort = (*rpcCoordinatorPort);
    wrkConf2->enableMonitoring = (monitoring);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0ull);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);
    cout << "worker2 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    // requesting the monitoring data
    auto metrics = std::set<MetricType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    auto const nodeNumber = static_cast<std::size_t>(3U);

    ASSERT_FALSE(crd->getMonitoringService()->isMonitoringEnabled());

    auto jsons = crd->getMonitoringService()->requestNewestMonitoringDataFromMetricStoreAsJson();
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

    ASSERT_EQ(jsons.size(), nodeNumber);
    auto rootId = crd->getTopology()->getRoot()->getId();
    NES_INFO("MonitoringIntegrationTest: Starting iteration with ID " << rootId);

    for (auto i = static_cast<std::size_t>(rootId); i < rootId + nodeNumber; ++i) {
        NES_INFO("ResourcesReaderTest: Coordinator requesting monitoring data from worker 127.0.0.1:"
                 + std::to_string(port + 10));
        auto json = jsons[std::to_string(i)];
        ASSERT_TRUE(json.has_field("registration"));
        json = json["registration"];
        for (int j = 0; j < (int) json.size(); j++) {
            auto data = json.at(j)["value"];
            ASSERT_TRUE(
                MetricValidator::isValidRegistrationMetrics(SystemResourcesReaderFactory::getSystemResourcesReader(), data));
        }
    }

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(MonitoringIntegrationTest, requestMetricsContinuouslyEnabled) {
    // WIP of issue #2620
    bool monitoring = true;

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->enableMonitoring = (monitoring);

    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical schema
    auto schema = DiskMetrics::getSchema("");
    crd->getSourceCatalogService()->registerLogicalSource("diskMetricsStream", schema);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->numberOfSlots = (12);
    workerConfig1->enableMonitoring = (monitoring);

    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->numberOfSlots = (12);
    workerConfig2->enableMonitoring = (monitoring);

    MonitoringSourceTypePtr sourceType = MonitoringSourceType::create(MetricCollectorType::DISK_COLLECTOR);

    auto physicalSource1 = PhysicalSource::create("diskMetricsStream", "diskMetrics1", sourceType);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService(); /*register logical schema qnv*/

    NES_INFO("MultiThreadedTest: Submit query");
    string query = R"(Query::from("diskMetricsStream").sink(PrintSinkDescriptor::create());)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_DEBUG("MultiThreadedTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("MultiThreadedTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultiThreadedTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultiThreadedTest: Test finished");
}

TEST_F(MonitoringIntegrationTest, requestMetricsContinuouslyEnabledWithMonitoringSinkMultiWorker) {
    bool monitoring = true;
    MetricCollectorType collectorType = MetricCollectorType::DISK_COLLECTOR;
    MetricType metricType = MetricUtils::createMetricFromCollector(collectorType)->getMetricType();
    MonitoringSourceTypePtr sourceType = MonitoringSourceType::create(collectorType);
    std::string metricCollectorStr = NES::toString(collectorType);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->enableMonitoring = monitoring;

    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical schema
    crd->getSourceCatalogService()->registerLogicalSource("disk3", NetworkMetrics::getSchema(""));
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->numberOfSlots = (12);
    workerConfig1->enableMonitoring = (monitoring);

    auto physicalSource1 = PhysicalSource::create("disk3", "diskMetrics1", sourceType);
    workerConfig1->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;
    workerConfig2->numberOfSlots = (22);
    workerConfig2->enableMonitoring = (monitoring);

    auto physicalSource2 = PhysicalSource::create("disk3", "diskMetrics2", sourceType);
    workerConfig2->physicalSources.add(physicalSource2);

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = *rpcCoordinatorPort;
    workerConfig3->numberOfSlots = (33);
    workerConfig3->enableMonitoring = (monitoring);

    auto physicalSource3 = PhysicalSource::create("disk3", "diskMetrics3", sourceType);
    workerConfig3->physicalSources.add(physicalSource3);

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService(); /*register logical schema qnv*/
    NES_INFO("MultiThreadedTest: Submit query");
    string query = R"(Query::from("disk3").sink(MonitoringSinkDescriptor::create(MetricCollectorType::%COLLECTOR%));)";
    query = std::regex_replace(query, std::regex("%COLLECTOR%"), metricCollectorStr);

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_DEBUG("MultiThreadedTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    NES_DEBUG("MultiThreadedTest: Stop query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");

    auto metricStore = crd->getMonitoringService()->getMonitoringManager()->getMetricStore();

    // test disk metrics
    for (uint64_t nodeId = 2; nodeId <= 4; nodeId++) {
        StoredNodeMetricsPtr storedMetrics = metricStore->getAllMetrics(nodeId);
        auto metricVec = storedMetrics->at(metricType);
        TimestampMetricPtr pairedDiskMetric = metricVec->at(0);
        MetricPtr retMetric = pairedDiskMetric->second;
        NES_INFO("MetricStoreTest: Stored metrics for ID " << nodeId << ": " << MetricUtils::toJson(storedMetrics));
        ASSERT_TRUE(MetricValidator::checkNodeIds(retMetric, nodeId));
        ASSERT_EQ(storedMetrics->size(), 2);
    }
}

}// namespace NES