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

uint16_t timeout = 15;

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

TEST_F(MonitoringIntegrationTest, requestStoredRegistrationMetricsDisabled) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalSourceName("default_logical"),
                                           TestUtils::physicalSourceName("test2"),
                                           TestUtils::workerHealthCheckWaitTime(1)});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalSourceName("default_logical"),
                                           TestUtils::physicalSourceName("test1"),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());
    //TODO: This should be addressed by issue 2803
    ASSERT_EQ(jsons.size(), noWorkers + 1);
}

TEST_F(MonitoringIntegrationTest, requestAllMetricsViaRest) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalSourceName("default_logical"),
                                           TestUtils::physicalSourceName("test2"),
                                           TestUtils::workerHealthCheckWaitTime(1),
                                           TestUtils::enableMonitoring()});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalSourceName("default_logical"),
                                           TestUtils::physicalSourceName("test1"),
                                           TestUtils::workerHealthCheckWaitTime(1),
                                           TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("metrics", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

    ASSERT_EQ(jsons.size(), noWorkers + 1);

    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID " << i);
        auto json = jsons[std::to_string(i)];
        NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
        ASSERT_TRUE(MetricValidator::isValidAll(SystemResourcesReaderFactory::getSystemResourcesReader(), json));
        ASSERT_TRUE(MetricValidator::checkNodeIds(json, i));
    }
}

TEST_F(MonitoringIntegrationTest, requestStoredMetricsViaRest) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalSourceName("default_logical"),
                                           TestUtils::physicalSourceName("test2"),
                                           TestUtils::workerHealthCheckWaitTime(1),
                                           TestUtils::enableMonitoring()});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalSourceName("default_logical"),
                                           TestUtils::physicalSourceName("test1"),
                                           TestUtils::workerHealthCheckWaitTime(1),
                                           TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n" + jsons.serialize());

    ASSERT_EQ(jsons.size(), noWorkers + 1);

    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID " << i);
        auto json = jsons[std::to_string(i)];
        NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
        auto jsonRegistration = json["registration"][0]["value"];
        ASSERT_TRUE(MetricValidator::isValidRegistrationMetrics(SystemResourcesReaderFactory::getSystemResourcesReader(),
                                                                jsonRegistration));
        ASSERT_EQ(jsonRegistration["NODE_ID"], i);
    }
}

TEST_F(MonitoringIntegrationTest, requestDiskMetricsWithMonitoringSinkMultiWorker) {
    bool monitoring = true;
    MetricCollectorType collectorType = MetricCollectorType::DISK_COLLECTOR;
    MetricType metricType = MetricUtils::createMetricFromCollectorType(collectorType)->getMetricType();
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

TEST_F(MonitoringIntegrationTest, requestCpuMetricsWithMonitoringSinkMultiWorker) {
    bool monitoring = true;
    MetricCollectorType collectorType = MetricCollectorType::CPU_COLLECTOR;
    MetricType metricType = MetricUtils::createMetricFromCollectorType(collectorType)->getMetricType();
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
    crd->getSourceCatalogService()->registerLogicalSource("cpu3", CpuMetrics::getSchema(""));
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->numberOfSlots = (12);
    workerConfig1->enableMonitoring = (monitoring);

    auto physicalSource1 = PhysicalSource::create("cpu3", "cpuMetrics1", sourceType);
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

    auto physicalSource2 = PhysicalSource::create("cpu3", "cpuMetrics2", sourceType);
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

    auto physicalSource3 = PhysicalSource::create("cpu3", "cpuMetrics3", sourceType);
    workerConfig3->physicalSources.add(physicalSource3);

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService(); /*register logical schema qnv*/
    NES_INFO("MultiThreadedTest: Submit query");
    string query = R"(Query::from("cpu3").sink(MonitoringSinkDescriptor::create(MetricCollectorType::%COLLECTOR%));)";
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

    // test cpu metrics
    for (uint64_t nodeId = 2; nodeId <= 4; nodeId++) {
        StoredNodeMetricsPtr storedMetrics = metricStore->getAllMetrics(nodeId);
        auto metricVec = storedMetrics->at(metricType);
        TimestampMetricPtr pairedCpuMetric = metricVec->at(0);
        MetricPtr retMetric = pairedCpuMetric->second;
        NES_INFO("MetricStoreTest: Stored metrics for ID " << nodeId << ": " << MetricUtils::toJson(storedMetrics));
        ASSERT_TRUE(MetricValidator::checkNodeIds(retMetric, nodeId));
        ASSERT_EQ(storedMetrics->size(), 2);
    }
}

TEST_F(MonitoringIntegrationTest, requestMemoryMetricsWithMonitoringSinkMultiWorker) {
    bool monitoring = true;
    MetricCollectorType collectorType = MetricCollectorType::MEMORY_COLLECTOR;
    MetricType metricType = MetricUtils::createMetricFromCollectorType(collectorType)->getMetricType();
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
    crd->getSourceCatalogService()->registerLogicalSource("memory3", MemoryMetrics::getSchema(""));
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->numberOfSlots = (12);
    workerConfig1->enableMonitoring = (monitoring);

    auto physicalSource1 = PhysicalSource::create("memory3", "memoryMetrics1", sourceType);
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

    auto physicalSource2 = PhysicalSource::create("memory3", "memoryMetrics2", sourceType);
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

    auto physicalSource3 = PhysicalSource::create("memory3", "memoryMetrics3", sourceType);
    workerConfig3->physicalSources.add(physicalSource3);

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService(); /*register logical schema qnv*/
    NES_INFO("MultiThreadedTest: Submit query");
    string query = R"(Query::from("memory3").sink(MonitoringSinkDescriptor::create(MetricCollectorType::%COLLECTOR%));)";
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

    // test memory metrics
    for (uint64_t nodeId = 2; nodeId <= 4; nodeId++) {
        StoredNodeMetricsPtr storedMetrics = metricStore->getAllMetrics(nodeId);
        auto metricVec = storedMetrics->at(metricType);
        TimestampMetricPtr pairedMemoryMetric = metricVec->at(0);
        MetricPtr retMetric = pairedMemoryMetric->second;
        NES_INFO("MetricStoreTest: Stored metrics for ID " << nodeId << ": " << MetricUtils::toJson(storedMetrics));
        ASSERT_TRUE(MetricValidator::checkNodeIds(retMetric, nodeId));
        ASSERT_EQ(storedMetrics->size(), 2);
    }
}

TEST_F(MonitoringIntegrationTest, requestAllMetricsFromMonitoringStreams) {
    uint64_t noWorkers = 2;
    uint64_t localBuffers = 4;
    uint64_t globalBuffers = 1024 * 40;
    std::set<std::string> expectedMonitoringStreams{"wrapped_network", "wrapped_cpu", "memory", "disk"};

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring(),
                                                    TestUtils::enableDebug(),
                                                    TestUtils::numberOfSlots(50),
                                                    TestUtils::numLocalBuffers(localBuffers),
                                                    TestUtils::numGlobalBuffers(globalBuffers)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalSourceName("default_logical"),
                                           TestUtils::physicalSourceName("test2"),
                                           TestUtils::workerHealthCheckWaitTime(1),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::enableDebug(),
                                           TestUtils::numberOfSlots(50),
                                           TestUtils::numLocalBuffers(localBuffers),
                                           TestUtils::numGlobalBuffers(globalBuffers)});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalSourceName("default_logical"),
                                           TestUtils::physicalSourceName("test1"),
                                           TestUtils::workerHealthCheckWaitTime(1),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::enableDebug(),
                                           TestUtils::numberOfSlots(50),
                                           TestUtils::numLocalBuffers(localBuffers),
                                           TestUtils::numGlobalBuffers(globalBuffers)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsonStart = TestUtils::makeMonitoringRestCall("start", std::to_string(*restPort));
    NES_INFO("MonitoringIntegrationTest: Started monitoring streams " << jsonStart);
    ASSERT_EQ(jsonStart.size(), expectedMonitoringStreams.size());

    ASSERT_TRUE(MetricValidator::waitForMonitoringStreamsOrTimeout(expectedMonitoringStreams, 10, *restPort));
    auto jsonMetrics = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));

    // test network metrics
    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID " << i);
        auto json = jsonMetrics[std::to_string(i)];
        NES_DEBUG("MonitoringIntegrationTest: JSON for node " << i << ":\n" << json);
        ASSERT_TRUE(MetricValidator::isValidAllStorage(SystemResourcesReaderFactory::getSystemResourcesReader(), json));
        ASSERT_TRUE(MetricValidator::checkNodeIdsStorage(json, i));
    }
}

}// namespace NES