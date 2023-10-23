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

#include <API/QueryAPI.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Services/QueryService.hpp>

#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Util/MetricValidator.hpp>

#include <Monitoring/MonitoringPlan.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/MetricCollectorType.hpp>

#include <Util/Logger/Logger.hpp>
#include <memory>
#include <nlohmann/json.hpp>

using std::cout;
using std::endl;
namespace NES {

class E2EMonitoringTest : public Testing::BaseIntegrationTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    uint16_t timeout = 15;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("E2EMonitoringTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup E2EMonitoringTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
    }
};

TEST_F(E2EMonitoringTest, requestStoredRegistrationMetrics) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n{}", jsons.dump());
    ASSERT_EQ(jsons.size(), noWorkers + 1);
}

TEST_F(E2EMonitoringTest, requestAllMetricsViaRest) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("metrics", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n{}", jsons.dump());

    ASSERT_EQ(jsons.size(), noWorkers + 1);
    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID {}", i);
        auto json = jsons[std::to_string(i)];
        NES_DEBUG("E2EMonitoringTest: JSON for node {}:\n{}", i, json.dump());
        auto jsonString = json.dump();
        nlohmann::json jsonLohmann = nlohmann::json::parse(jsonString);
        ASSERT_TRUE(
            MetricValidator::isValidAll(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), jsonLohmann));
        ASSERT_TRUE(MetricValidator::checkNodeIds(jsonLohmann, i));
    }
}

TEST_F(E2EMonitoringTest, requestStoredMetricsViaRest) {
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsons = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));
    NES_INFO("ResourcesReaderTest: Jsons received: \n{}", jsons.dump());

    ASSERT_EQ(jsons.size(), noWorkers + 1);

    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID {}", i);
        auto json = jsons[std::to_string(i)];
        NES_DEBUG("E2EMonitoringTest: JSON for node {}:\n{}", i, json.dump());
        auto jsonRegistration = json["RegistrationMetric"][0]["value"];
        auto jsonString = jsonRegistration.dump();
        nlohmann::json jsonRegistrationLohmann = nlohmann::json::parse(jsonString);
        ASSERT_TRUE(
            MetricValidator::isValidRegistrationMetrics(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(),
                                                        jsonRegistrationLohmann));
        ASSERT_EQ(jsonRegistrationLohmann["NODE_ID"], i);
    }
}

TEST_F(E2EMonitoringTest, requestAllMetricsFromMonitoringStreams) {
    auto expectedMonitoringStreams = Monitoring::MonitoringPlan::defaultPlan()->getMetricTypes();
    uint64_t noWorkers = 2;
    auto coordinator = TestUtils::startCoordinator({TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableMonitoring()});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::enableNautilusWorker(),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::enableMonitoring(),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    auto jsonStart = TestUtils::makeMonitoringRestCall("start", std::to_string(*restPort));
    NES_INFO("E2EMonitoringTest: Started monitoring streams {}", jsonStart.dump());
    ASSERT_EQ(jsonStart.size(), expectedMonitoringStreams.size());

    ASSERT_TRUE(MetricValidator::waitForMonitoringStreamsOrTimeout(expectedMonitoringStreams, 100, *restPort));
    auto jsonMetrics = TestUtils::makeMonitoringRestCall("storage", std::to_string(*restPort));

    // test network metrics
    for (uint64_t i = 1; i <= noWorkers + 1; i++) {
        NES_INFO("ResourcesReaderTest: Requesting monitoring data from node with ID {}", i);
        auto json = jsonMetrics[std::to_string(i)];
        NES_DEBUG("E2EMonitoringTest: JSON for node {}:\n{}", i, json.dump());
        auto jsonString = json.dump();
        nlohmann::json jsonLohmann = nlohmann::json::parse(jsonString);
        ASSERT_TRUE(MetricValidator::isValidAllStorage(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(),
                                                       jsonLohmann));
        ASSERT_TRUE(MetricValidator::checkNodeIdsStorage(jsonLohmann, i));
    }
}

TEST_F(E2EMonitoringTest, DISABLED_testNemoPlacementWithMonitoringSource) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->enableMonitoring = true;
    coordinatorConfig->optimizer.enableNemoPlacement = true;

    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->enableMonitoring = true;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTimestampCsvSink.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    auto query = Query::from("WrappedNetworkMetrics")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .byKey(Attribute("node_id"))
                     .apply(Sum(Attribute("tBytes")))
                     .sink(FileSinkDescriptor::create(outputFilePath, true));

    QueryId queryId = queryService->addQueryRequest("",
                                                    query.getQueryPlan(),
                                                    Optimizer::PlacementStrategy::BottomUp,
                                                    FaultToleranceType::NONE,
                                                    LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    auto lines = 5;
    ASSERT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(lines, outputFilePath, 30));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("ContinuousSourceTest: content=\n{}", content);
    auto lineCnt = countOccurrences("\n", content);
    EXPECT_EQ(countOccurrences(",", content), 4 * lineCnt);
    EXPECT_EQ(countOccurrences("timestamp", content), 1);

    NES_INFO("ContinuousSourceTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("ContinuousSourceTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("ContinuousSourceTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}

}// namespace NES