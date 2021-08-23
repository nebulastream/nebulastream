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

#include <chrono>
#include <thread>
#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Mobility/Geo/Projection/GeoCalculator.h>
#include <Mobility/LocationService.h>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>

namespace NES {

static int sleepTime = 100;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class MobilityNodeTest : public testing::Test {
  protected:
    NesCoordinatorPtr crd;
    std::string apiEndpoint;
    uint64_t port;

  public:
    static void SetUpTestCase() {
        NES::setupLogging("MobilityRESTEndpointTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MobilityRESTEndpointTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
        apiEndpoint = "http://127.0.0.1:" + std::to_string(restPort) + "/v1/nes";

        CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
        coordinatorConfig->setRpcPort(rpcPort);
        coordinatorConfig->setRestPort(restPort);

        NES_INFO("MobilityRESTEndpointTest: Start coordinator");
        crd = std::make_shared<NesCoordinator>(coordinatorConfig);
        port = crd->startCoordinator(/**blocking**/ false);

        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));

        EXPECT_NE(port, 0u);
        NES_INFO("MobilityRESTEndpointTest: Coordinator started successfully");
    }

    void TearDown() override {
        NES_INFO("MobilityRESTEndpointTest: Stop Coordinator");
        bool retStopCord = crd->stopCoordinator(true);
        EXPECT_TRUE(retStopCord);
        NES_INFO("MobilityRESTEndpointTest: Test finished");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MobilityRESTEndpointTest test class."); }
};

TEST_F(MobilityNodeTest, testAddLocationEnabledWorker) {
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("RESTEndpointTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setCoordinatorRestPort(restPort);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    workerConfig->setWorkerName("test_worker");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    wrk1->setWithRegisterLocation(true);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    EXPECT_TRUE(crd->getLocationService()->getLocationCatalog()->contains("test_worker"));

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
}

TEST_F(MobilityNodeTest, testDeployOneWorker) {
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("QueryDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setCoordinatorRestPort(restPort);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    LocationServicePtr locationService = crd->getLocationService();
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testDeployOneWorker.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("default_logical").movingRange("veh_1", 500).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));
    EXPECT_TRUE(locationService->getLocationCatalog()->contains("veh_1"));

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
}

TEST_F(MobilityNodeTest, testLocationUpdater) {
    const double movingRangeArea = 10'000;
    const int timeToSleep = sleepTime * 6;
    const int smallOffset = 40;
    const int bigOffset = 60;

    LocationServicePtr locationService = crd->getLocationService();

    // Add a sink
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    CartesianPointPtr cartesianCenter = GeoCalculator::geographicToCartesian(center);
    locationService->addSink("test_sink", movingRangeArea);
    locationService->updateNodeLocation("test_sink", center);

    // Add source inside range
    CartesianPointPtr cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + smallOffset, cartesianCenter->getY());
    locationService->addSource("test_source");
    locationService->updateNodeLocation("test_source", GeoCalculator::cartesianToGeographic(cartesianPoint));

    std::this_thread::sleep_for(std::chrono::milliseconds(timeToSleep));
    EXPECT_TRUE(locationService->getLocationCatalog()->getSource("test_source")->isEnabled());

    // Move source outside range
    cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + bigOffset, cartesianCenter->getY());
    locationService->updateNodeLocation("test_source", GeoCalculator::cartesianToGeographic(cartesianPoint));

    std::this_thread::sleep_for(std::chrono::milliseconds(timeToSleep));
    EXPECT_FALSE(crd->getLocationService()->getLocationCatalog()->getSource("test_source")->isEnabled());

    // Move sink closer to source
    CartesianPointPtr newCenter = std::make_shared<CartesianPoint>(cartesianCenter->getX() + smallOffset, cartesianCenter->getY());
    locationService->updateNodeLocation("test_sink", GeoCalculator::cartesianToGeographic(newCenter));

    std::this_thread::sleep_for(std::chrono::milliseconds(timeToSleep));
    EXPECT_TRUE(locationService->getLocationCatalog()->getSource("test_source")->isEnabled());
}

TEST_F(MobilityNodeTest, testMovingSinkReceivesData) {
    const int timeToSleep = sleepTime * 6;
    const int smallOffset = 40;
    const int bigOffset = 60;

    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    LocationServicePtr locationService = crd->getLocationService();
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    // Add Source

    NES_INFO("MovingSikTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setCoordinatorRestPort(restPort);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    wrkConf->setWorkerName("test_source");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    wrk1->setWithRegisterLocation(true);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MovingSikTest: Worker1 started successfully");

    NES_INFO("MovingSikTest: Updating location of source 1");
    GeoPointPtr sourcePosition = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    CartesianPointPtr sourceCartesianPosition = GeoCalculator::geographicToCartesian(sourcePosition);
    locationService->updateNodeLocation("test_source", sourcePosition);

    std::this_thread::sleep_for(std::chrono::milliseconds(timeToSleep));

    // Submit Query (and add geo sink -> added during query submission)

    std::string outputFilePath = "testDeployOneWorker.out";
    remove(outputFilePath.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("default_logical").movingRange("veh_1", 10000).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    // Update sink location
    CartesianPointPtr sinkCartesianPosition = std::make_shared<CartesianPoint>(sourceCartesianPosition->getX() - bigOffset, sourceCartesianPosition->getY());
    GeoPointPtr sinkPosition = GeoCalculator::cartesianToGeographic(sinkCartesianPosition);
    locationService->updateNodeLocation("veh_1", sinkPosition);

    // Check query started
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    // Check result is empty
    EXPECT_FALSE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    // Move sink closer to the source
    sinkCartesianPosition = std::make_shared<CartesianPoint>(sourceCartesianPosition->getX() - smallOffset, sourceCartesianPosition->getY());
    sinkPosition = GeoCalculator::cartesianToGeographic(sinkCartesianPosition);
    locationService->updateNodeLocation("veh_1", sinkPosition);

    std::this_thread::sleep_for(std::chrono::milliseconds(timeToSleep));

    // Check result is not empty
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
}

}
