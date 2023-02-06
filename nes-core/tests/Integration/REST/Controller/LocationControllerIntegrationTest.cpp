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

#include <API/Query.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <REST/ServerTypes.hpp>
#include <Services/LocationService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>

namespace NES {
class LocationControllerIntegrationTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationControllerIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationControllerIntegrationTest test class.");
        std::string singleLocationPath = std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv";
        remove(singleLocationPath.c_str());
        writeWaypointsToCsv(singleLocationPath, {{{52.5523, 13.3517}, 0}});
        std::string singleLocationPath2 = std::string(TEST_DATA_DIRECTORY) + "singleLocation2.csv";
        remove(singleLocationPath2.c_str());
        writeWaypointsToCsv(singleLocationPath2, {{{53.5523, -13.3517}, 0}});
    }

    static void TearDownTestCase() { NES_INFO("Tear down LocationControllerIntegrationTest test class."); }

    void startCoordinator() {
        NES_INFO("LocationControllerIntegrationTest: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("LocationControllerIntegrationTest: Coordinator started successfully");
        ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 0));
    }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";
    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
};

TEST_F(LocationControllerIntegrationTest, testGetLocationMissingQueryParameters) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    //test request without nodeId parameter
    nlohmann::json request;
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/location"});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    std::string errorMessage = res["message"].get<std::string>();
    ASSERT_EQ(errorMessage, "Missing QUERY parameter 'nodeId'");
    bool stopCrd = coordinator->stopCoordinator(true);
    ASSERT_TRUE(stopCrd);
}

TEST_F(LocationControllerIntegrationTest, testGetLocationNoSuchNodeId) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    //test request without nodeId parameter
    nlohmann::json request;
    // node id that doesn't exist
    uint64_t nodeId = 0;
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/location"},
                                cpr::Parameters{{"nodeId", std::to_string(nodeId)}});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 404l);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    std::string errorMessage = res["message"].get<std::string>();
    ASSERT_EQ(errorMessage, "No node with Id: " + std::to_string(nodeId));
}

TEST_F(LocationControllerIntegrationTest, testGetLocationNonNumericalNodeId) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    //test request without nodeId parameter
    nlohmann::json request;
    // provide node id that isn't an integer
    std::string nodeId = "abc";
    auto future =
        cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/location"}, cpr::Parameters{{"nodeId", nodeId}});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    std::string errorMessage = res["message"].get<std::string>();
    ASSERT_EQ(errorMessage, "Invalid QUERY parameter 'nodeId'. Expected type is 'UInt64'");
    bool stopCrd = coordinator->stopCoordinator(true);
    ASSERT_TRUE(stopCrd);
}

TEST_F(LocationControllerIntegrationTest, testGetSingleLocation) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    std::string latitude = "13.4";
    std::string longitude = "-23.0";
    std::string coordinateString = latitude + "," + longitude;
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = *rpcCoordinatorPort;
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
    wrkConf1->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(coordinateString));
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 1));
    uint64_t workerNodeId1 = wrk1->getTopologyNodeId();

    //test request of node location
    nlohmann::json request;
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/location"},
                                cpr::Parameters{{"nodeId", std::to_string(workerNodeId1)}});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 200l);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    EXPECT_EQ(res["id"], workerNodeId1);
    nlohmann::json::array_t locationData = res["location"];
    EXPECT_EQ(locationData[0].dump(), latitude);
    ASSERT_EQ(locationData[1].dump(), longitude);
    bool stopwrk1 = wrk1->stop(true);
    ASSERT_TRUE(stopwrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    ASSERT_TRUE(stopCrd);
}

TEST_F(LocationControllerIntegrationTest, testGetSingleLocationWhenNoLocationDataIsProvided) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 1));
    uint64_t workerNodeId1 = wrk1->getTopologyNodeId();

    //test request of node location
    nlohmann::json request;
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/location"},
                                cpr::Parameters{{"nodeId", std::to_string(workerNodeId1)}});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 200l);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    ASSERT_TRUE(res["location"].is_null());
    bool stopwrk1 = wrk1->stop(true);
    ASSERT_TRUE(stopwrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    ASSERT_TRUE(stopCrd);
}

TEST_F(LocationControllerIntegrationTest, testGetAllMobileLocationsNoMobileNodes) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    std::string latitude = "13.4";
    std::string longitude = "-23.0";
    std::string coordinateString = latitude + "," + longitude;
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = *rpcCoordinatorPort;
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
    wrkConf1->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(coordinateString));
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 1));

    //test request of node location
    nlohmann::json request;
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/location/allMobile"});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 200l);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    //no mobile nodes added yet, response should be empty
    ASSERT_TRUE(res.empty());
    bool stopwrk1 = wrk1->stop(true);
    ASSERT_TRUE(stopwrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    ASSERT_TRUE(stopCrd);
}

#ifdef S2DEF
TEST_F(LocationControllerIntegrationTest, testGetAllMobileLocationMobileNodesExist) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    auto topologyManagerService = coordinator->getTopologyManagerService();

    std::string latitude = "13.4";
    std::string longitude = "-23.0";
    std::string coordinateString = latitude + "," + longitude;
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = *rpcCoordinatorPort;
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
    wrkConf1->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(coordinateString));
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 1));

    //create mobile worker nodes
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = *rpcCoordinatorPort;
    wrkConf2->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf2->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf2->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    wrkConf2->mobilityConfiguration.pushDeviceLocationUpdates.setValue(false);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 2));

    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort = *rpcCoordinatorPort;
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation2.csv");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart3);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 3));

    uint64_t workerNodeId2 = wrk2->getTopologyNodeId();
    uint64_t workerNodeId3 = wrk3->getTopologyNodeId();

    nlohmann::json request;
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/location/allMobile"});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 200l);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG(res);
    ASSERT_TRUE(res.is_array());
    ASSERT_TRUE(res.size() == 2);
    auto locationData = std::vector<double>(2, 0);
    for (auto entry : res) {
        if (entry["id"] == workerNodeId2) {
            locationData[0] = 52.5523;
            locationData[1] = 13.3517;
        }
        if (entry["id"] == workerNodeId3) {
            locationData[0] = 53.5523;
            locationData[1] = -13.3517;
        }
        EXPECT_TRUE(entry.contains("location"));
        EXPECT_EQ(entry["location"], nlohmann::json(locationData));
    }
    bool stopwrk1 = wrk1->stop(true);
    ASSERT_TRUE(stopwrk1);
    bool stopwrk2 = wrk2->stop(true);
    ASSERT_TRUE(stopwrk2);
    bool stopwrk3 = wrk3->stop(true);
    ASSERT_TRUE(stopwrk3);
    bool stopCrd = coordinator->stopCoordinator(true);
    ASSERT_TRUE(stopCrd);
}
#endif
}// namespace NES