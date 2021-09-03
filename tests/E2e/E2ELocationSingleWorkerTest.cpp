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


#include <gtest/gtest.h>

#include <filesystem>
#include <boost/process.hpp>
#include <cpprest/http_client.h>

#include <Mobility/Geo/Projection/GeoCalculator.h>
#include <Util/TestUtils.hpp>
#include <Util/Logger.hpp>

using namespace std;
using namespace utility;
// Common utilities like string conversions
using namespace web;
// Common features like URIs.
using namespace web::http;
// Common HTTP functionality
using namespace web::http::client;
// HTTP client features
using namespace concurrency::streams;
// Asynchronous streams
namespace bp = boost::process;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4200;
uint64_t dataPort = 4400;
uint64_t restPort = 8000;
uint16_t timeout = 5;

static const int sleepTime = 500;

static const int smallOffset = 40;
static const int bigOffset = 60;

class E2ELocationSingleWorkerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ELocationSingleWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    string apiEndpoint;
    bp::child coordinatorProc;

    void SetUp() override {
        rpcPort += 10;
        dataPort += 10;
        restPort += 10;

        apiEndpoint = "http://localhost:" + std::to_string(restPort) + "/v1/nes";
    }

    [[nodiscard]] web::json::value getSource(const string& nodeId) const {
        web::http::client::http_client client(apiEndpoint);
        web::uri_builder builder("/geo/source");
        builder.append_query(("nodeId"), nodeId);

        web::json::value getSourceJsonReturn;

        client.request(web::http::methods::GET, builder.to_string())
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            EXPECT_EQ(response.status_code(), 200);
            return response.extract_json();
        })
        .then([&getSourceJsonReturn](const pplx::task<web::json::value>& task) {
            try {
                getSourceJsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("TestUtils: Error while setting return: " << e.what());
            }
        })
        .wait();

        return getSourceJsonReturn;
    }

    void updateLocation(const string& nodeId, double latitude, double longitude) const {
        web::json::value msg{};
        msg["nodeId"] =  web::json::value::string(nodeId);
        msg["latitude"] =  web::json::value::number(latitude);
        msg["longitude"] =  web::json::value::number(longitude);

        web::http::client::http_client client(apiEndpoint);
        client.request(web::http::methods::POST, "/geo/location", msg)
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            EXPECT_EQ(response.status_code(), 200);
            return response;
        })
        .wait();
    }

    void TearDown() override {
        NES_INFO("Killing coordinator process->PID: " << coordinatorProc.id());
        coordinatorProc.terminate();
    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};


TEST_F(E2ELocationSingleWorkerTest, testAddSourceAndUpdateLocation) {
    NES_INFO(" start coordinator");
    string cmdCoordinator = "./nesCoordinator --coordinatorPort=" + std::to_string(rpcPort) + " --restPort=" + std::to_string(restPort);
    coordinatorProc = bp::child(cmdCoordinator.c_str());

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());

    // Add worker with a range enabled source
    string sourceName = "test_source";
    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string workerCmd =
        "./nesWorker --coordinatorPort=" + std::to_string(rpcPort) + " --rpcPort=" + worker1RPCPort + " --dataPort=" + worker1DataPort
        + " --coordinatorRestPort=" + std::to_string(restPort)
        + " --registerLocation=true"
        + " --workerName=" + sourceName
        + " --workerRange=" + std::to_string(50);

    bp::child workerProc(workerCmd.c_str());
    uint64_t workerPid = workerProc.id();
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    NES_INFO("TestUtil: Executen GET request on URI " << apiEndpoint);

    // Check if source was registered
    web::json::value getSourceJsonReturn = getSource(sourceName);

    NES_INFO("get source: try to acc return");
    NES_DEBUG("getSource response: " << getSourceJsonReturn.serialize());
    string expected =
        R"({"enabled":false,"hasRange":false,"id":"test_source","latitude":0,"longitude":0})";
    NES_DEBUG("getSource response: expected = " << expected);
    ASSERT_EQ(getSourceJsonReturn.serialize(), expected);

    // Update source location

    updateLocation(sourceName, 6, 6);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime ));

    // Check if source registered new location and has a range

    getSourceJsonReturn = getSource(sourceName);
    NES_INFO("get source: try to acc return");
    NES_DEBUG("getSource response: " << getSourceJsonReturn.serialize());
    expected =
        R"({"enabled":false,"hasRange":true,"id":"test_source","latitude":6,"longitude":6})";
    NES_DEBUG("getSource response: expected = " << expected);
    ASSERT_EQ(getSourceJsonReturn.serialize(), expected);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
}

TEST_F(E2ELocationSingleWorkerTest, testExecutingMovingRangeQueryWithFileOutput) {
    NES_INFO(" start coordinator");
    string cmdCoordinator = "./nesCoordinator --coordinatorPort=" + std::to_string(rpcPort) + " --restPort=" + std::to_string(restPort);
    coordinatorProc = bp::child(cmdCoordinator.c_str());

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());

    // Add worker
    string sourceName = "test_source";
    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string workerCmd =
        "./nesWorker --coordinatorPort=" + std::to_string(rpcPort) + " --rpcPort=" + worker1RPCPort + " --dataPort=" + worker1DataPort
        + " --coordinatorRestPort=" + std::to_string(restPort)
        + " --registerLocation=true"
        + " --workerName=" + sourceName;

    bp::child workerProc(workerCmd.c_str());
    uint64_t workerPid = workerProc.id();
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    NES_INFO("TestUtil: Execute GET request on URI " << apiEndpoint);

    const double sourceLatitude = 52.5128417;
    const double sourceLongitude = 13.3213595;

    // Update source position
    GeoPointPtr sourcePosition = std::make_shared<GeoPoint>(sourceLatitude, sourceLongitude);
    CartesianPointPtr sourceCartesianPosition = GeoCalculator::geographicToCartesian(sourcePosition);
    updateLocation(sourceName, sourcePosition->getLatitude(), sourcePosition->getLongitude());

    // Submit moving range query
    string sinkName = "test_sink";
    string outputFilePath = "MovingRangeQueryWithFileOutput.txt";

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").movingRange(\")";
    ss << sinkName;
    ss << R"(\", 10000).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    // Update sink location
    CartesianPointPtr sinkCartesianPosition = std::make_shared<CartesianPoint>(sourceCartesianPosition->getX() - bigOffset, sourceCartesianPosition->getY());
    GeoPointPtr sinkPosition = GeoCalculator::cartesianToGeographic(sinkCartesianPosition);
    updateLocation(sinkName, sinkPosition->getLatitude(), sinkPosition->getLongitude());

    std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime ));

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

    // Check if result is empty
    EXPECT_FALSE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath, 1));

    // Move sink closer to the source
    sinkCartesianPosition = std::make_shared<CartesianPoint>(sourceCartesianPosition->getX() - smallOffset, sourceCartesianPosition->getY());
    sinkPosition = GeoCalculator::cartesianToGeographic(sinkCartesianPosition);
    updateLocation(sinkName, sinkPosition->getLatitude(), sinkPosition->getLongitude());

    // Check result is not empty
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath, 5));

    // Remove file
    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
}

TEST_F(E2ELocationSingleWorkerTest, testExecutingMovingRangeQueryWithFileOutputWithCustomTimeInverval) {
    NES_INFO(" start coordinator");
    string cmdCoordinator = "./nesCoordinator --coordinatorPort=" + std::to_string(rpcPort)
        + " --restPort=" + std::to_string(restPort)
        + " --locationUpdateInterval=" + std::to_string(sleepTime * 10);
    coordinatorProc = bp::child(cmdCoordinator.c_str());

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());

    // Add worker
    string sourceName = "test_source";
    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string workerCmd =
        "./nesWorker --coordinatorPort=" + std::to_string(rpcPort) + " --rpcPort=" + worker1RPCPort + " --dataPort=" + worker1DataPort
        + " --coordinatorRestPort=" + std::to_string(restPort)
        + " --registerLocation=true"
        + " --workerName=" + sourceName;

    bp::child workerProc(workerCmd.c_str());
    uint64_t workerPid = workerProc.id();
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    NES_INFO("TestUtil: Execute GET request on URI " << apiEndpoint);

    const double sourceLatitude = 52.5128417;
    const double sourceLongitude = 13.3213595;

    // Update source position
    GeoPointPtr sourcePosition = std::make_shared<GeoPoint>(sourceLatitude, sourceLongitude);
    CartesianPointPtr sourceCartesianPosition = GeoCalculator::geographicToCartesian(sourcePosition);
    updateLocation(sourceName, sourcePosition->getLatitude(), sourcePosition->getLongitude());

    // Submit moving range query
    string sinkName = "test_sink";
    string outputFilePath = "MovingRangeQueryWithFileOutput.txt";

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").movingRange(\")";
    ss << sinkName;
    ss << R"(\", 10000).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    // Update sink location
    CartesianPointPtr sinkCartesianPosition = std::make_shared<CartesianPoint>(sourceCartesianPosition->getX() - smallOffset, sourceCartesianPosition->getY());
    GeoPointPtr sinkPosition = GeoCalculator::cartesianToGeographic(sinkCartesianPosition);
    updateLocation(sinkName, sinkPosition->getLatitude(), sinkPosition->getLongitude());

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

    // Check if result is empty
    EXPECT_FALSE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath, 1));

    // Check result is not empty
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath, 5));

    // Remove file
    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
}

TEST_F(E2ELocationSingleWorkerTest, testExecutingMovingRangeQueryWithFileOutputWithCustomSourceTimeInverval) {
    NES_INFO(" start coordinator");
    string cmdCoordinator = "./nesCoordinator --coordinatorPort=" + std::to_string(rpcPort)
        + " --restPort=" + std::to_string(restPort);
    coordinatorProc = bp::child(cmdCoordinator.c_str());

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());

    // Add worker
    string sourceName = "test_source";
    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string workerCmd =
        "./nesWorker --coordinatorPort=" + std::to_string(rpcPort) + " --rpcPort=" + worker1RPCPort + " --dataPort=" + worker1DataPort
        + " --coordinatorRestPort=" + std::to_string(restPort)
        + " --registerLocation=true"
        + " --locationUpdateInterval=" + std::to_string(sleepTime * 10)
        + " --workerName=" + sourceName;

    bp::child workerProc(workerCmd.c_str());
    uint64_t workerPid = workerProc.id();
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    NES_INFO("TestUtil: Execute GET request on URI " << apiEndpoint);

    const double sourceLatitude = 52.5128417;
    const double sourceLongitude = 13.3213595;

    // Update source position
    GeoPointPtr sourcePosition = std::make_shared<GeoPoint>(sourceLatitude, sourceLongitude);
    CartesianPointPtr sourceCartesianPosition = GeoCalculator::geographicToCartesian(sourcePosition);
    updateLocation(sourceName, sourcePosition->getLatitude(), sourcePosition->getLongitude());

    // Submit moving range query
    string sinkName = "test_sink";
    string outputFilePath = "MovingRangeQueryWithFileOutput.txt";

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").movingRange(\")";
    ss << sinkName;
    ss << R"(\", 10000).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    // Update sink location
    CartesianPointPtr sinkCartesianPosition = std::make_shared<CartesianPoint>(sourceCartesianPosition->getX() - smallOffset, sourceCartesianPosition->getY());
    GeoPointPtr sinkPosition = GeoCalculator::cartesianToGeographic(sinkCartesianPosition);
    updateLocation(sinkName, sinkPosition->getLatitude(), sinkPosition->getLongitude());

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

    // Check if result is empty
    EXPECT_FALSE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath, 1));

    // Check result is not empty
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath, 4));

    // Remove file
    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
}


}
