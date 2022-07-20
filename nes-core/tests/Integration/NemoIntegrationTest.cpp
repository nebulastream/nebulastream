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

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Runtime/BufferManager.hpp>

#include <Services/MonitoringService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpprest/json.h>
#include <cstdint>
#include <memory>

using std::cout;
using std::endl;
namespace NES {

uint16_t timeout = 15;

class NemoIntegrationTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("NemoIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NemoIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        NES_INFO("NemoIntegrationTest: Setting up test with rpc port " << rpcCoordinatorPort << ", rest port " << restPort);
    }
};

TEST_F(NemoIntegrationTest, testNemoFlatTopologyMerge) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "windowOut.csv";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort),
    TestUtils::enableDebug(), TestUtils::setDistributedWindowChildThreshold(100), TestUtils::setDistributedWindowCombinerThreshold(100)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;

    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                           TestUtils::physicalSourceName("test_stream_1"),
                                           TestUtils::logicalSourceName("window"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(28),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1)});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                           TestUtils::physicalSourceName("test_stream_2"),
                                           TestUtils::logicalSourceName("window"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(28),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1)});

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\")"
          ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10)))"
          ".byKey(Attribute(\\\"id\\\"))"
          ".apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 3, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,102\n"
                             "10000,20000,1,290\n"
                             "0,10000,4,2\n"
                             "0,10000,11,10\n"
                             "0,10000,12,2\n"
                             "0,10000,16,4\n";

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    ASSERT_EQ(content, expectedContent);
}

TEST_F(NemoIntegrationTest, testNemoFlatTopologyNoMerge) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "windowOut.csv";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort),
                                                    TestUtils::enableDebug(), TestUtils::setDistributedWindowChildThreshold(0), TestUtils::setDistributedWindowCombinerThreshold(100)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;

    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                           TestUtils::physicalSourceName("test_stream_1"),
                                           TestUtils::logicalSourceName("window"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(28),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1)});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                           TestUtils::physicalSourceName("test_stream_2"),
                                           TestUtils::logicalSourceName("window"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(28),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1)});

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\")"
          ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10)))"
          ".byKey(Attribute(\\\"id\\\"))"
          ".apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,51\n"
                             "10000,20000,1,145\n"
                             "0,10000,4,1\n"
                             "0,10000,11,5\n"
                             "0,10000,12,1\n"
                             "0,10000,16,2\n"
                             "0,10000,1,51\n"
                             "10000,20000,1,145\n"
                             "0,10000,4,1\n"
                             "0,10000,11,5\n"
                             "0,10000,12,1\n"
                             "0,10000,16,2\n";

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    ASSERT_EQ(content, expectedContent);
}

TEST_F(NemoIntegrationTest, testNemoThreelevels) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "windowOut.csv";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort),
                                                    TestUtils::enableDebug(), TestUtils::setDistributedWindowChildThreshold(0), TestUtils::setDistributedWindowCombinerThreshold(100)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;

    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                           TestUtils::physicalSourceName("test_stream_1"),
                                           TestUtils::logicalSourceName("window"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(28),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1)});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                           TestUtils::physicalSourceName("test_stream_2"),
                                           TestUtils::logicalSourceName("window"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(28),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1)});

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\")"
          ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10)))"
          ".byKey(Attribute(\\\"id\\\"))"
          ".apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,51\n"
                             "10000,20000,1,145\n"
                             "0,10000,4,1\n"
                             "0,10000,11,5\n"
                             "0,10000,12,1\n"
                             "0,10000,16,2\n"
                             "0,10000,1,51\n"
                             "10000,20000,1,145\n"
                             "0,10000,4,1\n"
                             "0,10000,11,5\n"
                             "0,10000,12,1\n"
                             "0,10000,16,2\n";

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    ASSERT_EQ(content, expectedContent);
}

}// namespace NES