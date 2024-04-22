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
#define _TURN_OFF_PLATFORM_STRING// for cpprest/details/basic_types.h
#include <BaseIntegrationTest.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cstdio>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <unistd.h>

using namespace std;
namespace NES {

class E2ECoordinatorMultiWorkerTest : public Testing::BaseIntegrationTest {
  public:
    uint16_t timeout = 5;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("E2ECoordinatorWorkerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

/**
 * @brief Testing NES with a config using a hierarchical topology.
 */
TEST_F(E2ECoordinatorMultiWorkerTest, testHierarchicalTopology) {
    NES_INFO("start coordinator");
    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
              "->addField(createField(\\\"velocity\\\", BasicType::FLOAT32))"
              "->addField(createField(\\\"quantity\\\", BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short.csv"),
                                TestUtils::physicalSourceName("test_stream1"),
                                TestUtils::logicalSourceName("QnV"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(0),
                                TestUtils::sourceGatheringInterval(1000),
                                TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::parentId(2),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short.csv"),
                                TestUtils::physicalSourceName("test_stream2"),
                                TestUtils::logicalSourceName("QnV"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(0),
                                TestUtils::sourceGatheringInterval(1000),
                                TestUtils::workerHealthCheckWaitTime(1),
                                TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 10000, 2));

    auto worker3 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::parentId(3),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short.csv"),
                                TestUtils::physicalSourceName("test_stream2"),
                                TestUtils::logicalSourceName("QnV"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(0),
                                TestUtils::sourceGatheringInterval(1000),
                                TestUtils::workerHealthCheckWaitTime(1),
                                TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 10000, 3));

    auto topology = TestUtils::getTopology(*restPort);
    NES_INFO("The final topology:\n{}", topology);
    //check edges
    for (uint64_t i = 0; i < topology.at("edges").size(); i++) {
        EXPECT_EQ(topology["edges"][i]["target"].get<int>(), i + 1);
        EXPECT_EQ(topology["edges"][i]["source"].get<int>(), i + 2);
    }
}

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidQueryWithFileOutputTwoWorkerSameSource) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "testExecutingValidQueryWithFileOutputTwoWorker.out";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "BasicType::TEXT)->addField(createField(\\\"timestamp\\\", BasicType::UINT64))"
              "->addField(createField(\\\"velocity\\\", BasicType::FLOAT32))"
              "->addField(createField(\\\"quantity\\\", BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short.csv"),
                                TestUtils::physicalSourceName("test_stream1"),
                                TestUtils::logicalSourceName("QnV"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(0),
                                TestUtils::sourceGatheringInterval(1000),
                                TestUtils::workerHealthCheckWaitTime(1)});

    auto worker2 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short.csv"),
                                TestUtils::physicalSourceName("test_stream2"),
                                TestUtils::logicalSourceName("QnV"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(0),
                                TestUtils::sourceGatheringInterval(1000),
                                TestUtils::workerHealthCheckWaitTime(1)});

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit={}", ss.str());
    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").get<QueryId>();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    string expectedContent =
        "QnV$sensor_id:Text,QnV$timestamp:INTEGER(64 bits),QnV$velocity:Float(32 bits),QnV$quantity:INTEGER(64 bits)\n"
        "R2000073,1543624020000,102.629631,8\n"
        "R2000070,1543625280000,108.166664,5\n"
        "R2000073,1543624020000,102.629631,8\n"
        "R2000070,1543625280000,108.166664,5\n";

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("content={}", content);
    NES_INFO("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidQueryWithFileOutputTwoWorkerDifferentSource) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "testExecutingValidQueryWithFileOutputTwoWorker.out";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "BasicType::TEXT)->addField(createField(\\\"timestamp\\\", "
              "BasicType::UINT64))->addField(createField(\\\"velocity\\\", BasicType::FLOAT32))"
              "->addField(createField(\\\"quantity\\\", BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 = TestUtils::startWorker(
        {TestUtils::rpcPort(0),
         TestUtils::dataPort(0),
         TestUtils::coordinatorPort(*rpcCoordinatorPort),
         TestUtils::sourceType(SourceType::CSV_SOURCE),
         TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv"),
         TestUtils::physicalSourceName("test_stream1"),
         TestUtils::logicalSourceName("QnV"),
         TestUtils::numberOfBuffersToProduce(1),
         TestUtils::numberOfTuplesToProducePerBuffer(0),
         TestUtils::sourceGatheringInterval(1000),
         TestUtils::workerHealthCheckWaitTime(1)});

    auto worker2 = TestUtils::startWorker(
        {TestUtils::rpcPort(0),
         TestUtils::dataPort(0),
         TestUtils::coordinatorPort(*rpcCoordinatorPort),
         TestUtils::sourceType(SourceType::CSV_SOURCE),
         TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv"),
         TestUtils::physicalSourceName("test_stream2"),
         TestUtils::logicalSourceName("QnV"),
         TestUtils::numberOfBuffersToProduce(1),
         TestUtils::numberOfTuplesToProducePerBuffer(0),
         TestUtils::sourceGatheringInterval(1000),
         TestUtils::workerHealthCheckWaitTime(1)});

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit={}", ss.str());
    string body = ss.str();

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").get<QueryId>();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());

    std::string line;
    bool resultWrk1 = false;
    bool resultWrk2 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content{}", line);
        std::vector<string> content = Util::splitWithStringDelimiter<std::string>(line, ",");
        if (content.at(0) == "R2000073") {
            NES_INFO("First content={}", content.at(2));
            NES_INFO("First: expContent= 102.629631");
            if (content.at(2) == "102.629631") {
                resultWrk1 = true;
            }
        } else {
            NES_INFO("Second: content={}", content.at(2));
            NES_INFO("Second: expContent= 108.166664");
            if (content.at(2) == "108.166664") {
                resultWrk2 = true;
            }
        }
    }

    ASSERT_TRUE((resultWrk1 && resultWrk2));

    int response = remove(outputFilePath.c_str());
    ASSERT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidUserQueryWithTumblingWindowFileOutput) {
    //TODO result content does not end up in file?
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "testExecutingValidUserQueryWithTumblingWindowFileOutput.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",BasicType::UINT64))"
              "->addField(createField(\\\"id\\\",BasicType::UINT64))"
              "->addField(createField(\\\"timestamp\\\",BasicType::UINT64));\"}";
    schema << endl;

    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv"),
                                TestUtils::physicalSourceName("test_stream_1"),
                                TestUtils::logicalSourceName("window"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(28),
                                TestUtils::sourceGatheringInterval(1000),
                                TestUtils::workerHealthCheckWaitTime(1)});

    auto worker2 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv"),
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
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").get<QueryId>();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 3, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,10000,1,102\n"
        "0,10000,12,2\n"
        "0,10000,4,2\n"
        "0,10000,11,10\n"
        "0,10000,16,4\n"
        "10000,20000,1,290\n";

    NES_INFO("content={}", content);
    NES_INFO("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    ASSERT_TRUE(response == 0);
}

}// namespace NES