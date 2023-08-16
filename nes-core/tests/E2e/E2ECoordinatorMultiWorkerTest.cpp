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

uint16_t timeout = 5u;

class E2ECoordinatorMultiWorkerTest : public Testing::BaseIntegrationTest {
  public:
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

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType(SourceType::CSV_SOURCE),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short.csv"),
                                           TestUtils::physicalSourceName("test_stream1"),
                                           TestUtils::logicalSourceName("QnV"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(0),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::parentId(2),
                                           TestUtils::sourceType(SourceType::CSV_SOURCE),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short.csv"),
                                           TestUtils::physicalSourceName("test_stream2"),
                                           TestUtils::logicalSourceName("QnV"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(0),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1),
                                           TestUtils::enableDebug()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 10000, 2));

    auto worker3 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::parentId(3),
                                           TestUtils::sourceType(SourceType::CSV_SOURCE),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short.csv"),
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
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "BasicType::UINT64))->addField(createField(\\\"velocity\\\", BasicType::FLOAT32))"
              "->addField(createField(\\\"quantity\\\", BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType(SourceType::CSV_SOURCE),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short.csv"),
                                           TestUtils::physicalSourceName("test_stream1"),
                                           TestUtils::logicalSourceName("QnV"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(0),
                                           TestUtils::sourceGatheringInterval(1000),
                                           TestUtils::workerHealthCheckWaitTime(1)});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType(SourceType::CSV_SOURCE),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short.csv"),
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
    QueryId queryId = json_return.at("queryId").get<int>();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    string expectedContent =
        "QnV$sensor_id:ArrayType,QnV$timestamp:INTEGER(64 bits),QnV$velocity:Float(32 bits),QnV$quantity:INTEGER(64 bits)\n"
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
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "BasicType::UINT64))->addField(createField(\\\"velocity\\\", BasicType::FLOAT32))"
              "->addField(createField(\\\"quantity\\\", BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker1 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000073.csv"),
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
                                TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000070.csv"),
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
    QueryId queryId = json_return.at("queryId").get<int>();

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

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType(SourceType::CSV_SOURCE),
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
                                           TestUtils::sourceType(SourceType::CSV_SOURCE),
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
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").get<int>();

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
        "10000,20000,1,290\n"
        "0,10000,4,2\n"
        "0,10000,11,10\n"
        "0,10000,12,2\n"
        "0,10000,16,4\n";

    NES_INFO("content={}", content);
    NES_INFO("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    ASSERT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorMultiWorkerTest, DISABLED_testWindowingWithTwoWorkerWithTwoAggregationFunctions) {
    // This test is disabled because it tests the same functionality as
    // WindowDeploymentTest::testMultipleAggregationFunctionsOnMultipleWorkers
    // but starts the coordinator and workers as extra processes.
    // It checks for a bug that was triggered by a use case provider.
    // We leave it in here, in case we need a further E2E test for this use case.
    NES_DEBUG("Starting the coordinator.");
    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    // The next two options disable distributed windowing.
                                                    TestUtils::setDistributedWindowChildThreshold(1000),
                                                    TestUtils::setDistributedWindowCombinerThreshold(1000),
                                                    // Enable SLICING on the coordinator.
                                                    TestUtils::enableSlicingWindowing(true)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    NES_DEBUG("Configure a schema that consists of a timestamp, a grouping key and a value.");
    auto schema = "{\n"
                  "  \"logicalSourceName\": \"test_source\",\n"
                  "  \"schema\": \"Schema::create()->addField(createField(\\\"timestamp\\\", "
                  "UINT64))->addField(createField(\\\"key\\\", BasicType::UINT64))->addField(createField(\\\"value\\\", "
                  "BasicType::UINT64));\"\n"
                  "}";
    NES_DEBUG("Schema: {}", schema);
    ASSERT_TRUE(TestUtils::addLogicalSource(schema, std::to_string(*restPort)));

    NES_DEBUG("Create an input CSV file for the worker.");
    auto inputCsvPath = getTestResourceFolder() / "input.csv";
    std::ofstream inputCsvFile(inputCsvPath);
    inputCsvFile << "1100,1,58" << endl
                 << "1200,1,23" << endl
                 << "1300,2,94" << endl
                 << "1400,2,37" << endl
                 << "2500,1,83" << endl// Next window
                 << "2500,2,11" << endl
                 << flush;
    inputCsvFile.close();

    NES_DEBUG("Start the workers.");
    std::initializer_list<std::string> workerConfiguration1 = {TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                               TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                               TestUtils::csvSourceFilePath(inputCsvPath),
                                                               TestUtils::physicalSourceName("test_source_1"),
                                                               TestUtils::logicalSourceName("test_source"),
                                                               TestUtils::numberOfTuplesToProducePerBuffer(10),
                                                               TestUtils::enableSlicingWindowing()};
    std::initializer_list<std::string> workerConfiguration2 = {TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                                               TestUtils::sourceType(SourceType::CSV_SOURCE),
                                                               TestUtils::csvSourceFilePath(inputCsvPath),
                                                               TestUtils::physicalSourceName("test_source_2"),
                                                               TestUtils::logicalSourceName("test_source"),
                                                               TestUtils::numberOfTuplesToProducePerBuffer(10),
                                                               TestUtils::enableSlicingWindowing()};
    auto worker1 = TestUtils::startWorker(workerConfiguration1);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));
    auto worker2 = TestUtils::startWorker(workerConfiguration2);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    NES_DEBUG("Execute an aggregation query and write output to a file.");
    auto outputPath = getTestResourceFolder() / "output.csv";
    std::stringstream query;
    query
        << "{\n"
           "  \"userQuery\": "
           "\"Query::from(\\\"test_source\\\").window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), "
           "Seconds(1))).byKey(Attribute(\\\"key\\\")).apply(Sum(Attribute(\\\"value\\\"))->as(Attribute(\\\"Sum1\\\")), Count())"
        << ".sink(FileSinkDescriptor::create(\\\"" << outputPath.c_str()
        << "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"));\",\n"
           "  \"placement\": \"BottomUp\"\n"
           "}";
    NES_DEBUG("Query: {}", query.str());
    nlohmann::json json_result = TestUtils::startQueryViaRest(query.str(), std::to_string(*restPort));
    QueryId queryId = json_result["queryId"].get<int>();
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    std::stringstream outputPathAsString;
    outputPathAsString << outputPath;
    NES_DEBUG("Read in output file: {}", outputPathAsString.str());
    std::ifstream outputFile(outputPath);
    ASSERT_TRUE(outputFile.good());
    // Expected output:
    // 1000,2000,1,81,2
    // 1000,2000,2,131,2
    // 2000,3000,1,83,1
    // 2000,3000,2,11,1
    // Actual output:
    // 1000,2000,1,81,2
    // 1000,2000,2,94,1
    // ???
    for (std::string line; std::getline(outputFile, line);) {
        std::cout << line << endl;
    }
    FAIL();
}

}// namespace NES