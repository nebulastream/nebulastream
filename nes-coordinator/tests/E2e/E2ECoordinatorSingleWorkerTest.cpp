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
#include <Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <stdio.h>
#include <string>

using namespace std;
namespace NES {

class E2ECoordinatorSingleWorkerTest : public Testing::BaseIntegrationTest {
  public:
    uint16_t timeout = 5;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("E2ECoordinatorSingleWorkerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }
};

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithPrintOutput) {

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test")});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());")";
    ss << R"(,"placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutput) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test")});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");

    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    ifstream my_file(outputFilePath);
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(outputFilePath.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "default_logical$id:INTEGER(32 bits),default_logical$value:INTEGER(64 bits)\n"
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

    NES_INFO("content={}", content);
    NES_INFO("expContent={}", expectedContent);
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorSingleWorkerTest, DISABLED_testExecutingValidUserQueryVariableSizeWithFileOutput) {
    //TODO: This is part of issue #3146 and will be addressed there
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test")});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");

    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    ifstream my_file(outputFilePath);
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(outputFilePath.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

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

    NES_INFO("content={}", content);
    NES_INFO("expContent={}", expectedContent);
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputWithFilter) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "UserQueryWithFileOutputWithFilterTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test")});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").filter(Attribute(\"id\") >= 1).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit={}", ss.str());
    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    // if filter is applied correctly, no output is generated
    NES_INFO("read file={}", outputFilePath);
    ifstream outFile(outputFilePath);
    EXPECT_TRUE(outFile.good());
    std::string content((std::istreambuf_iterator<char>(outFile)), (std::istreambuf_iterator<char>()));
    NES_INFO("content={}", content);
    std::string expected = "default_logical$id:INTEGER(32 bits),default_logical$value:INTEGER(64 bits)\n"
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

    NES_DEBUG("expected={}", expected);
    EXPECT_EQ(expected, content);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputAndRegisterPhysource) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithFileOutputAndRegisterPhysourceTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::physicalSourceName("test_stream"),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::numberOfBuffersToProduce(2),
                                          TestUtils::sourceGatheringInterval(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");

    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    ifstream my_file(outputFilePath);
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(outputFilePath.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    std::string expectedContent = "default_logical$id:INTEGER(32 bits),default_logical$value:INTEGER(64 bits)\n"
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

    NES_INFO("content={}", content);
    NES_INFO("expContent={}", expectedContent);
    EXPECT_EQ(content, expectedContent);
    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputKTMUseCase) {
    NES_INFO("start coordinator");
    std::string testFile = "ktm-results.csv";
    remove(testFile.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::setDistributedWindowChildThreshold(1000),
                                                    TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::setDistributedWindowCombinerThreshold(1000)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"ktm\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"Time\\\",BasicType::UINT64))->"
              "addField(createField(\\\"Dist\\\",BasicType::UINT64))->"
              "addField(createField(\\\"ABS_Front_Wheel_Press\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"ABS_Rear_Wheel_Press\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"ABS_Front_Wheel_Speed\\\",BasicType::FLOAT64))->"// 5th col.
              "addField(createField(\\\"ABS_Rear_Wheel_Speed\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"V_GPS\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"MMDD\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"HHMM\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"LAS_Ax1\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"LAS_Ay1\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"LAS_Az_Vertical_Acc\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"ABS_Lean_Angle\\\",BasicType::FLOAT64))->"// 13th col.
              "addField(createField(\\\"ABS_Pitch_Info\\\",BasicType::FLOAT64))->"// 14th col.
              "addField(createField(\\\"ECU_Gear_Position\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"ECU_Accel_Position\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"ECU_Engine_Rpm\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"ECU_Water_Temperature\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"ECU_Oil_Temp_Sensor_Data\\\",BasicType::UINT64))->"
              "addField(createField(\\\"ECU_Side_StanD\\\",BasicType::UINT64))->"
              "addField(createField(\\\"Longitude\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"Latitude\\\",BasicType::FLOAT64))->"
              "addField(createField(\\\"Altitude\\\",BasicType::FLOAT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::CSV_SOURCE),
                                          TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "ktm.csv"),
                                          TestUtils::physicalSourceName("test_stream"),
                                          TestUtils::logicalSourceName("ktm"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::numberOfTuplesToProducePerBuffer(3),
                                          TestUtils::enableNautilusWorker(),
                                          TestUtils::sourceGatheringInterval(1),
                                          TestUtils::enableSlicingWindowing()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"ktm\"))";
    ss << R"(.window(TumblingWindow::of(EventTime(Attribute(\"Time\")), Seconds(1))))";
    ss << R"(.apply(Avg(Attribute(\"ABS_Lean_Angle\"))->as(Attribute(\"avg_value_1\")), Avg(Attribute(\"ABS_Pitch_Info\"))->as(Attribute(\"avg_value_2\")), Avg(Attribute(\"ABS_Front_Wheel_Speed\"))->as(Attribute(\"avg_value_3\")), Count()->as(Attribute(\"count_value\"))))";
    ss << R"(.sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")))";
    ss << R"(;","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    string expectedContent = "ktm$start:INTEGER(64 bits),ktm$end:INTEGER(64 bits),ktm$avg_value_1:Float(64 bits),ktm$avg_value_2:"
                             "Float(64 bits),ktm$avg_value_3:Float(64 bits),ktm$count_value:INTEGER(64 bits)\n"
                             "1543620000000,1543620001000,14.400000,0.800000,0.500000,2\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithTumblingWindowFileOutput) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithTumbWindowFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::setDistributedWindowChildThreshold(1000),
                                                    TestUtils::setDistributedWindowCombinerThreshold(1000)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",BasicType::UINT64))"
              "->addField(createField(\\\"id\\\",BasicType::UINT64))"
              "->addField(createField(\\\"timestamp\\\",BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::CSV_SOURCE),
                                          TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv"),
                                          TestUtils::physicalSourceName("test_stream"),
                                          TestUtils::logicalSourceName("window"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::sourceGatheringInterval(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

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

    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    // if filter is applied correctly, no output is generated
    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,10000,1,51\n"
        "0,10000,12,1\n"
        "0,10000,4,1\n"
        "0,10000,16,2\n"
        "0,10000,11,5\n"
        "10000,20000,1,145\n"
        "20000,30000,1,41\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithSlidingWindowFileOutput) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithSlidWindowFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::setDistributedWindowChildThreshold(1000),
                                                    TestUtils::setDistributedWindowCombinerThreshold(1000)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",BasicType::UINT64))->"
              "addField(createField(\\\"id\\\",BasicType::UINT64))->"
              "addField(createField(\\\"timestamp\\\",BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::CSV_SOURCE),
                                          TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv"),
                                          TestUtils::physicalSourceName("test_stream"),
                                          TestUtils::logicalSourceName("window"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::sourceGatheringInterval(1)});

    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\")"
          ".window(SlidingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10), Seconds(5)))"
          ".byKey(Attribute(\\\"id\\\"))"
          ".apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit={}", ss.str());
    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    string expectedContent =
        "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),window$value:INTEGER(64 bits)\n"
        "0,10000,1,51\n"
        "0,10000,12,1\n"
        "0,10000,4,1\n"
        "0,10000,11,5\n"
        "0,10000,16,2\n"
        "5000,15000,1,95\n"
        "10000,20000,1,145\n"
        "15000,25000,1,126\n"
        "20000,30000,1,41\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));
}

TEST_F(E2ECoordinatorSingleWorkerTest, testKillWorkerWithoutQuery) {
    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::coordinatorHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
    NES_DEBUG("start crd with pid={}", coordinator.getPid());

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test"),
                                          TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));
    NES_DEBUG("start worker with pid={}", worker.getPid());
    sleep(5);
    worker.kill();
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
}

TEST_F(E2ECoordinatorSingleWorkerTest, testKillWorkerWithQueryAfterUnregister) {
    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::coordinatorHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
    NES_DEBUG("start crd with pid={}", coordinator.getPid());

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test"),
                                          TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));
    NES_DEBUG("start worker with pid={}", worker.getPid());

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());")";
    ss << R"(,"placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    worker.kill();
    sleep(5);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
}

TEST_F(E2ECoordinatorSingleWorkerTest, testKillWorkerWithQueryDeployed) {
    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::coordinatorHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
    NES_DEBUG("start crd with pid={}", coordinator.getPid());

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test"),
                                          TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));
    NES_DEBUG("start worker with pid={}", worker.getPid());

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());")";
    ss << R"(,"placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    sleep(5);
    worker.kill();
    sleep(5);
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));
}

TEST_F(E2ECoordinatorSingleWorkerTest, DISABLED_testKillCoordinatorWithoutQuery) {
    remove("nesWorkerStarter.log");

    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::coordinatorHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
    NES_DEBUG("start crd with pid={}", coordinator.getPid());

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test"),
                                          TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));
    NES_DEBUG("start worker with pid={}", worker.getPid());
    sleep(5);
    coordinator.kill();
    sleep(5);

    string searchStr = "coordinator went down so shutting down the worker";
    ifstream inFile;
    string line;

    inFile.open("nesWorkerStarter.log");

    if (!inFile) {
        cout << "Unable to open file" << endl;
        exit(1);
    }

    size_t pos;
    bool found = false;
    while (inFile.good()) {
        getline(inFile, line);
        pos = line.find(searchStr);
        if (pos != string::npos) {
            cout << "Found line";
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
}

TEST_F(E2ECoordinatorSingleWorkerTest, DISABLED_testKillCoordinatorWithQueryRunning) {
    remove("nesWorkerStarter.log");

    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::coordinatorHealthCheckWaitTime(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
    NES_DEBUG("start crd with pid={}", coordinator.getPid());

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test"),
                                          TestUtils::workerHealthCheckWaitTime(1)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));
    NES_DEBUG("start worker with pid={}", worker.getPid());

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());")";
    ss << R"(,"placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    sleep(5);
    coordinator.kill();
    sleep(5);

    string searchStr = "coordinator went down so shutting down the worker";
    ifstream inFile;
    string line;

    inFile.open("nesWorkerStarter.log");

    if (!inFile) {
        cout << "Unable to open file" << endl;
        exit(1);
    }

    size_t pos;
    bool found = false;
    while (inFile.good()) {
        getline(inFile, line);
        pos = line.find(searchStr);
        if (pos != string::npos) {
            cout << "Found line";
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithThresholdWindowFileOutputKTMUseCase) {
    NES_INFO("start coordinator");
    std::string testFile = getTestResourceFolder() / "ktm-results.csv";
    NES_INFO("testFile = {}", testFile);
    remove(testFile.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::setDistributedWindowChildThreshold(1000),
                                                    TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::setDistributedWindowCombinerThreshold(1000)});

    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema
        << "{\"logicalSourceName\" : \"ktm\",\"schema\" "
           ":\"Schema::create()->addField(createField(\\\"Time\\\",BasicType::INT32))->"
           "addField(createField(\\\"Dist\\\",BasicType::UINT64))->"
           "addField(createField(\\\"ABS_Front_Wheel_Press\\\",BasicType::UINT64))->"
           "addField(createField(\\\"ABS_Rear_Wheel_Press\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"ABS_Front_Wheel_Speed\\\",BasicType::FLOAT64))->"// 5th col.
           "addField(createField(\\\"ABS_Rear_Wheel_Speed\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"V_GPS\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"MMDD\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"HHMM\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"LAS_Ax1\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"LAS_Ay1\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"LAS_Az_Vertical_Acc\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"ABS_Lean_Angle\\\",BasicType::FLOAT32))->"// 13th col.
           "addField(createField(\\\"ABS_Pitch_Info\\\",BasicType::FLOAT64))->"// 14th col.
           "addField(createField(\\\"ECU_Gear_Position\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"ECU_Accel_Position\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"ECU_Engine_Rpm\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"ECU_Water_Temperature\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"ECU_Oil_Temp_Sensor_Data\\\",BasicType::INT32))->"//TODO I changed that to i32 to prevent failure because of different data types
           "addField(createField(\\\"ECU_Side_StanD\\\",BasicType::INT32))->"
           "addField(createField(\\\"Longitude\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"Latitude\\\",BasicType::FLOAT64))->"
           "addField(createField(\\\"Altitude\\\",BasicType::FLOAT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker(
        {TestUtils::rpcPort(0),
         TestUtils::dataPort(0),
         TestUtils::coordinatorPort(*rpcCoordinatorPort),
         TestUtils::sourceType(SourceType::CSV_SOURCE),
         TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY)
                                      + "ktm_thresholdtest.csv"),//I created a new file to open and close a threshold window
         TestUtils::physicalSourceName("test_stream"),
         TestUtils::logicalSourceName("ktm"),
         TestUtils::numberOfBuffersToProduce(1),
         TestUtils::enableNautilusWorker(),
         TestUtils::numberOfTuplesToProducePerBuffer(4),
         TestUtils::sourceGatheringInterval(1),
         TestUtils::enableSlicingWindowing()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    //TODO : without logicalStreamName$Attribute (qualified att names) the Nautilus query fails with the error: attr. does not exits

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"ktm\"))";
    ss << R"(.window(ThresholdWindow::of(Attribute(\"ECU_Oil_Temp_Sensor_Data\") > 15)))";
    ss << R"(.apply(Min(Attribute(\"ABS_Lean_Angle\")), Avg(Attribute(\"ABS_Front_Wheel_Speed\")), Count()))";
    ss << R"(.sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")))";
    ss << R"(;","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    string expectedContent =
        "ktm$ABS_Lean_Angle:Float(32 bits),ktm$ABS_Front_Wheel_Speed:Float(64 bits),ktm$count:INTEGER(64 bits)\n"
        "14.300000,0.500000,2\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);
}

//TODO #3801 fixing bykey()
TEST_F(E2ECoordinatorSingleWorkerTest, DISABLED_testExecutingThresholdWindowKTMByKey) {
    NES_INFO("start coordinator");
    std::string testFile = getTestResourceFolder() / "ktm-results.csv";
    NES_INFO("testFile = {}", testFile);
    remove(testFile.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::setDistributedWindowChildThreshold(1000),
                                                    TestUtils::setDistributedWindowCombinerThreshold(1000)});

    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"ktm\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"Time\\\",INT32))->addField(createField(\\\"Dist\\\",UINT64))->"
              "addField(createField(\\\"ABS_Front_Wheel_Press\\\",UINT64))->"
              "addField(createField(\\\"ABS_Rear_Wheel_Press\\\",FLOAT64))->"
              "addField(createField(\\\"ABS_Front_Wheel_Speed\\\",FLOAT64))->"// 5th col.
              "addField(createField(\\\"ABS_Rear_Wheel_Speed\\\",FLOAT64))->"
              "addField(createField(\\\"V_GPS\\\",FLOAT64))->"
              "addField(createField(\\\"MMDD\\\",FLOAT64))->"
              "addField(createField(\\\"HHMM\\\",FLOAT64))->"
              "addField(createField(\\\"LAS_Ax1\\\",FLOAT64))->"
              "addField(createField(\\\"LAS_Ay1\\\",FLOAT64))->"
              "addField(createField(\\\"LAS_Az_Vertical_Acc\\\",FLOAT64))->"
              "addField(createField(\\\"ABS_Lean_Angle\\\",FLOAT32))->"// 13th col.
              "addField(createField(\\\"ABS_Pitch_Info\\\",FLOAT64))->"// 14th col.
              "addField(createField(\\\"ECU_Gear_Position\\\",FLOAT64))->"
              "addField(createField(\\\"ECU_Accel_Position\\\",FLOAT64))->"
              "addField(createField(\\\"ECU_Engine_Rpm\\\",FLOAT64))->"
              "addField(createField(\\\"ECU_Water_Temperature\\\",FLOAT64))->"
              "addField(createField(\\\"ECU_Oil_Temp_Sensor_Data\\\",FLOAT32))->"
              "addField(createField(\\\"ECU_Side_StanD\\\",INT32))->"
              "addField(createField(\\\"Longitude\\\",FLOAT64))->"
              "addField(createField(\\\"Latitude\\\",FLOAT64))->"
              "addField(createField(\\\"Altitude\\\",FLOAT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker(
        {TestUtils::rpcPort(0),
         TestUtils::dataPort(0),
         TestUtils::coordinatorPort(*rpcCoordinatorPort),
         TestUtils::sourceType(SourceType::CSV_SOURCE),
         TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY)
                                      + "ktm_thresholdtest.csv"),//I created a new file to open and close a threshold window
         TestUtils::physicalSourceName("test_stream"),
         TestUtils::logicalSourceName("ktm"),
         TestUtils::numberOfBuffersToProduce(1),
         TestUtils::enableNautilusWorker(),
         TestUtils::numberOfTuplesToProducePerBuffer(4),
         TestUtils::sourceGatheringInterval(1),
         TestUtils::enableSlicingWindowing()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"ktm\"))";
    ss << R"(.window(ThresholdWindow::of(Attribute(\"ECU_Oil_Temp_Sensor_Data\") > 15)))";
    ss << R"(.byKey(Attribute(\"Time\")))";
    ss << R"(.apply(Count(), Avg(Attribute(\"ABS_Lean_Angle\")), Avg(Attribute(\"ABS_Pitch_Info\")), Avg(Attribute(\"ABS_Front_Wheel_Speed\"))))";
    ss << R"(.sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")))";
    ss << R"(;","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    string expectedContent = "ktm$count:INTEGER(64 bits),ktm$ABS_Lean_Angle:Float(64 bits),ktm$ABS_Pitch_Info:Float(64 "
                             "bits),ktm$ABS_Front_Wheel_Speed:Float(64 bits)\n"
                             "2,14.400000,0.800000,0.500000\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testMoritz) {

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"defaultLogicalSourceName\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"f1\\\",BasicType::UINT64))->"
              "addField(createField(\\\"ts\\\",BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto path = std::filesystem::path(TEST_DATA_DIRECTORY) / "countMinInput.csv";

    auto worker = TestUtils::startWorker({
        TestUtils::rpcPort(0),
        TestUtils::dataPort(0),
        TestUtils::coordinatorPort(*rpcCoordinatorPort),
        TestUtils::sourceType(SourceType::CSV_SOURCE),
        TestUtils::csvSourceFilePath(std::filesystem::path(path)),
        TestUtils::logicalSourceName("defaultLogicalSourceName"),
        TestUtils::physicalSourceName("defaultPhysicalSourceName"),
        TestUtils::numberOfBuffersToProduce(1),
        TestUtils::numberOfTuplesToProducePerBuffer(6),

    });
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"defaultLogicalSourceName\"))";
    ss << R"(.window(TumblingWindow::of(EventTime(Attribute(\"ts\")), Seconds(5))))";
    ss << R"(.apply(Sum(Attribute(\"f1\"))).sink(PrintSinkDescriptor::create());")";
    ss << R"(,"placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit={}", ss.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");
    QueryId queryId = json_return["queryId"].get<uint64_t>();
    NES_INFO("Query ID: {}", queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    //EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));
}

}// namespace NES