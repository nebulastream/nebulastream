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
#include <Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cstdio>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <unistd.h>

#define GetCurrentDir getcwd

namespace NES {

class E2ECoordinatorMultiQueryTest : public Testing::BaseIntegrationTest {
  public:
    uint16_t timeout = 5;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("E2ECoordinatorMultiQueryTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

/**
 * @brief This test starts two workers and a coordinator and submit the same query but will output the results in different files
 */
TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithFileOutputTwoQueries) {
    NES_INFO("start coordinator");
    std::string pathQuery1 = getTestResourceFolder() / "query1.out";
    std::string pathQuery2 = getTestResourceFolder() / "query2.out";

    remove(pathQuery1.c_str());
    remove(pathQuery2.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableDebug(),
                                                    TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::numberOfSlots(8, true)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::enableNautilusWorker(),
                                          TestUtils::enableDebug(),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::numberOfSlots(8),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test")});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ssQuery1;
    ssQuery1 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery1 << pathQuery1;
    ssQuery1 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery1 << R"());","placement" : "BottomUp"})";
    NES_INFO("string submit for query1={}", ssQuery1.str());

    std::stringstream ssQuery2;
    ssQuery2 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery2 << pathQuery2;
    ssQuery2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery2 << R"());","placement" : "BottomUp"})";
    NES_INFO("string submit for query2={}", ssQuery2.str());

    nlohmann::json jsonReturnQ1 = TestUtils::startQueryViaRest(ssQuery1.str(), std::to_string(*restPort));
    NES_INFO("try to acc return Q1={}", jsonReturnQ1);
    QueryId queryId1 = jsonReturnQ1["queryId"].get<int>();
    NES_INFO("Query ID1: {}", queryId1);
    ASSERT_NE(queryId1, INVALID_QUERY_ID);

    nlohmann::json jsonReturnQ2 = TestUtils::startQueryViaRest(ssQuery2.str(), std::to_string(*restPort));
    NES_INFO("try to acc return Q2={}", jsonReturnQ2);

    QueryId queryId2 = jsonReturnQ2["queryId"].get<int>();
    NES_INFO("Query ID2: {}", queryId2);
    ASSERT_NE(queryId2, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(*restPort)));

    string ASSERTedContent = "default_logical$id:INTEGER(32 bits),default_logical$value:INTEGER(64 bits)\n"
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

    std::ifstream ifsQ1(pathQuery1.c_str());
    ASSERT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1={}", contentQ1);
    NES_INFO("expContent={}", ASSERTedContent);
    ASSERT_EQ(contentQ1, ASSERTedContent);

    std::ifstream ifsQ2(pathQuery2.c_str());
    ASSERT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2={}", contentQ2);
    NES_INFO("expContent={}", ASSERTedContent);
    ASSERT_EQ(contentQ2, ASSERTedContent);
}

/**
 * @brief This test starts two workers and a coordinator and submit too many queryIdAndCatalogEntryMapping such that we test if the error-prone process
 */
TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithFileOutputThreeQueriesWithErrorTest) {
    NES_INFO("start coordinator");
    std::string pathQuery1 = getTestResourceFolder() / "query1.out";
    std::string pathQuery2 = getTestResourceFolder() / "query2.out";
    std::string pathQuery3 = getTestResourceFolder() / "query3.out";

    remove(pathQuery1.c_str());
    remove(pathQuery2.c_str());
    remove(pathQuery3.c_str());

    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::enableNautilusCoordinator()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::enableNautilusWorker(),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType(SourceType::DEFAULT_SOURCE),
                                          TestUtils::logicalSourceName("default_logical"),
                                          TestUtils::physicalSourceName("test")});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ssQuery1;
    ssQuery1 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery1 << pathQuery1;
    ssQuery1 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery1 << R"());","placement" : "BottomUp"})";
    NES_INFO("string submit for query1={}", ssQuery1.str());

    std::stringstream ssQuery2;
    ssQuery2 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery2 << pathQuery2;
    ssQuery2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery2 << R"());","placement" : "BottomUp"})";
    NES_INFO("string submit for query2={}", ssQuery2.str());

    std::stringstream ssQuery3;
    ssQuery3 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery3 << pathQuery3;
    ssQuery3 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery3 << R"());","placement" : "BottomUp"})";
    NES_INFO("string submit for query3={}", ssQuery3.str());

    nlohmann::json jsonReturnQ1 = TestUtils::startQueryViaRest(ssQuery1.str(), std::to_string(*restPort));
    nlohmann::json jsonReturnQ2 = TestUtils::startQueryViaRest(ssQuery2.str(), std::to_string(*restPort));
    nlohmann::json jsonReturnQ3 = TestUtils::startQueryViaRest(ssQuery3.str(), std::to_string(*restPort));

    QueryId queryId1 = jsonReturnQ1["queryId"].get<int>();
    QueryId queryId2 = jsonReturnQ2["queryId"].get<int>();
    QueryId queryId3 = jsonReturnQ3["queryId"].get<int>();

    ASSERT_NE(queryId1, INVALID_QUERY_ID);
    ASSERT_NE(queryId2, INVALID_QUERY_ID);
    ASSERT_NE(queryId3, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId3, 1, std::to_string(*restPort)));

    string ASSERTedContent = "default_logical$id:INTEGER(32 bits),default_logical$value:INTEGER(64 bits)\n"
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

    std::ifstream ifsQ1(pathQuery1.c_str());
    ASSERT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1={}", contentQ1);
    NES_INFO("expContent={}", ASSERTedContent);
    ASSERT_EQ(contentQ1, ASSERTedContent);

    std::ifstream ifsQ2(pathQuery2.c_str());
    ASSERT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2={}", contentQ2);
    NES_INFO("expContent={}", ASSERTedContent);
    ASSERT_EQ(contentQ2, ASSERTedContent);

    std::ifstream ifsQ3(pathQuery3.c_str());
    ASSERT_TRUE(ifsQ3.good());
    std::string contentQ3((std::istreambuf_iterator<char>(ifsQ3)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q3={}", contentQ3);
    NES_INFO("expContent={}", ASSERTedContent);
    ASSERT_EQ(contentQ3, ASSERTedContent);
}

/**
 * @brief This test starts two workers and a coordinator and submits two different queryIdAndCatalogEntryMapping
 */
TEST_F(E2ECoordinatorMultiQueryTest, testTwoQueriesWithFileOutput) {
    NES_INFO("start coordinator");
    std::string Qpath1 = getTestResourceFolder() / "QueryQnV1.out";
    std::string Qpath2 = getTestResourceFolder() / "QueryQnV2.out";
    remove(Qpath1.c_str());
    remove(Qpath2.c_str());

    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort), TestUtils::enableNautilusCoordinator()});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createText())->addField(createField(\\\"timestamp\\\", "
              "BasicType::UINT64))->addField(createField(\\\"velocity\\\", "
              "BasicType::FLOAT32))->addField(createField(\\\"quantity\\\", BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::enableNautilusWorker(),
                                TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                TestUtils::logicalSourceName("QnV"),
                                TestUtils::sourceType(SourceType::CSV_SOURCE),
                                TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short.csv"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(0),
                                TestUtils::sourceGatheringInterval(1000),
                                TestUtils::physicalSourceName("test_stream")});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ssQuery1;
    ssQuery1 << "{\"userQuery\" : ";
    ssQuery1 << R"("Query::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\")";
    ssQuery1 << Qpath1;
    ssQuery1 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery1 << R"());","placement" : "BottomUp"})";
    ssQuery1 << endl;

    NES_INFO("query1 string submit={}", ssQuery1.str());
    string bodyQuery1 = ssQuery1.str();

    std::stringstream ssQuery2;
    ssQuery2 << "{\"userQuery\" : ";
    ssQuery2 << R"("Query::from(\"QnV\").filter(Attribute(\"quantity\") > 10).sink(FileSinkDescriptor::create(\")";
    ssQuery2 << Qpath2;
    ssQuery2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery2 << R"());","placement" : "BottomUp"})";
    ssQuery2 << endl;

    NES_INFO("query2 string submit={}", ssQuery2.str());
    string bodyQuery2 = ssQuery2.str();

    NES_INFO("send query 1:");
    nlohmann::json jsonReturnQ1 = TestUtils::startQueryViaRest(ssQuery1.str(), std::to_string(*restPort));
    NES_INFO("return from q1");

    NES_INFO("send query 2:");
    nlohmann::json jsonReturnQ2 = TestUtils::startQueryViaRest(ssQuery2.str(), std::to_string(*restPort));
    NES_INFO("return from q2");

    QueryId queryId1 = jsonReturnQ1["queryId"].get<int>();
    QueryId queryId2 = jsonReturnQ2["queryId"].get<int>();

    ASSERT_NE(queryId1, INVALID_QUERY_ID);
    ASSERT_NE(queryId2, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(*restPort)));

    string ASSERTedContent1 =
        "QnV$sensor_id:Text,QnV$timestamp:INTEGER(64 bits),QnV$velocity:Float(32 bits),QnV$quantity:INTEGER(64 bits)\n"
        "R2000073,1543624020000,102.629631,8\n"
        "R2000070,1543625280000,108.166664,5\n";

    string ASSERTedContent2 =
        "QnV$sensor_id:Text,QnV$timestamp:INTEGER(64 bits),QnV$velocity:Float(32 bits),QnV$quantity:INTEGER(64 bits)\n"
        "R2000073,1543622760000,63.277779,11\n"
        "R2000073,1543622940000,66.222221,12\n"
        "R2000073,1543623000000,74.666664,11\n"
        "R2000073,1543623480000,62.444443,13\n"
        "R2000073,1543624200000,64.611115,12\n"
        "R2000073,1543624260000,68.407410,11\n"
        "R2000073,1543625040000,56.666668,11\n"
        "R2000073,1543625400000,62.333332,11\n";

    std::ifstream ifsQ1(Qpath1.c_str());
    ASSERT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1={}", contentQ1);
    NES_INFO("expContent={}", ASSERTedContent1);
    ASSERT_EQ(contentQ1, ASSERTedContent1);

    std::ifstream ifsQ2(Qpath2.c_str());
    ASSERT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2={}", contentQ2);
    NES_INFO("expContent={}", ASSERTedContent2);
    ASSERT_EQ(contentQ2, ASSERTedContent2);
}

TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithTumblingWindowFileOutput) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithTumbWindowFileOutputTestResult.txt";
    std::string outputFilePath2 = getTestResourceFolder() / "ValidUserQueryWithTumbWindowFileOutputTestResult2.txt";
    remove(outputFilePath.c_str());
    remove(outputFilePath2.c_str());

    string coordinatorRPCPort = std::to_string(*rpcCoordinatorPort);

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::setDistributedWindowChildThreshold(100),
                                                    TestUtils::setDistributedWindowCombinerThreshold(0)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",BasicType::UINT64))"
              "->addField(createField(\\\"id\\\",BasicType::UINT64))"
              "->addField(createField(\\\"timestamp\\\",BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::logicalSourceName("window"),
                                          TestUtils::sourceType(SourceType::CSV_SOURCE),
                                          TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::sourceGatheringInterval(1000),
                                          TestUtils::physicalSourceName("test_stream")});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\").window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10)))"
          ".byKey(Attribute(\\\"id\\\")).apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;

    NES_INFO("query 1 string submit={}", ss.str());

    std::stringstream ss2;
    ss2 << "{\"userQuery\" : ";
    ss2 << "\"Query::from(\\\"window\\\").window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(20)))"
           ".byKey(Attribute(\\\"id\\\")).apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss2 << outputFilePath2;
    ss2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss2 << R"());","placement" : "BottomUp"})";
    ss2 << endl;

    NES_INFO("query 2 string submit={}", ss2.str());

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("return from query 1");

    nlohmann::json json_return_Q2 = TestUtils::startQueryViaRest(ss2.str(), std::to_string(*restPort));
    NES_INFO("return from query 2");

    NES_INFO("try to acc return");
    QueryId queryId1 = json_return["queryId"].get<int>();
    NES_INFO("Query ID 1: {}", queryId1);
    ASSERT_NE(queryId1, INVALID_QUERY_ID);
    QueryId queryId2 = json_return_Q2["queryId"].get<int>();
    NES_INFO("Query ID 2: {}", queryId2);
    ASSERT_NE(queryId2, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(*restPort)));

    string ASSERTedContent1 = "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),"
                              "window$value:INTEGER(64 bits)\n"
                              "0,10000,1,51\n"
                              "10000,20000,1,145\n"
                              "0,10000,4,1\n"
                              "0,10000,11,5\n"
                              "0,10000,12,1\n"
                              "0,10000,16,2\n";

    string ASSERTedContent2 = "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),"
                              "window$value:INTEGER(64 bits)\n"
                              "0,20000,1,196\n"
                              "0,20000,4,1\n"
                              "0,20000,11,5\n"
                              "0,20000,12,1\n"
                              "0,20000,16,2\n";

    std::ifstream ifsQ1(outputFilePath.c_str());
    ASSERT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1={}", contentQ1);
    NES_INFO("expContent={}", ASSERTedContent1);
    ASSERT_EQ(contentQ1, ASSERTedContent1);

    std::ifstream ifsQ2(outputFilePath2.c_str());
    ASSERT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2={}", contentQ2);
    NES_INFO("expContent={}", ASSERTedContent2);
    ASSERT_EQ(contentQ2, ASSERTedContent2);
}

TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithSlidingWindowFileOutput) {
    NES_INFO("start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithSlidWindowFileOutputTestResult.txt";
    std::string outputFilePath2 = getTestResourceFolder() / "ValidUserQueryWithSlidWindowFileOutputTestResult2.txt";
    remove(outputFilePath.c_str());
    remove(outputFilePath2.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort),
                                                    TestUtils::restPort(*restPort),
                                                    TestUtils::enableNautilusCoordinator(),
                                                    TestUtils::setDistributedWindowChildThreshold(100),
                                                    TestUtils::setDistributedWindowCombinerThreshold(0)});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",BasicType::UINT64))"
              "->addField(createField(\\\"id\\\",BasicType::UINT64))"
              "->addField(createField(\\\"timestamp\\\",BasicType::UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit={}", schema.str());
    ASSERT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::enableNautilusWorker(),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::logicalSourceName("window"),
                                          TestUtils::sourceType(SourceType::CSV_SOURCE),
                                          TestUtils::csvSourceFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::sourceGatheringInterval(1000),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::physicalSourceName("test_stream")});
    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\").window(SlidingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10), "
          "Seconds(5)))"
          ".byKey(Attribute(\\\"id\\\")).apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;

    NES_INFO("query 1 string submit={}", ss.str());

    std::stringstream ss2;
    ss2 << "{\"userQuery\" : ";
    ss2 << "\"Query::from(\\\"window\\\").window(SlidingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(20), "
           "Seconds(10)))"
           ".byKey(Attribute(\\\"id\\\")).apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss2 << outputFilePath2;
    ss2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss2 << R"());","placement" : "BottomUp"})";
    ss2 << endl;

    NES_INFO("query 2 string submit={}", ss2.str());
    string body2 = ss2.str();

    nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("return from query 1");

    nlohmann::json json_return_Q2 = TestUtils::startQueryViaRest(ss2.str(), std::to_string(*restPort));
    NES_INFO("return from query 2");

    NES_INFO("try to acc return");
    QueryId queryId1 = json_return["queryId"].get<int>();
    NES_INFO("Query ID 1: {}", queryId1);
    ASSERT_NE(queryId1, INVALID_QUERY_ID);
    QueryId queryId2 = json_return_Q2["queryId"].get<int>();
    NES_INFO("Query ID 2: {}", queryId2);
    ASSERT_NE(queryId2, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(*restPort)));

    string ASSERTedContent1 = "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),"
                              "window$value:INTEGER(64 bits)\n"
                              "0,10000,1,51\n"
                              "0,10000,4,1\n"
                              "0,10000,11,5\n"
                              "0,10000,12,1\n"
                              "0,10000,16,2\n"
                              "5000,15000,1,95\n"
                              "10000,20000,1,145\n";

    string ASSERTedContent2 = "window$start:INTEGER(64 bits),window$end:INTEGER(64 bits),window$id:INTEGER(64 bits),"
                              "window$value:INTEGER(64 bits)\n"
                              "0,20000,1,196\n"
                              "0,20000,4,1\n"
                              "0,20000,11,5\n"
                              "0,20000,12,1\n"
                              "0,20000,16,2\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(ASSERTedContent1, outputFilePath));
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(ASSERTedContent2, outputFilePath2));
}

}// namespace NES