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

#include <Plans/Query/QueryId.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>
#define GetCurrentDir getcwd
#include <Util/TestUtils.hpp>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <cstdio>
#include <sstream>

using namespace utility;
// Common utilities like string conversions
using namespace web;
// Common features like URIs.
using namespace web::http;
// Common HTTP functionality
using namespace web::http::client;
// HTTP client features
using namespace concurrency::streams;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 1200;
uint64_t dataPort = 1400;
uint64_t restPort = 8000;
uint16_t timeout = 5;

class E2ECoordinatorMultiQueryTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorMultiQueryTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    void SetUp() override {
        rpcPort += 10;
        dataPort += 10;
        restPort += 10;
    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

/**
 * @brief This test starts two workers and a coordinator and submit the same query but will output the results in different files
 */
TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithFileOutputTwoQueries) {
    NES_INFO(" start coordinator");
    std::string pathQuery1 = "query1.out";
    std::string pathQuery2 = "query2.out";

    remove(pathQuery1.c_str());
    remove(pathQuery2.c_str());

    auto coordinator = TestUtils::startCoordinator(
        {TestUtils::coordinatorPort(rpcPort), TestUtils::restPort(restPort), TestUtils::numberOfSlots(8)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(rpcPort + 3),
                                          TestUtils::dataPort(dataPort),
                                          TestUtils::coordinatorPort(rpcPort),
                                          TestUtils::numberOfSlots(8)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    std::stringstream ssQuery1;
    ssQuery1 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery1 << pathQuery1;
    ssQuery1 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery1 << R"());","strategyName" : "BottomUp"})";
    NES_INFO("string submit for query1=" << ssQuery1.str());

    std::stringstream ssQuery2;
    ssQuery2 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery2 << pathQuery2;
    ssQuery2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery2 << R"());","strategyName" : "BottomUp"})";
    NES_INFO("string submit for query2=" << ssQuery2.str());

    web::json::value jsonReturnQ1 = TestUtils::startQueryViaRest(ssQuery1.str(), std::to_string(restPort));
    NES_INFO("try to acc return Q1=" << jsonReturnQ1);
    QueryId queryId1 = jsonReturnQ1.at("queryId").as_integer();
    NES_INFO("Query ID1: " << queryId1);
    EXPECT_NE(queryId1, INVALID_QUERY_ID);

    web::json::value jsonReturnQ2 = TestUtils::startQueryViaRest(ssQuery2.str(), std::to_string(restPort));
    NES_INFO("try to acc return Q2=" << jsonReturnQ2);

    QueryId queryId2 = jsonReturnQ2.at("queryId").as_integer();
    NES_INFO("Query ID2: " << queryId2);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(restPort)));

    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId2, std::to_string(restPort)));

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

    std::ifstream ifsQ1(pathQuery1.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ1, expectedContent);

    std::ifstream ifsQ2(pathQuery2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ2, expectedContent);
}

/**
 * @brief This test starts two workers and a coordinator and submit too many queries such that we test if the error-prone process
 */
TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithFileOutputThreeQueriesWithErrorTest) {
    NES_INFO(" start coordinator");
    std::string pathQuery1 = "query1.out";
    std::string pathQuery2 = "query2.out";
    std::string pathQuery3 = "query3.out";

    remove(pathQuery1.c_str());
    remove(pathQuery2.c_str());
    remove(pathQuery3.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::coordinatorPort(rpcPort), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    auto worker = TestUtils::startWorker(
        {TestUtils::rpcPort(rpcPort + 3), TestUtils::dataPort(dataPort), TestUtils::coordinatorPort(rpcPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    std::stringstream ssQuery1;
    ssQuery1 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery1 << pathQuery1;
    ssQuery1 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery1 << R"());","strategyName" : "BottomUp"})";
    NES_INFO("string submit for query1=" << ssQuery1.str());

    std::stringstream ssQuery2;
    ssQuery2 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery2 << pathQuery2;
    ssQuery2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery2 << R"());","strategyName" : "BottomUp"})";
    NES_INFO("string submit for query2=" << ssQuery2.str());

    std::stringstream ssQuery3;
    ssQuery3 << R"({"userQuery" : "Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ssQuery3 << pathQuery3;
    ssQuery3 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery3 << R"());","strategyName" : "BottomUp"})";
    NES_INFO("string submit for query3=" << ssQuery3.str());

    web::json::value jsonReturnQ1 = TestUtils::startQueryViaRest(ssQuery1.str(), std::to_string(restPort));
    web::json::value jsonReturnQ2 = TestUtils::startQueryViaRest(ssQuery2.str(), std::to_string(restPort));
    web::json::value jsonReturnQ3 = TestUtils::startQueryViaRest(ssQuery3.str(), std::to_string(restPort));

    QueryId queryId1 = jsonReturnQ1.at("queryId").as_integer();
    QueryId queryId2 = jsonReturnQ2.at("queryId").as_integer();
    QueryId queryId3 = jsonReturnQ3.at("queryId").as_integer();

    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);
    EXPECT_NE(queryId3, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId3, 1, std::to_string(restPort)));

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

    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId2, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId3, std::to_string(restPort)));

    std::ifstream ifsQ1(pathQuery1.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ1, expectedContent);

    std::ifstream ifsQ2(pathQuery2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ2, expectedContent);

    std::ifstream ifsQ3(pathQuery3.c_str());
    EXPECT_TRUE(ifsQ3.good());
    std::string contentQ3((std::istreambuf_iterator<char>(ifsQ3)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q3=" << contentQ3);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ3, expectedContent);
}

/**
 * @brief This test starts two workers and a coordinator and submits two different queries
 */
TEST_F(E2ECoordinatorMultiQueryTest, testTwoQueriesWithFileOutput) {
    NES_INFO(" start coordinator");
    std::string Qpath1 = "QueryQnV1.out";
    std::string Qpath2 = "QueryQnV2.out";
    remove(Qpath1.c_str());
    remove(Qpath2.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::coordinatorPort(rpcPort), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"streamName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(rpcPort + 3),
                                          TestUtils::dataPort(dataPort),
                                          TestUtils::coordinatorPort(rpcPort),
                                          TestUtils::logicalStreamName("QnV"),
                                          TestUtils::sourceType("CSVSource"),
                                          TestUtils::csvSourceFilePath("../tests/test_data/QnV_short.csv"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::sourceFrequency(1000),
                                          TestUtils::physicalStreamName("test_stream")});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    std::stringstream ssQuery1;
    ssQuery1 << "{\"userQuery\" : ";
    ssQuery1 << R"("Query::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\")";
    ssQuery1 << Qpath1;
    ssQuery1 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery1 << R"());","strategyName" : "BottomUp"})";
    ssQuery1 << endl;

    NES_INFO("query1 string submit=" << ssQuery1.str());
    string bodyQuery1 = ssQuery1.str();

    std::stringstream ssQuery2;
    ssQuery2 << "{\"userQuery\" : ";
    ssQuery2 << R"("Query::from(\"QnV\").filter(Attribute(\"quantity\") > 10).sink(FileSinkDescriptor::create(\")";
    ssQuery2 << Qpath2;
    ssQuery2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ssQuery2 << R"());","strategyName" : "BottomUp"})";
    ssQuery2 << endl;

    NES_INFO("query2 string submit=" << ssQuery2.str());
    string bodyQuery2 = ssQuery2.str();

    NES_INFO("send query 1:");
    web::json::value jsonReturnQ1 = TestUtils::startQueryViaRest(ssQuery1.str(), std::to_string(restPort));
    NES_INFO("return from q1");

    NES_INFO("send query 2:");
    web::json::value jsonReturnQ2 = TestUtils::startQueryViaRest(ssQuery2.str(), std::to_string(restPort));
    NES_INFO("return from q2");

    QueryId queryId1 = jsonReturnQ1.at("queryId").as_integer();
    QueryId queryId2 = jsonReturnQ2.at("queryId").as_integer();

    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(restPort)));

    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId2, std::to_string(restPort)));

    string expectedContent1 = "QnV$sensor_id:ArrayType,QnV$timestamp:INTEGER,QnV$velocity:(Float),QnV$quantity:INTEGER\n"
                              "R2000073,1543624020000,102.629631,8\n"
                              "R2000070,1543625280000,108.166664,5\n";

    string expectedContent2 = "QnV$sensor_id:ArrayType,QnV$timestamp:INTEGER,QnV$velocity:(Float),QnV$quantity:INTEGER\n"
                              "R2000073,1543622760000,63.277779,11\n"
                              "R2000073,1543622940000,66.222221,12\n"
                              "R2000073,1543623000000,74.666664,11\n"
                              "R2000073,1543623480000,62.444443,13\n"
                              "R2000073,1543624200000,64.611115,12\n"
                              "R2000073,1543624260000,68.407410,11\n"
                              "R2000073,1543625040000,56.666668,11\n"
                              "R2000073,1543625400000,62.333332,11\n";

    std::ifstream ifsQ1(Qpath1.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent1);
    EXPECT_EQ(contentQ1, expectedContent1);

    std::ifstream ifsQ2(Qpath2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent2);
    EXPECT_EQ(contentQ2, expectedContent2);
}

TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithTumblingWindowFileOutput) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "ValidUserQueryWithTumbWindowFileOutputTestResult.txt";
    std::string outputFilePath2 = "ValidUserQueryWithTumbWindowFileOutputTestResult2.txt";
    remove(outputFilePath.c_str());
    remove(outputFilePath2.c_str());

    string coordinatorRPCPort = std::to_string(rpcPort);

    auto coordinator = TestUtils::startCoordinator({TestUtils::coordinatorPort(rpcPort), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"streamName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(rpcPort + 3),
                                          TestUtils::dataPort(dataPort),
                                          TestUtils::coordinatorPort(rpcPort),
                                          TestUtils::logicalStreamName("window"),
                                          TestUtils::sourceType("CSVSource"),
                                          TestUtils::csvSourceFilePath("../tests/test_data/window.csv"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::sourceFrequency(1000),
                                          TestUtils::physicalStreamName("test_stream")});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\").window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10)))"
          ".byKey(Attribute(\\\"id\\\")).apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query 1 string submit=" << ss.str());

    std::stringstream ss2;
    ss2 << "{\"userQuery\" : ";
    ss2 << "\"Query::from(\\\"window\\\").window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(20)))"
           ".byKey(Attribute(\\\"id\\\")).apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss2 << outputFilePath2;
    ss2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss2 << R"());","strategyName" : "BottomUp"})";
    ss2 << endl;

    NES_INFO("query 2 string submit=" << ss2.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    NES_INFO("return from query 1");

    web::json::value json_return_Q2 = TestUtils::startQueryViaRest(ss2.str(), std::to_string(restPort));
    NES_INFO("return from query 2");

    NES_INFO("try to acc return");
    QueryId queryId1 = json_return.at("queryId").as_integer();
    NES_INFO("Query ID 1: " << queryId1);
    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    QueryId queryId2 = json_return_Q2.at("queryId").as_integer();
    NES_INFO("Query ID 2: " << queryId2);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(restPort)));

    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId2, std::to_string(restPort)));

    string expectedContent1 = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                              "0,10000,1,51\n"
                              "10000,20000,1,145\n"
                              "0,10000,4,1\n"
                              "0,10000,11,5\n"
                              "0,10000,12,1\n"
                              "0,10000,16,2\n";

    string expectedContent2 = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                              "0,20000,1,196\n"
                              "0,20000,4,1\n"
                              "0,20000,11,5\n"
                              "0,20000,12,1\n"
                              "0,20000,16,2\n";

    std::ifstream ifsQ1(outputFilePath.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent1);
    EXPECT_EQ(contentQ1, expectedContent1);

    std::ifstream ifsQ2(outputFilePath2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)), (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent2);
    EXPECT_EQ(contentQ2, expectedContent2);
}

TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithSlidingWindowFileOutput) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "ValidUserQueryWithSlidWindowFileOutputTestResult.txt";
    std::string outputFilePath2 = "ValidUserQueryWithSlidWindowFileOutputTestResult2.txt";
    remove(outputFilePath.c_str());
    remove(outputFilePath2.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::coordinatorPort(rpcPort), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"streamName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(rpcPort + 3),
                                          TestUtils::dataPort(dataPort),
                                          TestUtils::coordinatorPort(rpcPort),
                                          TestUtils::logicalStreamName("window"),
                                          TestUtils::sourceType("CSVSource"),
                                          TestUtils::csvSourceFilePath("../tests/test_data/window.csv"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::sourceFrequency(1000),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::physicalStreamName("test_stream")});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\").window(SlidingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10), "
          "Seconds(5)))"
          ".byKey(Attribute(\\\"id\\\")).apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query 1 string submit=" << ss.str());

    std::stringstream ss2;
    ss2 << "{\"userQuery\" : ";
    ss2 << "\"Query::from(\\\"window\\\").window(SlidingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(20), "
           "Seconds(10)))"
           ".byKey(Attribute(\\\"id\\\")).apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss2 << outputFilePath2;
    ss2 << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss2 << R"());","strategyName" : "BottomUp"})";
    ss2 << endl;

    NES_INFO("query 2 string submit=" << ss2.str());
    string body2 = ss2.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    NES_INFO("return from query 1");

    web::json::value json_return_Q2 = TestUtils::startQueryViaRest(ss2.str(), std::to_string(restPort));
    NES_INFO("return from query 2");

    NES_INFO("try to acc return");
    QueryId queryId1 = json_return.at("queryId").as_integer();
    NES_INFO("Query ID 1: " << queryId1);
    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    QueryId queryId2 = json_return_Q2.at("queryId").as_integer();
    NES_INFO("Query ID 2: " << queryId2);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1, std::to_string(restPort)));

    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId1, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId2, std::to_string(restPort)));

    string expectedContent1 = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                              "0,10000,1,51\n"
                              "0,10000,4,1\n"
                              "0,10000,11,5\n"
                              "0,10000,12,1\n"
                              "0,10000,16,2\n"
                              "5000,15000,1,95\n"
                              "10000,20000,1,145\n";

    string expectedContent2 = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                              "0,20000,1,196\n"
                              "0,20000,4,1\n"
                              "0,20000,11,5\n"
                              "0,20000,12,1\n"
                              "0,20000,16,2\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent2, outputFilePath2));
}

}// namespace NES