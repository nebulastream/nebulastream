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
#include <Plans/Query/QueryId.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpprest/details/basic_types.h>
#include <cpprest/http_client.h>
#include <gtest/gtest.h>
#include "../util/NesBaseTest.hpp"
#include <sstream>
#include <stdio.h>
#include <string>

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
//#define _XPLATSTR(x) _XPLATSTR(x)
namespace NES {

uint16_t timeout = 5;

class E2ECoordinatorSingleWorkerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorSingleWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithPrintOutput) {

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    auto worker = TestUtils::startWorker(
        {TestUtils::rpcPort(0), TestUtils::dataPort(0), TestUtils::coordinatorPort(*rpcCoordinatorPort),
         TestUtils::sourceType("DefaultSource"),
         TestUtils::logicalStreamName("default_logical"),
         TestUtils::physicalStreamName("test")});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());")";
    ss << R"(,"strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutput) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    auto worker = TestUtils::startWorker(
        {TestUtils::rpcPort(0), TestUtils::dataPort(0), TestUtils::coordinatorPort(*rpcCoordinatorPort),
         TestUtils::sourceType("DefaultSource"),
         TestUtils::logicalStreamName("default_logical"),
         TestUtils::physicalStreamName("test")});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");

    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

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

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputWithFilter) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "UserQueryWithFileOutputWithFilterTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    auto worker = TestUtils::startWorker(
        {TestUtils::rpcPort(0), TestUtils::dataPort(0), TestUtils::coordinatorPort(*rpcCoordinatorPort),
         TestUtils::sourceType("DefaultSource"),
         TestUtils::logicalStreamName("default_logical"),
         TestUtils::physicalStreamName("test")});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").filter(Attribute(\"id\") >= 1).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    // if filter is applied correctly, no output is generated
    NES_INFO("read file=" << outputFilePath);
    ifstream outFile(outputFilePath);
    EXPECT_TRUE(outFile.good());
    std::string content((std::istreambuf_iterator<char>(outFile)), (std::istreambuf_iterator<char>()));
    NES_INFO("content=" << content);
    std::string expected = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
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

    NES_DEBUG("expected=" << expected);
    EXPECT_EQ(expected, content);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputAndRegisterPhyStream) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithFileOutputAndRegisterPhyStreamTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType("DefaultSource"),
                                          TestUtils::physicalStreamName("test_stream"),
                                          TestUtils::logicalStreamName("default_logical"),
                                          TestUtils::numberOfBuffersToProduce(2),
                                          TestUtils::sourceFrequency(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    NES_INFO("try to acc return");

    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

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

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);
    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputExdraUseCase) {
    NES_INFO(" start coordinator");
    std::string testFile = "exdra.csv";
    remove(testFile.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(*restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType("CSVSource"),
                                          TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "exdra.csv"),
                                          TestUtils::physicalStreamName("test_stream"),
                                          TestUtils::logicalStreamName("exdra"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::numberOfTuplesToProducePerBuffer(11),
                                          TestUtils::sourceFrequency(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"exdra\").sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    // XXX:
    string expectedContent =
        "exdra$id:INTEGER,exdra$metadata_generated:INTEGER,exdra$metadata_title:ArrayType,exdra$metadata_id:ArrayType,exdra$"
        "features_type:"
        "ArrayType,exdra$features_properties_capacity:INTEGER,exdra$features_properties_efficiency:(Float),exdra$features_"
        "properties_"
        "mag:(Float),exdra$features_properties_time:INTEGER,exdra$features_properties_updated:INTEGER,exdra$features_properties_"
        "type:ArrayType,exdra$features_geometry_type:ArrayType,exdra$features_geometry_coordinates_longitude:(Float),exdra$"
        "features_"
        "geometry_coordinates_latitude:(Float),exdra$features_eventId :ArrayType\n"
        "1,1262343610000,Wind Turbine Data Generated for Nebula "
        "Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,"
        "Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
        "2,1262343620010,Wind Turbine Data Generated for Nebula "
        "Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,"
        "Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
        "3,1262343630020,Wind Turbine Data Generated for Nebula "
        "Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,"
        "electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
        "4,1262343640030,Wind Turbine Data Generated for Nebula "
        "Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,"
        "Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
        "5,1262343650040,Wind Turbine Data Generated for Nebula "
        "Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,"
        "electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
        "6,1262343660050,Wind Turbine Data Generated for Nebula "
        "Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,"
        "electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
        "7,1262343670060,Wind Turbine Data Generated for Nebula "
        "Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,"
        "electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
        "8,1262343680070,Wind Turbine Data Generated for Nebula "
        "Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,"
        "electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
        "9,1262343690080,Wind Turbine Data Generated for Nebula "
        "Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,"
        "Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
        "10,1262343700090,Wind Turbine Data Generated for Nebula "
        "Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,"
        "Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
        "11,1262343710100,Wind Turbine Data Generated for Nebula "
        "Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,"
        "Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithTumblingWindowFileOutput) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithTumbWindowFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType("CSVSource"),
                                          TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                          TestUtils::physicalStreamName("test_stream"),
                                          TestUtils::logicalStreamName("window"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::sourceFrequency(1)});
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

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

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    // if filter is applied correctly, no output is generated
    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,51\n"
                             "10000,20000,1,145\n"
                             "0,10000,4,1\n"
                             "0,10000,11,5\n"
                             "0,10000,12,1\n"
                             "0,10000,16,2\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithSlidingWindowFileOutput) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithSlidWindowFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPort), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(*restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(0),
                                          TestUtils::dataPort(0),
                                          TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                          TestUtils::sourceType("CSVSource"),
                                          TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                          TestUtils::physicalStreamName("test_stream"),
                                          TestUtils::logicalStreamName("window"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::sourceFrequency(1)});

    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\")"
          ".window(SlidingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10), Seconds(5)))"
          ".byKey(Attribute(\\\"id\\\"))"
          ".apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,51\n"
                             "0,10000,4,1\n"
                             "0,10000,11,5\n"
                             "0,10000,12,1\n"
                             "0,10000,16,2\n"
                             "5000,15000,1,95\n"
                             "10000,20000,1,145\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));
}

}// namespace NES