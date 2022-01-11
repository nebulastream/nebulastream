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
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <cstdio>
#include <gtest/gtest.h>
#include "../util/NesBaseTest.hpp"
#include <sstream>
#include <string>
#include <unistd.h>

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

namespace NES {

uint16_t timeout = 5u;

class E2ECoordinatorMultiWorkerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

TEST_F(E2ECoordinatorMultiWorkerTest, DISABLED_testExecutingValidUserQueryWithFileOutputTwoWorker) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "ValidUserQueryWithFileOutputTwoWorkerTestResult.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPor), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPor),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalStreamName("default_logical"),
                                           TestUtils::physicalStreamName("test2")});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPor),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalStreamName("default_logical"),
                                           TestUtils::physicalStreamName("test1")});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(*restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
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
                             "1,1\n"
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

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidQueryWithFileOutputTwoWorkerSameSource) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "testExecutingValidQueryWithFileOutputTwoWorker.out";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPor), TestUtils::restPort(restPort)});
    ASSERT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(*restPort)));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short.csv"),
                                           TestUtils::physicalStreamName("test_stream1"),
                                           TestUtils::logicalStreamName("QnV"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(0),
                                           TestUtils::sourceFrequency(1000)});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short.csv"),
                                           TestUtils::physicalStreamName("test_stream2"),
                                           TestUtils::logicalStreamName("QnV"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(0),
                                           TestUtils::sourceFrequency(1000)});

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\")";
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

    string expectedContent = "QnV$sensor_id:ArrayType,QnV$timestamp:INTEGER,QnV$velocity:(Float),QnV$quantity:INTEGER\n"
                             "R2000073,1543624020000,102.629631,8\n"
                             "R2000070,1543625280000,108.166664,5\n"
                             "R2000073,1543624020000,102.629631,8\n"
                             "R2000070,1543625280000,108.166664,5\n";

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    ASSERT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidQueryWithFileOutputTwoWorkerDifferentSource) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "testExecutingValidQueryWithFileOutputTwoWorker.out";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPor), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(*restPort)));

    auto worker1 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPor),
                                TestUtils::sourceType("CSVSource"),
                                TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000073.csv"),
                                TestUtils::physicalStreamName("test_stream1"),
                                TestUtils::logicalStreamName("QnV"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(0),
                                TestUtils::sourceFrequency(1000)});

    auto worker2 =
        TestUtils::startWorker({TestUtils::rpcPort(0),
                                TestUtils::dataPort(0),
                                TestUtils::coordinatorPort(*rpcCoordinatorPor),
                                TestUtils::sourceType("CSVSource"),
                                TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000070.csv"),
                                TestUtils::physicalStreamName("test_stream2"),
                                TestUtils::logicalStreamName("QnV"),
                                TestUtils::numberOfBuffersToProduce(1),
                                TestUtils::numberOfTuplesToProducePerBuffer(0),
                                TestUtils::sourceFrequency(1000)});

    ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(*restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(*restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());

    std::string line;
    bool resultWrk1 = false;
    bool resultWrk2 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        std::vector<string> content = Util::splitWithStringDelimiter<std::string>(line, ",");
        if (content.at(0) == "R2000073") {
            NES_INFO("First content=" << content.at(2));
            NES_INFO("First: expContent= 102.629631");
            if (content.at(2) == "102.629631") {
                resultWrk1 = true;
            }
        } else {
            NES_INFO("Second: content=" << content.at(2));
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
    NES_INFO(" start coordinator");
    std::string outputFilePath = getTestResourceFolder() / "testExecutingValidUserQueryWithTumblingWindowFileOutput.txt";
    remove(outputFilePath.c_str());

    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPor), TestUtils::restPort(restPort)});
    ASSERT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;

    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(*restPort)));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                           TestUtils::physicalStreamName("test_stream_1"),
                                           TestUtils::logicalStreamName("window"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(28),
                                           TestUtils::sourceFrequency(1000)});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPort),
                                           TestUtils::sourceType("CSVSource"),
                                           TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv"),
                                           TestUtils::physicalStreamName("test_stream_2"),
                                           TestUtils::logicalStreamName("window"),
                                           TestUtils::numberOfBuffersToProduce(1),
                                           TestUtils::numberOfTuplesToProducePerBuffer(28),
                                           TestUtils::sourceFrequency(1000)});

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

    int response = remove(outputFilePath.c_str());
    ASSERT_TRUE(response == 0);
}

TEST_F(E2ECoordinatorMultiWorkerTest, DISABLED_testExecutingMonitoringTwoWorker) {
    NES_INFO(" start coordinator");
    auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(*rpcCoordinatorPor), TestUtils::restPort(restPort)});
    ASSERT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    auto worker1 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPor),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalStreamName("default_logical"),
                                           TestUtils::physicalStreamName("test")});

    auto worker2 = TestUtils::startWorker({TestUtils::rpcPort(0),
                                           TestUtils::dataPort(0),
                                           TestUtils::coordinatorPort(*rpcCoordinatorPor),
                                           TestUtils::sourceType("DefaultSource"),
                                           TestUtils::logicalStreamName("default_logical"),
                                           TestUtils::physicalStreamName("test")});
    ASSERT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 2));

    web::json::value json_return;

    web::http::client::http_client client("http://localhost:" + std::to_string(*restPort) + "/v1/nes/monitoring/metrics/");

    client.request(web::http::methods::GET)
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&json_return](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                json_return = task.get();
            } catch (const web::http::http_exception& e) {
                NES_INFO("error while setting return");
                NES_INFO("error " << e.what());
            }
        })
        .wait();

    ASSERT_EQ(json_return.size(), 3ul);
    NES_INFO("RETURN: " << json_return.size());
    NES_INFO("RETURN: " << json_return);

    for (std::size_t i{1UL}; i <= json_return.size(); ++i) {
        auto json = json_return[std::to_string(i)];
        NES_INFO("SUB RETURN: " << json);

        ASSERT_TRUE(json.has_field("disk"));
        ASSERT_EQ(json["disk"].size(), 5U);

        ASSERT_TRUE(json.has_field("cpu"));
        auto numCores = json["cpu"]["NUM_CORES"].as_integer();
        ASSERT_EQ(json["cpu"].size(), numCores + 2U);

        ASSERT_TRUE(json.has_field("network"));
        ASSERT_TRUE(json["network"].size() > 0U);

        ASSERT_TRUE(json.has_field("memory"));
        ASSERT_EQ(json["memory"].size(), 13ul);
    }
}

}// namespace NES