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

#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>
#define GetCurrentDir getcwd
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <boost/process.hpp>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <cstdio>
#include <sstream>

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

class E2ECoordinatorWorkerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() {
        NES_INFO("Tear down ActorCoordinatorWorkerTest test class.");
    }
};

TEST_F(E2ECoordinatorWorkerTest, testExecutingValidUserQueryWithFileOutputTwoWorker) {
    NES_INFO(" start coordinator");
    std::string outputFilePath =
        "ValidUserQueryWithFileOutputTwoWorkerTestResult.txt";
    remove(outputFilePath.c_str());

    string cmdCoord = "./nesCoordinator --coordinatorPort=12348";
    bp::child coordinatorProc(cmdCoord.c_str());

    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(2);

    string cmdWrk1 = "./nesWorker --coordinatorPort=12348 --rpcPort=12351 --dataPort=12352";
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string cmdWrk2 = "./nesWorker --coordinatorPort=12348 --rpcPort=12353 --dataPort=12354";
    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid1 = workerProc1.id();
    size_t workerPid2 = workerProc2.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://127.0.0.1:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then([](const web::http::http_response& response) {
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

    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|id:UINT32|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|id:UINT32|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "|1|1|\n"
        "+----------------------------------------------------+";
    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    NES_INFO("Killing worker 1 process->PID: " << workerPid1);
    workerProc1.terminate();
    NES_INFO("Killing worker 2 process->PID: " << workerPid2);
    workerProc2.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorWorkerTest, testExecutingValidSimplePatternWithFileOutputTwoWorkerSameSource) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "testExecutingValidSimplePatternWithFileOutputTwoWorker.out";
    remove(outputFilePath.c_str());

    string path = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(path.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);
    size_t coordinatorPid = coordinatorProc.id();

    std::stringstream schema;
    schema << "{\"streamName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    string schemabody = schema.str();

    web::json::value json_returnSchema;

    web::http::client::http_client clientSchema(
        "http://127.0.0.1:8081/v1/nes/streamCatalog/addLogicalStream");
    clientSchema.request(web::http::methods::POST, _XPLATSTR("/"), schemabody).then([](const web::http::http_response& response) {
                                                                                  NES_INFO("get first then");
                                                                                  return response.extract_json();
                                                                              })
        .then([&json_returnSchema](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                json_returnSchema = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("error while setting return");
                NES_ERROR("error " << e.what());
            }
        })
        .wait();

    NES_INFO("try to acc return");
    bool success = json_returnSchema.at("Success").as_bool();
    NES_INFO("RegisteredStream: " << success);
    EXPECT_TRUE(success);

    string path2 = "./nesWorker --coordinatorPort=12346 --rpcPort=12351 --dataPort=12352 --logicalStreamName=QnV --physicalStreamName=test_stream1 --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc1(path2.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());
    size_t workerPid1 = workerProc1.id();
    sleep(1);

    string path3 = "./nesWorker --coordinatorPort=12346 --rpcPort=12353 --dataPort=12354 --logicalStreamName=QnV --physicalStreamName=test_stream2 --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc2(path3.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());
    size_t workerPid2 = workerProc2.id();
    sleep(1);

    std::stringstream ss;
    ss << "{\"pattern\" : ";
    ss << "\"Pattern::from(\\\"QnV\\\").filter(Attribute(\\\"velocity\\\") > 100).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://127.0.0.1:8081/v1/nes/pattern/execute-pattern");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then([](const web::http::http_response& response) {
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

    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|sensor_id:CHAR|timestamp:UINT64|velocity:FLOAT32|quantity:UINT64|PatternId:INT32|\n"
        "+----------------------------------------------------+\n"
        "|R2000073|1543624020000|102.629631|8|1|\n"
        "|R2000070|1543625280000|108.166664|5|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|sensor_id:CHAR|timestamp:UINT64|velocity:FLOAT32|quantity:UINT64|PatternId:INT32|\n"
        "+----------------------------------------------------+\n"
        "|R2000073|1543624020000|102.629631|8|1|\n"
        "|R2000070|1543625280000|108.166664|5|1|\n"
        "+----------------------------------------------------+";

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    NES_INFO("Killing worker 1 process->PID: " << workerPid1);
    workerProc1.terminate();
    NES_INFO("Killing worker 2 process->PID: " << workerPid2);
    workerProc2.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorWorkerTest, testExecutingValidSimplePatternWithFileOutputTwoWorkerDifferentSource) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "testExecutingValidSimplePatternWithFileOutputTwoWorker.out";
    remove(outputFilePath.c_str());

    string path = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(path.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);
    size_t coordinatorPid = coordinatorProc.id();

    std::stringstream schema;
    schema << "{\"streamName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    string schemabody = schema.str();

    web::json::value json_returnSchema;

    web::http::client::http_client clientSchema(
        "http://127.0.0.1:8081/v1/nes/streamCatalog/addLogicalStream");
    clientSchema.request(web::http::methods::POST, _XPLATSTR("/"), schemabody).then([](const web::http::http_response& response) {
                                                                                  NES_INFO("get first then");
                                                                                  return response.extract_json();
                                                                              })
        .then([&json_returnSchema](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                json_returnSchema = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("error while setting return");
                NES_ERROR("error " << e.what());
            }
        })
        .wait();

    NES_INFO("try to acc return");
    bool success = json_returnSchema.at("Success").as_bool();
    NES_INFO("RegisteredStream: " << success);
    EXPECT_TRUE(success);

    string path2 = "./nesWorker --coordinatorPort=12346 --rpcPort=12351 --dataPort=12352 --logicalStreamName=QnV --physicalStreamName=test_stream1 --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short_R2000073.csv --numberOfBuffersToProduce=1 --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc1(path2.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());
    size_t workerPid1 = workerProc1.id();
    sleep(1);

    string path3 = "./nesWorker --coordinatorPort=12346 --rpcPort=12353 --dataPort=12354 --logicalStreamName=QnV --physicalStreamName=test_stream2 --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short_R2000070.csv --numberOfBuffersToProduce=1 --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc2(path3.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());
    size_t workerPid2 = workerProc2.id();
    sleep(1);

    std::stringstream ss;
    ss << "{\"pattern\" : ";
    ss << "\"Pattern::from(\\\"QnV\\\").filter(Attribute(\\\"velocity\\\") > 100).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://127.0.0.1:8081/v1/nes/pattern/execute-pattern");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then([](const web::http::http_response& response) {
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

    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string line;
    int rowNumber = 0;
    bool resultWrk1 = false;
    bool resultWrk2 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        rowNumber++;
        if ((rowNumber > 3 && rowNumber < 6) || (rowNumber > 8 && rowNumber < 11)) {
            std::vector<string> content = UtilityFunctions::split(line, '|');
            if (content.at(1) == "R2000073") {
                NES_INFO("E2ECoordinatorWorkerTest(testExecutingValidSimplePatternWithFileOutputTwoWorkerDifferentSource): content=" << content.at(2));
                NES_INFO("E2ECoordinatorWorkerTest(testExecutingValidSimplePatternWithFileOutputTwoWorkerDifferentSource): expContent= 102.629631");
                EXPECT_EQ(content.at(3), "102.629631");
                resultWrk1 = true;
            } else {
                NES_INFO("E2ECoordinatorWorkerTest(testExecutingValidSimplePatternWithFileOutputTwoWorkerDifferentSource): content=" << content.at(2));
                NES_INFO("E2ECoordinatorWorkerTest(testExecutingValidSimplePatternWithFileOutputTwoWorkerDifferentSource): expContent= 108.166664");
                EXPECT_EQ(content.at(3), "108.166664");
                resultWrk2 = true;
            }
        }
    }

    EXPECT_TRUE((resultWrk1 && resultWrk2));

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    NES_INFO("Killing worker 1 process->PID: " << workerPid1);
    workerProc1.terminate();
    NES_INFO("Killing worker 2 process->PID: " << workerPid2);
    workerProc2.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorWorkerTest, DISABLED_testExecutingValidUserQueryWithTumblingWindowFileOutput) {
    //TODO result content does not end up in file?
    NES_INFO(" start coordinator");
    std::string outputFilePath =
        "testExecutingValidUserQueryWithTumblingWindowFileOutput.txt";
    remove(outputFilePath.c_str());

    string cmdCoord = "./nesCoordinator --coordinatorPort=12348";
    bp::child coordinatorProc(cmdCoord.c_str());

    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(2);

    std::stringstream schema;
    schema << "{\"streamName\" : \"window\",\"schema\" :\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    string schemabody = schema.str();

    web::json::value json_returnSchema;

    web::http::client::http_client clientSchema(
        "http://127.0.0.1:8081/v1/nes/streamCatalog/addLogicalStream");
    clientSchema.request(web::http::methods::POST, _XPLATSTR("/"), schemabody).then([](const web::http::http_response& response) {
                                                                                  NES_INFO("get first then");
                                                                                  return response.extract_json();
                                                                              })
        .then([&json_returnSchema](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                json_returnSchema = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("error while setting return");
                NES_ERROR("error " << e.what());
            }
        })
        .wait();

    NES_INFO("try to acc return");
    bool success = json_returnSchema.at("Success").as_bool();
    NES_INFO("RegisteredStream: " << success);
    EXPECT_TRUE(success);

    string cmdWrk1 = "./nesWorker --coordinatorPort=12348 --rpcPort=12351 --dataPort=12352 --logicalStreamName=window --physicalStreamName=test_stream_1 --sourceType=CSVSource --sourceConfig=../tests/test_data/window.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string cmdWrk2 = "./nesWorker --coordinatorPort=12348 --rpcPort=12353 --dataPort=12354 --logicalStreamName=window --physicalStreamName=test_stream_2 --sourceType=CSVSource --sourceConfig=../tests/test_data/window.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid1 = workerProc1.id();
    size_t workerPid2 = workerProc2.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\").windowByKey(Attribute(\\\"id\\\"), TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10)), Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://127.0.0.1:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then([](const web::http::http_response& response) {
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

    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:INT64|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|0|10000|1|307|\n"
        "|10000|20000|1|870|\n"
        "|0|10000|4|6|\n"
        "|10000|20000|4|0|\n"
        "|0|10000|11|30|\n"
        "|10000|20000|11|0|\n"
        "|0|10000|12|7|\n"
        "|10000|20000|12|0|\n"
        "|0|10000|16|12|\n"
        "|10000|20000|16|0|\n"
        "+----------------------------------------------------+";

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    NES_INFO("Killing worker 1 process->PID: " << workerPid1);
    workerProc1.terminate();
    NES_INFO("Killing worker 2 process->PID: " << workerPid2);
    workerProc2.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorWorkerTest, DISABLED_testExecutingMonitoringTwoWorker) {
    NES_INFO(" start coordinator");
    string cmdCoord = "./nesCoordinator --coordinatorPort=12348";
    bp::child coordinatorProc(cmdCoord.c_str());

    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(2);

    string cmdWrk1 = "./nesWorker --coordinatorPort=12348 --rpcPort=12351 --dataPort=12352";
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string cmdWrk2 = "./nesWorker --coordinatorPort=12348 --rpcPort=12353 --dataPort=12354";
    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid1 = workerProc1.id();
    size_t workerPid2 = workerProc2.id();

    web::json::value json_return;

    web::http::client::http_client client("http://localhost:8081/v1/nes/monitoring/metrics/");

    client.request(web::http::methods::GET).then([](const web::http::http_response& response) {
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

    EXPECT_EQ(json_return.size(), 3);
    NES_INFO("RETURN: " << json_return.size());
    NES_INFO("RETURN: " << json_return);

    std::string v1S = json_return.at("1").at("schema").as_string();
    std::string v1T = json_return.at("1").at("tupleBuffer").as_string();
    std::string v2S = json_return.at("2").at("schema").as_string();
    std::string v2T = json_return.at("2").at("tupleBuffer").as_string();
    std::string v3S = json_return.at("3").at("schema").as_string();
    std::string v3T = json_return.at("3").at("tupleBuffer").as_string();

    EXPECT_TRUE(!v1S.empty());
    EXPECT_TRUE(!v1T.empty());
    EXPECT_TRUE(!v2S.empty());
    EXPECT_TRUE(!v2T.empty());
    EXPECT_TRUE(!v3S.empty());
    EXPECT_TRUE(!v3T.empty());

    NES_INFO("Killing worker 1 process->PID: " << workerPid1);
    workerProc1.terminate();
    NES_INFO("Killing worker 2 process->PID: " << workerPid2);
    workerProc2.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorWorkerTest, testExecutingYSBQueryWithFileOutputTwoWorker) {
    size_t numBuffers = 1;
    size_t numTuples = 10;
    size_t expectedBuffers = 2;
    size_t expectedLinesOut = 26;

    NES_INFO(" start coordinator");
    std::string outputFilePath = "YSBQueryWithFileOutputTwoWorkerTestResult.txt";
    remove(outputFilePath.c_str());

    string cmdCoord = "./nesCoordinator --coordinatorPort=12348";
    bp::child coordinatorProc(cmdCoord.c_str());

    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(2);

    string cmdWrk1 = "./nesWorker --coordinatorPort=12348 --rpcPort=12351 --dataPort=12352 --logicalStreamName=ysb --physicalStreamName=ysb1 --sourceType=YSBSource --numberOfBuffersToProduce="
        + std::to_string(numBuffers) + " --numberOfTuplesToProducePerBuffer=" + std::to_string(numTuples) + " --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string cmdWrk2 = "./nesWorker --coordinatorPort=12348 --rpcPort=12353 --dataPort=12354 --logicalStreamName=ysb --physicalStreamName=ysb2 --sourceType=YSBSource --numberOfBuffersToProduce="
        + std::to_string(numBuffers) + " --numberOfTuplesToProducePerBuffer=" + std::to_string(numTuples) + " --sourceFrequency=1 --endlessRepeat=on";

    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid1 = workerProc1.id();
    size_t workerPid2 = workerProc2.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"ysb\\\").sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://127.0.0.1:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then([](const web::http::http_response& response) {
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

    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, expectedBuffers));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    NES_INFO("ContinuousSourceTest: content=" << content);
    EXPECT_TRUE(!content.empty());

    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_INFO("Number of lines " << n);
    EXPECT_TRUE(n == expectedLinesOut);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    NES_INFO("Killing worker 1 process->PID: " << workerPid1);
    workerProc1.terminate();
    NES_INFO("Killing worker 2 process->PID: " << workerPid2);
    workerProc2.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

}// namespace NES