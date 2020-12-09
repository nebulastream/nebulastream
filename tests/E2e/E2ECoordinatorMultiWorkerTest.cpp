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

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 1200;
uint64_t dataPort = 1400;
uint64_t restPort = 8000;

class E2ECoordinatorMultiWorkerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    void SetUp() {
        rpcPort += 10;
        dataPort += 10;
        restPort += 10;
    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidUserQueryWithFileOutputTwoWorker) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "ValidUserQueryWithFileOutputTwoWorkerTestResult.txt";
    remove(outputFilePath.c_str());

    string coordinatorRPCPort = std::to_string(rpcPort);
    string cmdCoord = "./nesCoordinator --coordinatorPort=" + coordinatorRPCPort + " --restPort=" + std::to_string(restPort);
    bp::child coordinatorProc(cmdCoord.c_str());

    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(2);

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string cmdWrk1 =
        "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort + " --dataPort=" + worker1DataPort;
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string cmdWrk2 =
        "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort + " --dataPort=" + worker2DataPort;
    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    uint64_t coordinatorPid = coordinatorProc.id();
    uint64_t workerPid1 = workerProc1.id();
    uint64_t workerPid2 = workerProc2.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"";
    ss << "));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));

    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "id:INTEGER,value:INTEGER\n"
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

    NES_INFO("Killing worker 1 process->PID: " << workerPid1);
    workerProc1.terminate();
    NES_INFO("Killing worker 2 process->PID: " << workerPid2);
    workerProc2.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidSimplePatternWithFileOutputTwoWorkerSameSource) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "testExecutingValidSimplePatternWithFileOutputTwoWorker.out";
    remove(outputFilePath.c_str());

    string coordinatorRPCPort = std::to_string(rpcPort);
    string path = "./nesCoordinator --coordinatorPort=" + coordinatorRPCPort + " --restPort=" + std::to_string(restPort);
    bp::child coordinatorProc(path.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);
    uint64_t coordinatorPid = coordinatorProc.id();

    std::stringstream schema;
    schema << "{\"streamName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string path2 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort
        + " --dataPort=" + worker1DataPort
        + " --logicalStreamName=QnV --physicalStreamName=test_stream1 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc1(path2.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());
    uint64_t workerPid1 = workerProc1.id();
    sleep(1);

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string path3 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort
        + " --dataPort=" + worker2DataPort
        + " --logicalStreamName=QnV --physicalStreamName=test_stream2 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc2(path3.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());
    uint64_t workerPid2 = workerProc2.id();
    sleep(1);

    std::stringstream ss;
    ss << "{\"pattern\" : ";
    ss << "\"Pattern::from(\\\"QnV\\\").filter(Attribute(\\\"velocity\\\") > 100).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"";
    ss << "));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "sensor_id:Char,timestamp:INTEGER,velocity:(Float),quantity:INTEGER,PatternId:INTEGER\n"
                             "R2000073,1543624020000,102.629631,8,1\n"
                             "R2000070,1543625280000,108.166664,5,1\n"
                             "R2000073,1543624020000,102.629631,8,1\n"
                             "R2000070,1543625280000,108.166664,5,1\n";

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

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidSimplePatternWithFileOutputTwoWorkerDifferentSource) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "testExecutingValidSimplePatternWithFileOutputTwoWorker.out";
    remove(outputFilePath.c_str());

    string coordinatorRPCPort = std::to_string(rpcPort);
    string path = "./nesCoordinator --coordinatorPort=" + coordinatorRPCPort + " --restPort=" + std::to_string(restPort);
    bp::child coordinatorProc(path.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);
    uint64_t coordinatorPid = coordinatorProc.id();

    std::stringstream schema;
    schema << "{\"streamName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string path2 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort
        + " --dataPort=" + worker1DataPort
        + " --logicalStreamName=QnV --physicalStreamName=test_stream1 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/QnV_short_R2000073.csv "
          "--numberOfBuffersToProduce=1 --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc1(path2.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());
    uint64_t workerPid1 = workerProc1.id();
    sleep(1);

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string path3 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort
        + " --dataPort=" + worker2DataPort
        + " --logicalStreamName=QnV --physicalStreamName=test_stream2 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/QnV_short_R2000070.csv "
          "--numberOfBuffersToProduce=1 --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc2(path3.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());
    uint64_t workerPid2 = workerProc2.id();
    sleep(1);

    std::stringstream ss;
    ss << "{\"pattern\" : ";
    ss << "\"Pattern::from(\\\"QnV\\\").filter(Attribute(\\\"velocity\\\") > 100).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"";
    ss << "));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string line;
    bool resultWrk1 = false;
    bool resultWrk2 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        std::vector<string> content = UtilityFunctions::split(line, ',');
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

TEST_F(E2ECoordinatorMultiWorkerTest, testExecutingValidUserQueryWithTumblingWindowFileOutput) {
    //TODO result content does not end up in file?
    NES_INFO(" start coordinator");
    std::string outputFilePath = "testExecutingValidUserQueryWithTumblingWindowFileOutput.txt";
    remove(outputFilePath.c_str());

    string coordinatorRPCPort = std::to_string(rpcPort);
    string cmdCoord = "./nesCoordinator --coordinatorPort=" + coordinatorRPCPort + " --restPort=" + std::to_string(restPort);
    bp::child coordinatorProc(cmdCoord.c_str());

    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(2);

    std::stringstream schema;
    schema << "{\"streamName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    ASSERT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string cmdWrk1 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort
        + " --dataPort=" + worker1DataPort
        + " --logicalStreamName=window --physicalStreamName=test_stream_1 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/window.csv "
          "--numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string cmdWrk2 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort
        + " --dataPort=" + worker2DataPort
        + " --logicalStreamName=window "
          "--physicalStreamName=test_stream_2 --sourceType=CSVSource --sourceConfig=../tests/test_data/window.csv "
          "--numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    uint64_t coordinatorPid = coordinatorProc.id();
    uint64_t workerPid1 = workerProc1.id();
    uint64_t workerPid2 = workerProc2.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\").windowByKey(Attribute(\\\"id\\\"), "
          "TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10)), "
          "Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"";
    ss << "));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 3, std::to_string(restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
                             "0,10000,1,102\n"
                             "10000,20000,1,290\n"
                             "0,10000,4,2\n"
                             "0,10000,11,10\n"
                             "0,10000,12,2\n"
                             "0,10000,16,4\n";

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

TEST_F(E2ECoordinatorMultiWorkerTest, DISABLED_testExecutingMonitoringTwoWorker) {
    NES_INFO(" start coordinator");
    string coordinatorRPCPort = std::to_string(rpcPort);
    string cmdCoord = "./nesCoordinator --coordinatorPort=" + coordinatorRPCPort + " --restPort=" + std::to_string(restPort);
    bp::child coordinatorProc(cmdCoord.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(2);

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string cmdWrk1 =
        "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort + " --dataPort=" + worker1DataPort;
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string cmdWrk2 =
        "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort + " --dataPort=" + worker2DataPort;
    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    uint64_t coordinatorPid = coordinatorProc.id();
    uint64_t workerPid1 = workerProc1.id();
    uint64_t workerPid2 = workerProc2.id();

    web::json::value json_return;

    web::http::client::http_client client("http://localhost:8081/v1/nes/monitoring/metrics/");

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

TEST_F(E2ECoordinatorMultiWorkerTest, DISABLED_testExecutingYSBQueryWithFileOutputTwoWorker) {
    uint64_t numBuffers = 3;
    uint64_t numTuples = 10;

    NES_INFO(" start coordinator");
    std::string outputFilePath = "YSBQueryWithFileOutputTwoWorkerTestResult.txt";
    remove(outputFilePath.c_str());

    string coordinatorRPCPort = std::to_string(rpcPort);
    string cmdCoord = "./nesCoordinator --coordinatorPort=" + coordinatorRPCPort + " --restPort=" + std::to_string(restPort);
    bp::child coordinatorProc(cmdCoord.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(2);

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string cmdWrk1 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort
        + " --dataPort=" + worker1DataPort
        + " --logicalStreamName=ysb --physicalStreamName=ysb1 --sourceType=YSBSource --numberOfBuffersToProduce="
        + std::to_string(numBuffers) + " --numberOfTuplesToProducePerBuffer=" + std::to_string(numTuples)
        + " --sourceFrequency=1 --endlessRepeat=on";
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string cmdWrk2 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort
        + " --dataPort=" + worker2DataPort
        + " --logicalStreamName=ysb --physicalStreamName=ysb2 --sourceType=YSBSource --numberOfBuffersToProduce="
        + std::to_string(numBuffers) + " --numberOfTuplesToProducePerBuffer=" + std::to_string(numTuples)
        + " --sourceFrequency=1 --endlessRepeat=on";

    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    uint64_t coordinatorPid = coordinatorProc.id();
    uint64_t workerPid1 = workerProc1.id();
    uint64_t workerPid2 = workerProc2.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"ysb\\\").windowByKey(Attribute(\\\"campaign_id\\\"), "
          "TumblingWindow::of(EventTime(Attribute(\\\"current_ms\\\")), Milliseconds(1)), "
          "Sum(Attribute(\\\"user_id\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"";
    ss << "));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 4, std::to_string(restPort)));
    ASSERT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("ContinuousSourceTest: content=" << content);
    EXPECT_TRUE(!content.empty());

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