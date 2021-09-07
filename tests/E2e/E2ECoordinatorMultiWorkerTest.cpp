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
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <boost/process.hpp>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <cstdio>
#include <gtest/gtest.h>
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
// Asynchronous streams
namespace bp = boost::process;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 1200u;
uint64_t dataPort = 1400u;
uint64_t restPort = 8000u;
uint16_t timeout = 5u;

class E2ECoordinatorMultiWorkerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    void SetUp() override {
        rpcPort += 10;
        dataPort += 10;
        restPort += 10;
    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

TEST_F(E2ECoordinatorMultiWorkerTest, DISABLED_testExecutingValidUserQueryWithFileOutputTwoWorker) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "ValidUserQueryWithFileOutputTwoWorkerTestResult.txt";
    remove(outputFilePath.c_str());

    string coordinatorRPCPort = std::to_string(rpcPort);
    string cmdCoord = "./nesCoordinator --coordinatorPort=" + coordinatorRPCPort + " --restPort=" + std::to_string(restPort);
    bp::child coordinatorProc(cmdCoord.c_str());

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());

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

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"default_logical\").sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));

    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

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
    uint64_t coordinatorPid = coordinatorProc.id();
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"streamName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string path2 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort
        + " --dataPort=" + worker1DataPort
        + " --logicalStreamName=QnV --physicalStreamName=test_stream1 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1000";
    bp::child workerProc1(path2.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());
    uint64_t workerPid1 = workerProc1.id();

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string path3 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort
        + " --dataPort=" + worker2DataPort
        + " --logicalStreamName=QnV --physicalStreamName=test_stream2 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1000";
    bp::child workerProc2(path3.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());
    uint64_t workerPid2 = workerProc2.id();

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"pattern\" : ";
    ss << R"("Pattern::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    string expectedContent =
        "QnV$sensor_id:ArrayType,QnV$timestamp:INTEGER,QnV$velocity:(Float),QnV$quantity:INTEGER,QnV$PatternId:INTEGER\n"
        "R2000073,1543624020000,102.629631,8,1\n"
        "R2000070,1543625280000,108.166664,5,1\n"
        "R2000073,1543624020000,102.629631,8,1\n"
        "R2000070,1543625280000,108.166664,5,1\n";

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

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
    uint64_t coordinatorPid = coordinatorProc.id();
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"streamName\" : \"QnV\",\"schema\" : \"Schema::create()->addField(\\\"sensor_id\\\", "
              "DataTypeFactory::createFixedChar(8))->addField(createField(\\\"timestamp\\\", "
              "UINT64))->addField(createField(\\\"velocity\\\", FLOAT32))->addField(createField(\\\"quantity\\\", UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string path2 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort
        + " --dataPort=" + worker1DataPort
        + " --logicalStreamName=QnV --physicalStreamName=test_stream1 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/QnV_short_R2000073.csv "
          "--numberOfBuffersToProduce=1 --sourceFrequency=1000";
    bp::child workerProc1(path2.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());
    uint64_t workerPid1 = workerProc1.id();

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string path3 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort
        + " --dataPort=" + worker2DataPort
        + " --logicalStreamName=QnV --physicalStreamName=test_stream2 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/QnV_short_R2000070.csv "
          "--numberOfBuffersToProduce=1 --sourceFrequency=1000";
    bp::child workerProc2(path3.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());
    uint64_t workerPid2 = workerProc2.id();

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 2));

    std::stringstream ss;
    ss << "{\"pattern\" : ";
    ss << R"("Pattern::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\")";
    ss << outputFilePath;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","strategyName" : "BottomUp"})";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 2, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string line;
    bool resultWrk1 = false;
    bool resultWrk2 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        std::vector<string> content = UtilityFunctions::splitWithStringDelimiter<std::string>(line, ",");
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

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());

    std::stringstream schema;
    schema << "{\"streamName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;

    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    string worker1RPCPort = std::to_string(rpcPort + 3);
    string worker1DataPort = std::to_string(dataPort);
    string cmdWrk1 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker1RPCPort
        + " --dataPort=" + worker1DataPort
        + " --logicalStreamName=window --physicalStreamName=test_stream_1 --sourceType=CSVSource "
          "--sourceConfig=../tests/test_data/window.csv "
          "--numberOfBuffersToProduce=1 --sourceFrequency=1000 --numberOfTuplesToProducePerBuffer=28";
    bp::child workerProc1(cmdWrk1.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());

    string worker2RPCPort = std::to_string(rpcPort + 6);
    string worker2DataPort = std::to_string(dataPort + 2);
    string cmdWrk2 = "./nesWorker --coordinatorPort=" + coordinatorRPCPort + " --rpcPort=" + worker2RPCPort
        + " --dataPort=" + worker2DataPort
        + " --logicalStreamName=window "
          "--physicalStreamName=test_stream_2 --sourceType=CSVSource --sourceConfig=../tests/test_data/window.csv "
          "--numberOfBuffersToProduce=1 --sourceFrequency=1000 --numberOfTuplesToProducePerBuffer=28";
    bp::child workerProc2(cmdWrk2.c_str());
    NES_INFO("started worker 2 with pid = " << workerProc2.id());

    uint64_t coordinatorPid = coordinatorProc.id();
    uint64_t workerPid1 = workerProc1.id();
    uint64_t workerPid2 = workerProc2.id();

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 2));

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

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
    QueryId queryId = json_return.at("queryId").as_integer();

    NES_INFO("try to acc return");
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 3, std::to_string(restPort)));
    EXPECT_TRUE(TestUtils::stopQueryViaRest(queryId, std::to_string(restPort)));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
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

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());

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

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 2));

    web::json::value json_return;

    web::http::client::http_client client("http://localhost:" + std::to_string(restPort) + "/v1/nes/monitoring/metrics/");

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

    EXPECT_EQ(json_return.size(), 3ul);
    NES_INFO("RETURN: " << json_return.size());
    NES_INFO("RETURN: " << json_return);

    for (std::size_t i{1UL}; i <= json_return.size(); ++i) {
        auto json = json_return[std::to_string(i)];
        NES_INFO("SUB RETURN: " << json);

        EXPECT_TRUE(json.has_field("disk"));
        EXPECT_EQ(json["disk"].size(), 5U);

        EXPECT_TRUE(json.has_field("cpu"));
        auto numCores = json["cpu"]["NUM_CORES"].as_integer();
        EXPECT_EQ(json["cpu"].size(), numCores + 2U);

        EXPECT_TRUE(json.has_field("network"));
        EXPECT_TRUE(json["network"].size() > 0U);

        EXPECT_TRUE(json.has_field("memory"));
        EXPECT_EQ(json["memory"].size(), 13ul);
    }

    NES_INFO("Killing worker 1 process->PID: " << workerPid1);
    workerProc1.terminate();
    NES_INFO("Killing worker 2 process->PID: " << workerPid2);
    workerProc2.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

}// namespace NES