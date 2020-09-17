#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>
#define GetCurrentDir getcwd
#include <Util/TestUtils.hpp>
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

    string path2 = "./nesWorker --coordinatorPort=12346 --rpcPort=12351 --dataPort=12352 --logicalStreamName=QnV --physicalStreamName=test_stream1 --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc1(path2.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());
    size_t workerPid1 = workerProc1.id();
    sleep(1);

    string path3 = "./nesWorker --coordinatorPort=12346 --rpcPort=12353 --dataPort=12354 --logicalStreamName=QnV --physicalStreamName=test_stream2 --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
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

TEST_F(E2ECoordinatorWorkerTest, DISABLED_testExecutingValidSimplePatternWithFileOutputTwoWorkerDifferentSource) {
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

    string path2 = "./nesWorker --coordinatorPort=12346 --rpcPort=12351 --dataPort=12352 --logicalStreamName=QnV --physicalStreamName=test_stream1 --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short_R2000073.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc1(path2.c_str());
    NES_INFO("started worker 1 with pid = " << workerProc1.id());
    size_t workerPid1 = workerProc1.id();
    sleep(1);

    string path3 = "./nesWorker --coordinatorPort=12346 --rpcPort=12353 --dataPort=12354 --logicalStreamName=QnV --physicalStreamName=test_stream2 --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short_R2000070.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
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
}// namespace NES