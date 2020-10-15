#include <Plans/Query/QueryId.hpp>
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

class E2ECoordinatorMultiQueryTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorMultiQueryTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() {
        NES_INFO("Tear down ActorCoordinatorWorkerTest test class.");
    }
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

    string cmdCoord = "./nesCoordinator --coordinatorPort=12346 --numberOfSlots=8";
    bp::child coordinatorProc(cmdCoord.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);

    string cmdWrk = "./nesWorker --coordinatorPort=12346 --numberOfSlots=8";
    bp::child workerProc(cmdWrk.c_str());
    NES_INFO("started worker with pid = " << workerProc.id());

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ssQuery1;
    ssQuery1 << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery1 << pathQuery1;
    ssQuery1 << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    NES_INFO("string submit for query1=" << ssQuery1.str());

    std::stringstream ssQuery2;
    ssQuery2 << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery2 << pathQuery2;
    ssQuery2 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << std::endl;
    NES_INFO("string submit for query2=" << ssQuery2.str());

    web::json::value jsonReturnQ1;
    web::http::client::http_client clientQ1("http://127.0.0.1:8081/v1/nes/");
    clientQ1.request(web::http::methods::POST, "/query/execute-query", ssQuery1.str())
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&jsonReturnQ1](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                jsonReturnQ1 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_INFO("error while setting return");
                NES_INFO("error " << e.what());
            }
        })
        .wait();

    NES_INFO("return from q1");
    NES_INFO("try to acc return Q1=" << jsonReturnQ1);
    QueryId queryId1 = jsonReturnQ1.at("queryId").as_integer();
    NES_INFO("Query ID1: " << queryId1);
    EXPECT_NE(queryId1, INVALID_QUERY_ID);

    web::json::value jsonReturnQ2;
    web::http::client::http_client clientQ2(
        "http://127.0.0.1:8081/v1/nes/query/execute-query");
    clientQ2.request(web::http::methods::POST, "/", ssQuery2.str()).then([](const web::http::http_response& response) {
                                                                       NES_INFO("get first then");
                                                                       return response.extract_json();
                                                                   })
        .then([&jsonReturnQ2](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                jsonReturnQ2 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_INFO("error while setting return");
                NES_INFO("error " << e.what());
            }
        })
        .wait();
    NES_INFO("return from q2");

    NES_INFO("try to acc return Q2=" << jsonReturnQ2);

    QueryId queryId2 = jsonReturnQ2.at("queryId").as_integer();
    NES_INFO("Query ID2: " << queryId2);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1));

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
        "+----------------------------------------------------+";

    std::ifstream ifsQ1(pathQuery1.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ1, expectedContent);

    std::ifstream ifsQ2(pathQuery2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ2, expectedContent);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
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

    string cmdCoord = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(cmdCoord.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);

    string cmdWrk = "./nesWorker --coordinatorPort=12346";
    bp::child workerProc(cmdWrk.c_str());
    NES_INFO("started worker with pid = " << workerProc.id());

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ssQuery1;
    ssQuery1
        << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery1 << pathQuery1;
    ssQuery1 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << std::endl;
    NES_INFO("string submit for query1=" << ssQuery1.str());

    std::stringstream ssQuery2;
    ssQuery2 << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery2 << pathQuery2;
    ssQuery2 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << std::endl;
    NES_INFO("string submit for query2=" << ssQuery2.str());

    std::stringstream ssQuery3;
    ssQuery3 << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery3 << pathQuery3;
    ssQuery3 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << std::endl;
    NES_INFO("string submit for query3=" << ssQuery3.str());

    web::json::value jsonReturnQ1;
    web::http::client::http_client clientQ1("http://127.0.0.1:8081/v1/nes/");
    clientQ1.request(web::http::methods::POST, "/query/execute-query", ssQuery1.str())
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&jsonReturnQ1](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return" << task.get().to_string());
                jsonReturnQ1 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_INFO("error while setting return");
                NES_INFO("error " << e.what());
            }
        })
        .wait();

    NES_INFO("return from q1");

    web::json::value jsonReturnQ2;
    web::http::client::http_client clientQ2("http://127.0.0.1:8081/v1/nes/query/execute-query");
    clientQ2.request(web::http::methods::POST, "/", ssQuery2.str()).then([](const web::http::http_response& response) {
                                                                       NES_INFO("get first then");
                                                                       return response.extract_json();
                                                                   })
        .then([&jsonReturnQ2](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return" << task.get().to_string());
                jsonReturnQ2 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_INFO("error while setting return");
                NES_INFO("error " << e.what());
            }
        })
        .wait();
    NES_INFO("return from q2");

    NES_INFO("send query 3:");
    web::json::value jsonReturnQ3;
    web::http::client::http_client clientQ3("http://127.0.0.1:8081/v1/nes/query/execute-query");
    clientQ3.request(web::http::methods::POST, "/", ssQuery3.str()).then([](const web::http::http_response& response) {
                                                                       NES_INFO("get first then");
                                                                       return response.extract_json();
                                                                   })
        .then([&jsonReturnQ3](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return" << task.get().to_string());
                jsonReturnQ3 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_INFO("error while setting return");
                NES_INFO("error " << e.what());
            }
        })
        .wait();

    NES_INFO("return from q3");

    QueryId queryId1 = jsonReturnQ1.at("queryId").as_integer();
    QueryId queryId2 = jsonReturnQ2.at("queryId").as_integer();
    QueryId queryId3 = jsonReturnQ3.at("queryId").as_integer();

    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);
    EXPECT_NE(queryId3, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId3, 1));

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
        "+----------------------------------------------------+";

    std::ifstream ifsQ1(pathQuery1.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ1, expectedContent);

    std::ifstream ifsQ2(pathQuery2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent);
    EXPECT_EQ(contentQ2, expectedContent);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

/**
 * @brief This test starts two workers and a coordinator and submits two pattern-queries
 */
TEST_F(E2ECoordinatorMultiQueryTest, testTwoPatternsWithFileOutput) {
    NES_INFO(" start coordinator");
    std::string pathPattern1 = "patternQnV1.out";
    std::string pathPattern2 = "patternQnV2.out";
    remove(pathPattern1.c_str());
    remove(pathPattern2.c_str());

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

    string path2 = "./nesWorker --coordinatorPort=12346 --logicalStreamName=QnV --physicalStreamName=test_stream --sourceType=CSVSource --sourceConfig=../tests/test_data/QnV_short.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc(path2.c_str());
    NES_INFO("started worker with pid = " << workerProc.id());
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ssPattern1;
    ssPattern1 << "{\"pattern\" : ";
    ssPattern1 << "\"Pattern::from(\\\"QnV\\\").filter(Attribute(\\\"velocity\\\") > 100).sink(FileSinkDescriptor::create(\\\"";
    ssPattern1 << pathPattern1;
    ssPattern1 << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ssPattern1 << endl;

    NES_INFO("pattern1 string submit=" << ssPattern1.str());
    string bodyPattern1 = ssPattern1.str();

    std::stringstream ssPattern2;
    ssPattern2 << "{\"pattern\" : ";
    ssPattern2 << "\"Pattern::from(\\\"QnV\\\").filter(Attribute(\\\"quantity\\\") > 10).sink(FileSinkDescriptor::create(\\\"";
    ssPattern2 << pathPattern2;
    ssPattern2 << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ssPattern2 << endl;

    NES_INFO("pattern2 string submit=" << ssPattern2.str());
    string bodyPattern2 = ssPattern2.str();

    NES_INFO("send query 1:");
    web::json::value jsonReturnQ1;
    web::http::client::http_client clientQ1("http://127.0.0.1:8081/v1/nes/");
    clientQ1.request(web::http::methods::POST, "/pattern/execute-pattern", ssPattern1.str())
        .then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();
        })
        .then([&jsonReturnQ1](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("set return" << task.get().to_string());
              jsonReturnQ1 = task.get();
          } catch (const web::http::http_exception& e) {
              NES_INFO("error while setting return");
              NES_INFO("error " << e.what());
          }
        })
        .wait();

    NES_INFO("return from q1");

    NES_INFO("send query 2:");
    web::json::value jsonReturnQ2;
    web::http::client::http_client clientQ2("http://127.0.0.1:8081/v1/nes/query/execute-query");
    clientQ2.request(web::http::methods::POST, "/", ssPattern2.str()).then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();
        })
        .then([&jsonReturnQ2](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("set return" << task.get().to_string());
              jsonReturnQ2 = task.get();
          } catch (const web::http::http_exception& e) {
              NES_INFO("error while setting return");
              NES_INFO("error " << e.what());
          }
        })
        .wait();
    NES_INFO("return from q2");


    QueryId queryId1 = jsonReturnQ1.at("queryId").as_integer();
    QueryId queryId2 = jsonReturnQ2.at("queryId").as_integer();

    EXPECT_NE(queryId1, INVALID_QUERY_ID);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);


    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId1, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1));

    string expectedContent1 =
        "+----------------------------------------------------+\n"
        "|sensor_id:CHAR|timestamp:UINT64|velocity:FLOAT32|quantity:UINT64|PatternId:INT32|\n"
        "+----------------------------------------------------+\n"
        "|R2000073|1543624020000|102.629631|8|1|\n"
        "|R2000070|1543625280000|108.166664|5|1|\n"
        "+----------------------------------------------------+";

    string expectedContent2 =
        "+----------------------------------------------------+\n"
        "|sensor_id:CHAR|timestamp:UINT64|velocity:FLOAT32|quantity:UINT64|PatternId:INT32|\n"
        "+----------------------------------------------------+\n"
        "|R2000073|1543622760000|63.277779|11|1|\n"
        "|R2000073|1543622940000|66.222221|12|1|\n"
        "|R2000073|1543623000000|74.666664|11|1|\n"
        "|R2000073|1543623480000|62.444443|13|1|\n"
        "|R2000073|1543624200000|64.611115|12|1|\n"
        "|R2000073|1543624260000|68.407410|11|1|\n"
        "|R2000073|1543625040000|56.666668|11|1|\n"
        "|R2000073|1543625400000|62.333332|11|1|\n"
        "+----------------------------------------------------+";

    std::ifstream ifsQ1(pathPattern1.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent1);
    EXPECT_EQ(contentQ1, expectedContent1);

    std::ifstream ifsQ2(pathPattern2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent2);
    EXPECT_EQ(contentQ2, expectedContent2);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithTumblingWindowFileOutput) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "ValidUserQueryWithTumbWindowFileOutputTestResult.txt";
    std::string outputFilePath2 = "ValidUserQueryWithTumbWindowFileOutputTestResult2.txt";
    remove(outputFilePath.c_str());
    remove(outputFilePath2.c_str());

    string path = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(path.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);

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

    string path2 = "./nesWorker --coordinatorPort=12346 --logicalStreamName=window --physicalStreamName=test_stream --sourceType=CSVSource --sourceConfig=../tests/test_data/window.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc(path2.c_str());
    NES_INFO("started worker with pid = " << workerProc.id());
    size_t workerPid = workerProc.id();
    sleep(1);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\").windowByKey(Attribute(\\\"id\\\"), TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10)), Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query 1 string submit=" << ss.str());
    string body = ss.str();

    std::stringstream ss2;
    ss2 << "{\"userQuery\" : ";
    ss2 << "\"Query::from(\\\"window\\\").windowByKey(Attribute(\\\"id\\\"), TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(20)), Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss2 << outputFilePath2;
    ss2 << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss2 << endl;

    NES_INFO("query 2 string submit=" << ss2.str());
    string body2 = ss2.str();

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
              NES_ERROR("error while setting return");
              NES_ERROR("error " << e.what());
          }
        })
        .wait();
    NES_INFO("return from query 1");

    web::json::value json_return_Q2;
    web::http::client::http_client clientQ2(
        "http://127.0.0.1:8081/v1/nes/query/execute-query");
    clientQ2.request(web::http::methods::POST, "/", ss2.str()).then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();
        })
        .then([&json_return_Q2](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("set return");
              json_return_Q2 = task.get();
          } catch (const web::http::http_exception& e) {
              NES_INFO("error while setting return");
              NES_INFO("error " << e.what());
          }
        })
        .wait();
    NES_INFO("return from query 2");


    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID 1: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    QueryId queryId2 = json_return_Q2.at("queryId").as_integer();
    NES_INFO("Query ID 2: " << queryId2);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1));

    string expectedContent1 =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:UINT64|value:UINT64|\n"
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

    string expectedContent2 =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:UINT64|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|0|20000|1|1177|\n"
        "|0|20000|4|6|\n"
        "|0|20000|11|30|\n"
        "|0|20000|12|7|\n"
        "|0|20000|16|12|\n"
        "+----------------------------------------------------+";

    std::ifstream ifsQ1(outputFilePath.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent1);
    EXPECT_EQ(contentQ1, expectedContent1);

    std::ifstream ifsQ2(outputFilePath2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent2);
    EXPECT_EQ(contentQ2, expectedContent2);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorProc.id());
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithSlidingWindowFileOutput) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "ValidUserQueryWithSlidWindowFileOutputTestResult.txt";
    std::string outputFilePath2 = "ValidUserQueryWithSlidWindowFileOutputTestResult2.txt";
    remove(outputFilePath.c_str());
    remove(outputFilePath2.c_str());

    string path = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(path.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);

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

    string path2 = "./nesWorker --coordinatorPort=12346 --logicalStreamName=window --physicalStreamName=test_stream --sourceType=CSVSource --sourceConfig=../tests/test_data/window.csv --numberOfBuffersToProduce=1 --sourceFrequency=1";
    bp::child workerProc(path2.c_str());
    NES_INFO("started worker with pid = " << workerProc.id());
    size_t workerPid = workerProc.id();
    sleep(1);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"window\\\").windowByKey(Attribute(\\\"id\\\"), SlidingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(10), Seconds(5)), Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query 1 string submit=" << ss.str());
    string body = ss.str();

    std::stringstream ss2;
    ss2 << "{\"userQuery\" : ";
    ss2 << "\"Query::from(\\\"window\\\").windowByKey(Attribute(\\\"id\\\"), SlidingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(20), Seconds(10)), Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
    ss2 << outputFilePath2;
    ss2 << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss2 << endl;

    NES_INFO("query 2 string submit=" << ss2.str());
    string body2 = ss2.str();

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
              NES_ERROR("error while setting return");
              NES_ERROR("error " << e.what());
          }
        })
        .wait();
    NES_INFO("return from query 1");

    web::json::value json_return_Q2;
    web::http::client::http_client clientQ2(
        "http://127.0.0.1:8081/v1/nes/query/execute-query");
    clientQ2.request(web::http::methods::POST, "/", ss2.str()).then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();
        })
        .then([&json_return_Q2](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("set return");
              json_return_Q2 = task.get();
          } catch (const web::http::http_exception& e) {
              NES_INFO("error while setting return");
              NES_INFO("error " << e.what());
          }
        })
        .wait();
    NES_INFO("return from query 2");


    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID 1: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    QueryId queryId2 = json_return_Q2.at("queryId").as_integer();
    NES_INFO("Query ID 2: " << queryId2);
    EXPECT_NE(queryId2, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId2, 1));

    string expectedContent1 =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:UINT64|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|10000|20000|1|870|\n"
        "|5000|15000|1|570|\n"
        "|0|10000|1|307|\n"
        "|10000|20000|4|0|\n"
        "|5000|15000|4|0|\n"
        "|0|10000|4|6|\n"
        "|10000|20000|11|0|\n"
        "|5000|15000|11|0|\n"
        "|0|10000|11|30|\n"
        "|10000|20000|12|0|\n"
        "|5000|15000|12|0|\n"
        "|0|10000|12|7|\n"
        "|10000|20000|16|0|\n"
        "|5000|15000|16|0|\n"
        "|0|10000|16|12|\n"
        "+----------------------------------------------------+";

    string expectedContent2 =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:UINT64|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|0|20000|1|1177|\n"
        "|0|20000|4|6|\n"
        "|0|20000|11|30|\n"
        "|0|20000|12|7|\n"
        "|0|20000|16|12|\n"
        "+----------------------------------------------------+";

    std::ifstream ifsQ1(outputFilePath.c_str());
    EXPECT_TRUE(ifsQ1.good());
    std::string contentQ1((std::istreambuf_iterator<char>(ifsQ1)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q1=" << contentQ1);
    NES_INFO("expContent=" << expectedContent1);
    EXPECT_EQ(contentQ1, expectedContent1);

    std::ifstream ifsQ2(outputFilePath2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)),
                          (std::istreambuf_iterator<char>()));
    NES_INFO("content Q2=" << contentQ2);
    NES_INFO("expContent=" << expectedContent2);
    EXPECT_EQ(contentQ2, expectedContent2);

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorProc.id());
    coordinatorProc.terminate();
}

}// namespace NES