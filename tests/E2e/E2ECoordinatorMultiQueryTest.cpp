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
TEST_F(E2ECoordinatorMultiQueryTest, DISABLED_testExecutingValidUserQueryWithFileOutputTwoQueries) {
    NES_INFO(" start coordinator");
    std::string pathQuery1 = "query1.out";
    std::string pathQuery2 = "query2.out";

    remove(pathQuery1.c_str());
    remove(pathQuery2.c_str());

    string cmdCoord = "../nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(cmdCoord.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);

    string cmdWrk = "../nesWorker --coordinatorPort=12346";
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
    uint64_t queryId1 = jsonReturnQ1.at("queryId").as_integer();
    NES_INFO("Query ID1: " << queryId1);
    EXPECT_TRUE(queryId1 != -1);

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

    uint64_t queryId2 = jsonReturnQ2.at("queryId").as_integer();
    NES_INFO("Query ID2: " << queryId2);
    EXPECT_TRUE(queryId2 != -1);

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

    string cmdCoord = "../nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(cmdCoord.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);

    string cmdWrk = "../nesWorker --coordinatorPort=12346";
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
    NES_INFO("string submit for query2=" << ssQuery3.str());

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

    uint64_t queryId1 = jsonReturnQ2.at("queryId").as_integer();
    uint64_t queryId2 = jsonReturnQ2.at("queryId").as_integer();
    uint64_t queryId3 = jsonReturnQ2.at("queryId").as_integer();

    EXPECT_TRUE(queryId1 != -1);
    EXPECT_TRUE(queryId2 != -1);
    EXPECT_TRUE(queryId3 != -1);

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

}// namespace NES