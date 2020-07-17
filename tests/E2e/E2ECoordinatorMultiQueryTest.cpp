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

class E2ECoordinatorMultiQueryTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorMultiQueryTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ActorCoordinatorWorkerTest test class."
                  << std::endl;
    }
};

/**
 * @brief This test starts two workers and a coordinator and submit the same query but will output the results in different files
 */
TEST_F(E2ECoordinatorMultiQueryTest, DISABLED_testExecutingValidUserQueryWithFileOutputTwoQueries) {
    cout << " start coordinator" << endl;
    std::string pathQuery1 = "query1.out";
    std::string pathQuery2 = "query2.out";

    remove(pathQuery1.c_str());
    remove(pathQuery2.c_str());

    string cmdCoord = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(cmdCoord.c_str());
    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(1);

    string cmdWrk = "./nesWorker --coordinatorPort=12346";
    bp::child workerProc(cmdWrk.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ssQuery1;
    ssQuery1
        << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery1 << pathQuery1;
    ssQuery1 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << endl;
    cout << "string submit for query1=" << ssQuery1.str();

    std::stringstream ssQuery2;
    ssQuery2
        << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery2 << pathQuery2;
    ssQuery2 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << endl;
    cout << "string submit for query2=" << ssQuery2.str();

    web::json::value jsonReturnQ1;
    web::http::client::http_client clientQ1("http://localhost:8081/v1/nes/");
    clientQ1.request(web::http::methods::POST, "/query/execute-query", ssQuery1.str())
        .then([](const web::http::http_response& response) {
            cout << "get first then" << endl;
            return response.extract_json();
        })
        .then([&jsonReturnQ1](const pplx::task<web::json::value>& task) {
            try {
                cout << "set return" << endl;
                jsonReturnQ1 = task.get();
            } catch (const web::http::http_exception& e) {
                cout << "error while setting return" << endl;
                std::cout << "error " << e.what() << std::endl;
            }
        })
        .wait();

    cout << "return from q1" << endl;
    std::cout << "try to acc return Q1=" << jsonReturnQ1 << std::endl;
    string queryId1 = jsonReturnQ1.at("queryId").as_string();
    std::cout << "Query ID1: " << queryId1 << std::endl;
    EXPECT_TRUE(!queryId1.empty());

    web::json::value jsonReturnQ2;
    web::http::client::http_client clientQ2(
        "http://localhost:8081/v1/nes/query/execute-query");
    clientQ2.request(web::http::methods::POST, "/", ssQuery2.str()).then([](const web::http::http_response& response) {
                                                                       cout << "get first then" << endl;
                                                                       return response.extract_json();
                                                                   })
        .then([&jsonReturnQ2](const pplx::task<web::json::value>& task) {
            try {
                cout << "set return" << endl;
                jsonReturnQ2 = task.get();
            } catch (const web::http::http_exception& e) {
                cout << "error while setting return" << endl;
                std::cout << "error " << e.what() << std::endl;
            }
        })
        .wait();
    cout << "return from q2" << endl;

    std::cout << "try to acc return Q2=" << jsonReturnQ2 << std::endl;

    string queryId2 = jsonReturnQ2.at("queryId").as_string();
    std::cout << "Query ID2: " << queryId2 << std::endl;
    EXPECT_TRUE(!queryId2.empty());

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
    cout << "content Q1=" << contentQ1 << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(contentQ1, expectedContent);

    std::ifstream ifsQ2(pathQuery2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)),
                          (std::istreambuf_iterator<char>()));
    cout << "content Q2=" << contentQ2 << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(contentQ2, expectedContent);

    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

/**
 * @brief This test starts two workers and a coordinator and submit too many queries such that we test if the error-prone process
 */
TEST_F(E2ECoordinatorMultiQueryTest, testExecutingValidUserQueryWithFileOutputThreeQueriesWithErrorTest) {
    cout << " start coordinator" << endl;
    std::string pathQuery1 = "query1.out";
    std::string pathQuery2 = "query2.out";
    std::string pathQuery3 = "query3.out";

    remove(pathQuery1.c_str());
    remove(pathQuery2.c_str());
    remove(pathQuery3.c_str());

    string cmdCoord = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(cmdCoord.c_str());
    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(1);

    string cmdWrk = "./nesWorker --coordinatorPort=12346";
    bp::child workerProc(cmdWrk.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ssQuery1;
    ssQuery1
        << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery1 << pathQuery1;
    ssQuery1 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << endl;
    cout << "string submit for query1=" << ssQuery1.str();

    std::stringstream ssQuery2;
    ssQuery2 << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery2 << pathQuery2;
    ssQuery2 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << endl;
    cout << "string submit for query2=" << ssQuery2.str();

    std::stringstream ssQuery3;
    ssQuery3 << "{\"userQuery\" : \"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ssQuery3 << pathQuery3;
    ssQuery3 << "\\\"));\",\"strategyName\" : \"BottomUp\"}" << endl;
    cout << "string submit for query2=" << ssQuery3.str();

    web::json::value jsonReturnQ1;
    web::http::client::http_client clientQ1("http://localhost:8081/v1/nes/");
    clientQ1.request(web::http::methods::POST, "/query/execute-query", ssQuery1.str())
        .then([](const web::http::http_response& response) {
            cout << "get first then" << endl;
            return response.extract_json();
        })
        .then([&jsonReturnQ1](const pplx::task<web::json::value>& task) {
            try {
                cout << "set return" << task.get().to_string() << endl;
                jsonReturnQ1 = task.get();
            } catch (const web::http::http_exception& e) {
                cout << "error while setting return" << endl;
                std::cout << "error " << e.what() << std::endl;
            }
        })
        .wait();

    cout << "return from q1" << endl;

    web::json::value jsonReturnQ2;
    web::http::client::http_client clientQ2("http://localhost:8081/v1/nes/query/execute-query");
    clientQ2.request(web::http::methods::POST, "/", ssQuery2.str()).then([](const web::http::http_response& response) {
                                                                       cout << "get first then" << endl;
                                                                       return response.extract_json();
                                                                   })
        .then([&jsonReturnQ2](const pplx::task<web::json::value>& task) {
            try {
                cout << "set return" << task.get().to_string() << endl;
                jsonReturnQ2 = task.get();
            } catch (const web::http::http_exception& e) {
                cout << "error while setting return" << endl;
                std::cout << "error " << e.what() << std::endl;
            }
        })
        .wait();
    cout << "return from q2" << endl;

    cout << "send query 3:" << endl;
    web::json::value jsonReturnQ3;
    web::http::client::http_client clientQ3("http://localhost:8081/v1/nes/query/execute-query");
    clientQ3.request(web::http::methods::POST, "/", ssQuery3.str()).then([](const web::http::http_response& response) {
                                                                       cout << "get first then" << endl;
                                                                       return response.extract_json();
                                                                   })
        .then([&jsonReturnQ3](const pplx::task<web::json::value>& task) {
            try {
                cout << "set return" << task.get().to_string() << endl;
                jsonReturnQ3 = task.get();
            } catch (const web::http::http_exception& e) {
                cout << "error while setting return" << endl;
                std::cout << "error " << e.what() << std::endl;
            }
        })
        .wait();

    cout << "return from q3" << endl;

    string queryId1 = jsonReturnQ2.at("queryId").as_string();
    string queryId2 = jsonReturnQ2.at("queryId").as_string();
    string queryId3 = jsonReturnQ2.at("queryId").as_string();

    EXPECT_TRUE(!queryId1.empty());
    EXPECT_TRUE(!queryId2.empty());
    EXPECT_TRUE(!queryId3.empty());

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
    cout << "content Q1=" << contentQ1 << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(contentQ1, expectedContent);

    std::ifstream ifsQ2(pathQuery2.c_str());
    EXPECT_TRUE(ifsQ2.good());
    std::string contentQ2((std::istreambuf_iterator<char>(ifsQ2)),
                          (std::istreambuf_iterator<char>()));
    cout << "content Q2=" << contentQ2 << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(contentQ2, expectedContent);

    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

}// namespace NES