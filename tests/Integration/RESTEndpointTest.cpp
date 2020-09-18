#include <Plans/Query/QueryId.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>

#define GetCurrentDir getcwd
#include <Util/TestUtils.hpp>
#include <boost/process.hpp>
#include <cpprest/details/basic_types.h>
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
//#define _XPLATSTR(x) _XPLATSTR(x)
namespace NES {

class RESTEndpointTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("RESTEndpointTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup RESTEndpointTest test class.");
    }

    static void TearDownTestCase() {
        NES_INFO("Tear down RESTEndpointTest test class.");
    }
};

TEST_F(RESTEndpointTest, testGetExecutionPlanFromWithSingleWorker) {
    NES_INFO(" start coordinator");
    std::string outputFilePath = "testGetExecutionPlanFromWithSingleWorker.txt";
    remove(outputFilePath.c_str());

    string path = "../nesCoordinator --coordinatorPort=12267";
    bp::child coordinatorProc(path.c_str());
    NES_INFO("started coordinator with pid = " << coordinatorProc.id());
    sleep(1);

    string path2 = "../nesWorker --coordinatorPort=12267";
    bp::child workerProc(path2.c_str());
    NES_INFO("started worker with pid = " << workerProc.id());
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"default_logical\\\").filter(Attribute(\\\"id\\\") >= 1).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    NES_INFO("query string submit=" << ss.str());
    string querySubmissionRequestBody = ss.str();

    web::json::value querySubmissionJsonReturn;

    web::http::client::http_client querySubmissionClient(
        "http://127.0.0.1:8081/v1/nes/query/execute-query");
    querySubmissionClient.request(web::http::methods::POST, _XPLATSTR("/"), querySubmissionRequestBody).then([](const web::http::http_response& response) {
                                                                      NES_INFO("get first then");
                                                                      return response.extract_json();
                                                                  })
        .then([&querySubmissionJsonReturn](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("submit query: set return");
                querySubmissionJsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("submit query: error while setting return");
                NES_ERROR("submit query: error " << e.what());
            }
        })
        .wait();

    NES_INFO("get execution-plan: try to acc return");
    QueryId queryId = querySubmissionJsonReturn.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    sleep(5);
    // get the execution plan
    std::stringstream getExecutionPlanStringStream;
    getExecutionPlanStringStream << "{\"queryId\" : ";
    getExecutionPlanStringStream << queryId;
    getExecutionPlanStringStream << "}";
    getExecutionPlanStringStream << endl;

    NES_INFO("get execution plan request body=" << getExecutionPlanStringStream.str());
    string getExecutionPlanRequestBody = getExecutionPlanStringStream.str();
    web::json::value getExecutionPlanJsonReturn;

    web::http::client::http_client getExecutionPlanClient(
        "http://127.0.0.1:8081/v1/nes/query/execution-plan");
    getExecutionPlanClient.request(web::http::methods::GET, _XPLATSTR("/"), getExecutionPlanRequestBody).then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();
        })
        .then([&getExecutionPlanJsonReturn](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("get execution-plan: set return");
              getExecutionPlanJsonReturn = task.get();
          } catch (const web::http::http_exception& e) {
              NES_ERROR("get execution-plan: error while setting return");
              NES_ERROR("get execution-plan: error " << e.what());
          }
        })
        .wait();

    NES_INFO("get execution-plan: try to acc return");
    NES_DEBUG("getExecutionPlan response: " << getExecutionPlanJsonReturn.serialize());

    ASSERT_EQ(getExecutionPlanJsonReturn.serialize(), "{\"executionNodes\":[{\"executionNodeId\":2,\"querySubPlan\":[{\"operator\":\"SOURCE(OP-1)=>FILTER(OP-2)=>SINK_SYS(OP-5)\",\"querySubPlanId\":1}]},{\"executionNodeId\":1,\"querySubPlan\":[{\"operator\":\"SOURCE_SYS(OP-4)=>SINK(OP-3)\",\"querySubPlanId\":2}]}]}");
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1));

    NES_INFO("Killing worker process->PID: " << workerPid);
    workerProc.terminate();
    NES_INFO("Killing coordinator process->PID: " << coordinatorPid);
    coordinatorProc.terminate();
}


}// namespace NES
