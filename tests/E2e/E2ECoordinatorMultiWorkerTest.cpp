#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <string>
#include <unistd.h>
#define GetCurrentDir getcwd
#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <boost/process.hpp>
#include <cstdio>
#include <sstream>
#include <Util/TestUtils.hpp>

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
        std::cout << "Tear down ActorCoordinatorWorkerTest test class."
                  << std::endl;
    }
};

TEST_F(E2ECoordinatorWorkerTest, testExecutingValidUserQueryWithFileOutputTwoWorker) {
    cout << " start coordinator" << endl;
    std::string outputFilePath =
        "ValidUserQueryWithFileOutputTwoWorkerTestResult.txt";
    remove(outputFilePath.c_str());

    string cmdCoord = "./nesCoordinator --coordinatorPort=12348";
    bp::child coordinatorProc(cmdCoord.c_str());

    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(2);

    string cmdWrk1 = "./nesWorker --coordinatorPort=12348 --rpcPort=12351 --dataPort=12352";
    bp::child workerProc1(cmdWrk1.c_str());
    cout << "started worker 1 with pid = " << workerProc1.id() << endl;

    string cmdWrk2 = "./nesWorker --coordinatorPort=12348 --rpcPort=12353 --dataPort=12354";
    bp::child workerProc2(cmdWrk2.c_str());
    cout << "started worker 2 with pid = " << workerProc2.id() << endl;

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid1 = workerProc1.id();
    size_t workerPid2 = workerProc2.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    cout << "string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://127.0.0.1:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then([](const web::http::http_response& response) {
                                                              cout << "get first then" << endl;
                                                              return response.extract_json();
                                                          })
        .then([&json_return](const pplx::task<web::json::value>& task) {
            try {
                cout << "set return" << endl;
                json_return = task.get();
            } catch (const web::http::http_exception& e) {
                cout << "error while setting return" << endl;
                std::cout << "error " << e.what() << std::endl;
            }
        })
        .wait();

    string queryId = json_return.at("queryId").as_string();

    std::cout << "try to acc return" << std::endl;
    std::cout << "Query ID: " << queryId << std::endl;
    EXPECT_TRUE(!queryId.empty());

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
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    cout << "Killing worker 1 process->PID: " << workerPid1 << endl;
    workerProc1.terminate();
    cout << "Killing worker 2 process->PID: " << workerPid2 << endl;
    workerProc2.terminate();
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}
}