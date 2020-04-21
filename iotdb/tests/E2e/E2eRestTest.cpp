#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <string>
#include <thread>
#include <unistd.h>
#define GetCurrentDir getcwd
#include <gtest/gtest.h>
#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <boost/process.hpp>
#include <err.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>
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

class E2eRestTest : public testing::Test {
  public:
    string host = "localhost";
    int port = 8081;
    string url = "http://localhost:8081/v1/nes/query/execute-query";

    static void SetUpTestCase() {
        NES::setupLogging("E2eRestTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ActorCoordinatorWorkerTest test class."
                  << std::endl;
    }
};

std::string GetCurrentWorkingDir(void) {
    char buff[FILENAME_MAX];
    GetCurrentDir(buff, FILENAME_MAX);
    std::string current_working_dir(buff);
    return current_working_dir;
}

TEST_F(E2eRestTest, _testExecutingValidUserQueryWithPrintOutput) {
    cout << " start coordinator" << endl;
    string path = "./nesCoordinator --actor_port=12345";
    bp::child coordinatorProc(path.c_str());

    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(2);

    string path2 = "./nesWorker --actor_port=12345";
    bp::child workerProc(path2.c_str());
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"InputQuery::from(default_logical).print(std::cout);\"";
    ss << R"(,"strategyName" : "BottomUp"})";
    ss << endl;
    cout << "string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, U("/"), body).then(
        [](const web::http::http_response& response) {
          cout << "get first then with response" << endl;
          return response.extract_json();
        }).then([&json_return](const pplx::task<web::json::value>& task) {
      try {
          cout << "set return" << endl;
          json_return = task.get();
          cout << "ret is=" << json_return << endl;
      } catch (const web::http::http_exception& e) {
          cout << "error while setting return" << endl;
          std::cout << "error " << e.what() << std::endl;
      }
    }).wait();

    std::cout << "try to acc return" << std::endl;

    string queryId = json_return.at("queryId").as_string();
    std::cout << "Query ID: " << queryId << std::endl;
    EXPECT_TRUE(!queryId.empty());

    sleep(2);
    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    sleep(2);
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}
TEST_F(E2eRestTest, testExecutingValidUserQueryWithFileOutput) {
    cout << " start coordinator" << endl;
    std::string outputFilePath = "ValidUserQueryWithFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    string path = "./nesCoordinator --actor_port=12346";
    bp::child coordinatorProc(path.c_str());

    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(2);

    string path2 = "./nesWorker --actor_port=12346";
    bp::child workerProc(path2.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"InputQuery::from(default_logical).writeToFile(\\\"";
    ss << outputFilePath;
    ss << "\\\");\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    cout << "string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, U("/"), body).then(
        [](const web::http::http_response& response) {
          cout << "get first then" << endl;
          return response.extract_json();
        }).then([&json_return](const pplx::task<web::json::value>& task) {
      try {
          cout << "set return" << endl;
          json_return = task.get();
      } catch (const web::http::http_exception& e) {
          cout << "error while setting return" << endl;
          std::cout << "error " << e.what() << std::endl;
      }
    }).wait();
    sleep(4);

    std::cout << "try to acc return" << std::endl;

    string queryId = json_return.at("queryId").as_string();
    std::cout << "Query ID: " << queryId << std::endl;
    EXPECT_TRUE(!queryId.empty());

    sleep(2);

    ifstream my_file(outputFilePath);
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(outputFilePath.c_str());
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
        "+----------------------------------------------------+";
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    sleep(2);
    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    sleep(2);
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

TEST_F(E2eRestTest, _testExecutingValidUserQueryWithFileOutputWithFilter) {
    cout << " start coordinator" << endl;
    std::string outputFilePath = "UserQueryWithFileOutputWithFilterTestResult.txt";
    remove(outputFilePath.c_str());

    string path = "./nesCoordinator --actor_port=12346";
    bp::child coordinatorProc(path.c_str());

    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(2);

    string path2 = "./nesWorker --actor_port=12346";
    bp::child workerProc(path2.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"InputQuery::from(default_logical).filter(default_logical[\\\"id\\\"] > 3).writeToFile(\\\"";
    ss << outputFilePath;
    ss << "\\\");\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    cout << "query string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, U("/"), body).then(
        [](const web::http::http_response& response) {
          cout << "get first then" << endl;
          return response.extract_json();
        }).then([&json_return](const pplx::task<web::json::value>& task) {
      try {
          cout << "set return" << endl;
          json_return = task.get();
      } catch (const web::http::http_exception& e) {
          cout << "error while setting return" << endl;
          std::cout << "error " << e.what() << std::endl;
      }
    }).wait();

    std::cout << "try to acc return" << std::endl;
    string queryId = json_return.at("queryId").as_string();
    std::cout << "Query ID: " << queryId << std::endl;
    EXPECT_TRUE(!queryId.empty());

    sleep(2);

    // if filter is applied correctly, no output is generated
    ifstream outFile(outputFilePath);
    EXPECT_TRUE(!outFile.good());

    sleep(2);
    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    sleep(2);
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

TEST_F(E2eRestTest, _testExecutingValidUserQueryWithFileOutputTwoWorker) {
    cout << " start coordinator" << endl;
    std::string outputFilePath =
        "ValidUserQueryWithFileOutputTwoWorkerTestResult.txt";
    remove(outputFilePath.c_str());

    string cmdCoord = "./nesCoordinator --actor_port=12346";
    bp::child coordinatorProc(cmdCoord.c_str());

    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(2);

    string cmdWrk = "./nesWorker --actor_port=12346";
    bp::child workerProc1(cmdWrk.c_str());
    cout << "started worker 1 with pid = " << workerProc1.id() << endl;

    bp::child workerProc2(cmdWrk.c_str());
    cout << "started worker 2 with pid = " << workerProc2.id() << endl;

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid1 = workerProc1.id();
    size_t workerPid2 = workerProc2.id();
    sleep(3);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"InputQuery::from(default_logical).writeToFile(\\\"";
    ss << outputFilePath;
    ss << "\\\");\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    cout << "string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, U("/"), body).then(
        [](const web::http::http_response& response) {
          cout << "get first then" << endl;
          return response.extract_json();
        }).then([&json_return](const pplx::task<web::json::value>& task) {
      try {
          cout << "set return" << endl;
          json_return = task.get();
      } catch (const web::http::http_exception& e) {
          cout << "error while setting return" << endl;
          std::cout << "error " << e.what() << std::endl;
      }
    }).wait();
    sleep(3);
    std::cout << "try to acc return" << std::endl;

    string queryId = json_return.at("queryId").as_string();
    std::cout << "Query ID: " << queryId << std::endl;
    EXPECT_TRUE(!queryId.empty());

    sleep(2);

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

    sleep(2);
    cout << "Killing worker 1 process->PID: " << workerPid1 << endl;
    workerProc1.terminate();
    cout << "Killing worker 2 process->PID: " << workerPid2 << endl;
    workerProc2.terminate();
    sleep(2);
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

TEST_F(E2eRestTest, _testExecutingValidUserQueryWithFileOutputAndRegisterPhyStream) {
    cout << " start coordinator" << endl;
    std::string outputFilePath =
        "ValidUserQueryWithFileOutputAndRegisterPhyStreamTestResult.txt";
    remove(outputFilePath.c_str());

    string path = "./nesCoordinator --actor_port=12346";
    bp::child coordinatorProc(path.c_str());

    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(2);

    string path2 =
        "./nesWorker --actor_port=12346 --sourceType=DefaultSource --numberOfBuffersToProduce=2 --physicalStreamName=test_stream --logicalStreamName=default_logical";
    bp::child workerProc(path2.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"InputQuery::from(default_logical).writeToFile(\\\"";
    ss << outputFilePath;
    ss << "\\\");\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    cout << "string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, U("/"), body).then(
        [](const web::http::http_response& response) {
          cout << "get first then" << endl;
          return response.extract_json();
        }).then([&json_return](const pplx::task<web::json::value>& task) {
      try {
          cout << "set return" << endl;
          json_return = task.get();
      } catch (const web::http::http_exception& e) {
          cout << "error while setting return" << endl;
          std::cout << "error " << e.what() << std::endl;
      }
    }).wait();
    sleep(2);

    std::cout << "try to acc return" << std::endl;

    string queryId = json_return.at("queryId").as_string();
    std::cout << "Query ID: " << queryId << std::endl;
    EXPECT_TRUE(!queryId.empty());

    sleep(2);

    ifstream my_file(outputFilePath);
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(outputFilePath.c_str());
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
        "+----------------------------------------------------+";
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(content, expectedContent);

    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);

    sleep(2);
    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    sleep(2);
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

TEST_F(E2eRestTest, testExecutingValidUserQueryWithFileOutputExdraUseCase) {
    cout << " start coordinator" << endl;
    std::string testFile = "exdra.csv";
    remove(testFile.c_str());

    string path = "./nesCoordinator --actor_port=12346";
    bp::child coordinatorProc(path.c_str());
    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(2);

    string path2 =
        "./nesWorker --actor_port=12346 --sourceType=CSVSource --sourceConfig=tests/test_data/exdra.csv --numberOfBuffersToProduce=1 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra";

    bp::child workerProc(path2.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"InputQuery::from(exdra).writeToCSVFile(\\\"";
    ss << testFile;
    ss << "\\\", \\\"truncate\\\"";
    ss << ");\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    cout << "string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, U("/"), body).then(
        [](const web::http::http_response& response) {
          cout << "get first then" << endl;
          return response.extract_json();
        }).then([&json_return](const pplx::task<web::json::value>& task) {
      try {
          cout << "set return" << endl;
          json_return = task.get();
      } catch (const web::http::http_exception& e) {
          cout << "error while setting return" << endl;
          std::cout << "error " << e.what() << std::endl;
      }
    }).wait();
    sleep(2);

    std::cout << "try to acc return" << std::endl;
    string queryId = json_return.at("queryId").as_string();
    std::cout << "Query ID: " << queryId << std::endl;
    EXPECT_TRUE(!queryId.empty());

    sleep(2);

    ifstream testFileOutput(testFile);
    EXPECT_TRUE(testFileOutput.good());

    std::ifstream ifs(testFile.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n";
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(content, expectedContent);

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);

    sleep(2);
    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    sleep(2);
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

TEST_F(E2eRestTest, testExecutingValidUserQueryWithFileOutputTwoQueries) {
    cout << " start coordinator" << endl;
    std::string pathQuery1 = "query1.out";
    std::string pathQuery2 = "query2.out";

    remove(pathQuery1.c_str());
    remove(pathQuery2.c_str());

    string cmdCoord = "./nesCoordinator --actor_port=12346";
    bp::child coordinatorProc(cmdCoord.c_str());

    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(2);

    string cmdWrk = "./nesWorker --actor_port=12346";
    bp::child workerProc(cmdWrk.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;

    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    sleep(3);

    std::stringstream ssQuery1;
    ssQuery1
        << "{\"userQuery\" : \"InputQuery::from(default_logical).writeToFile(\\\"";
    ssQuery1 << pathQuery1;
    ssQuery1 << "\\\");\",\"strategyName\" : \"BottomUp\"}" << endl;
    cout << "string submit for query1=" << ssQuery1.str();

    std::stringstream ssQuery2;
    ssQuery2
        << "{\"userQuery\" : \"InputQuery::from(default_logical).writeToFile(\\\"";
    ssQuery2 << pathQuery2;
    ssQuery2 << "\\\");\",\"strategyName\" : \"BottomUp\"}" << endl;
    cout << "string submit for query1=" << ssQuery2.str();

    web::json::value jsonReturnQ1;
    web::http::client::http_client clientQ1(
        "http://localhost:8081/v1/nes/query/execute-query");
    clientQ1.request(web::http::methods::POST, U("/"), ssQuery1.str()).then(
        [](const web::http::http_response& response) {
          cout << "get first then" << endl;
          return response.extract_json();
        }).then([&jsonReturnQ1](const pplx::task<web::json::value>& task) {
      try {
          cout << "set return" << endl;
          jsonReturnQ1 = task.get();
      } catch (const web::http::http_exception& e) {
          cout << "error while setting return" << endl;
          std::cout << "error " << e.what() << std::endl;
      }
    });

    cout << "return from q1" << endl;
    web::json::value jsonReturnQ2;
    web::http::client::http_client clientQ2(
        "http://localhost:8081/v1/nes/query/execute-query");
    clientQ2.request(web::http::methods::POST, U("/"), ssQuery2.str()).then(
        [](const web::http::http_response& response) {
          cout << "get first then" << endl;
          return response.extract_json();
        }).then([&jsonReturnQ2](const pplx::task<web::json::value>& task) {
      try {
          cout << "set return" << endl;
          jsonReturnQ2 = task.get();
      } catch (const web::http::http_exception& e) {
          cout << "error while setting return" << endl;
          std::cout << "error " << e.what() << std::endl;
      }
    }).wait();
    cout << "return from q2" << endl;
    sleep(5);

    std::cout << "try to acc return Q1=" << jsonReturnQ1 << std::endl;
    std::cout << "try to acc return Q2=" << jsonReturnQ2 << std::endl;

    string queryId1 = jsonReturnQ1.at("queryId").as_string();
    std::cout << "Query ID1: " << queryId1 << std::endl;
    EXPECT_TRUE(!queryId1.empty());

    string queryId2 = jsonReturnQ2.at("queryId").as_string();
    std::cout << "Query ID2: " << queryId2 << std::endl;
    EXPECT_TRUE(!queryId2.empty());
    sleep(2);

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

    sleep(2);
    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    sleep(2);
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}
}
