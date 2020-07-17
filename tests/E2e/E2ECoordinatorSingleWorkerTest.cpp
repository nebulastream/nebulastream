#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <string>
#include <unistd.h>
#define GetCurrentDir getcwd
#include <cpprest/http_client.h>
#include <cpprest/details/basic_types.h>
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
//#define _XPLATSTR(x) _XPLATSTR(x)
namespace NES {

class E2ECoordinatorSingleWorkerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("E2ECoordinatorSingleWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup E2e test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ActorCoordinatorWorkerTest test class."
                  << std::endl;
    }
};


TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithPrintOutput) {
    cout << " start coordinator" << endl;
    string path = "./nesCoordinator --coordinatorPort=12345";
    bp::child coordinatorProc(path.c_str());

    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(1);

    string path2 = "./nesWorker --coordinatorPort=12345";
    bp::child workerProc(path2.c_str());
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"default_logical\\\").sink(PrintSinkDescriptor::create());\"";
    ss << R"(,"strategyName" : "BottomUp"})";
    ss << endl;
    cout << "string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then(
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

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1));

    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}
TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutput) {
    cout << " start coordinator" << endl;
    std::string outputFilePath = "ValidUserQueryWithFileOutputTestResult.txt";
    remove(outputFilePath.c_str());

    string path = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(path.c_str());
    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(1);

    string path2 = "./nesWorker --coordinatorPort=12346";
    bp::child workerProc(path2.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();

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
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then(
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

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1));

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

    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}
TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputWithFilter) {
    cout << " start coordinator" << endl;
    std::string outputFilePath = "UserQueryWithFileOutputWithFilterTestResult.txt";
    remove(outputFilePath.c_str());

    string path = "./nesCoordinator --coordinatorPort=12267";
    bp::child coordinatorProc(path.c_str());
    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(1);

    string path2 = "./nesWorker --coordinatorPort=12267";
    bp::child workerProc(path2.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"default_logical\\\").filter(Attribute(\\\"id\\\") > 3).sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\"));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;

    cout << "query string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then(
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

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 0));

    // if filter is applied correctly, no output is generated
    ifstream outFile(outputFilePath);
    EXPECT_TRUE(!outFile.good());

    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputAndRegisterPhyStream) {
    cout << " start coordinator" << endl;
    std::string outputFilePath =
        "ValidUserQueryWithFileOutputAndRegisterPhyStreamTestResult.txt";
    remove(outputFilePath.c_str());

    string path = "./nesCoordinator --coordinatorPort=12342";
    bp::child coordinatorProc(path.c_str());
    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(1);

    string path2 =
        "./nesWorker --coordinatorPort=12342 --sourceType=DefaultSource --numberOfBuffersToProduce=2 --physicalStreamName=test_stream --logicalStreamName=default_logical";
    bp::child workerProc(path2.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();

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
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then(
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

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1));

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

    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

TEST_F(E2ECoordinatorSingleWorkerTest, testExecutingValidUserQueryWithFileOutputExdraUseCase) {
    cout << " start coordinator" << endl;
    std::string testFile = "exdra.csv";
    remove(testFile.c_str());

    string path = "./nesCoordinator --coordinatorPort=12346";
    bp::child coordinatorProc(path.c_str());
    cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
    sleep(1);

    string path2 =
        "./nesWorker --coordinatorPort=12346 --sourceType=CSVSource --sourceConfig=tests/test_data/exdra.csv --numberOfBuffersToProduce=1 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra";

    bp::child workerProc(path2.c_str());
    cout << "started worker with pid = " << workerProc.id() << endl;
    size_t coordinatorPid = coordinatorProc.id();
    size_t workerPid = workerProc.id();
    //sleep(3);

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"exdra\\\").sink(FileSinkDescriptor::create(\\\"";
    ss << testFile;
    ss << "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"";
    ss << "));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    cout << "string submit=" << ss.str();
    string body = ss.str();

    web::json::value json_return;

    web::http::client::http_client client(
        "http://localhost:8081/v1/nes/query/execute-query");
    client.request(web::http::methods::POST, _XPLATSTR("/"), body).then(
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
    //sleep(2);

    std::cout << "try to acc return" << std::endl;
    string queryId = json_return.at("queryId").as_string();
    std::cout << "Query ID: " << queryId << std::endl;
    EXPECT_TRUE(!queryId.empty());

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1));

    //sleep(2);

    ifstream testFileOutput(testFile);
    EXPECT_TRUE(testFileOutput.good());

    std::ifstream ifs(testFile.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "type:Char,metadata_generated:INTEGER,metadata_title:Char,metadata_id:Char,features_type:Char,features_properties_capacity:INTEGER,features_properties_efficiency:(Float),features_properties_mag:(Float),features_properties_time:INTEGER,features_properties_updated:INTEGER,features_properties_type:Char,features_geometry_type:Char,features_geometry_coordinates_longitude:(Float),features_geometry_coordinates_latitude:(Float),features_eventId :Char,\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n";
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(content, expectedContent);

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);

    //sleep(2);
    cout << "Killing worker process->PID: " << workerPid << endl;
    workerProc.terminate();
    //sleep(2);
    cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
    coordinatorProc.terminate();
}

}
