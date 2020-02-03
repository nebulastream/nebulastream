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
  std::string outputFilePath = "blob.txt";
  int coordinatorPid;
  int workerPid;

  static void SetUpTestCase() {
    setupLogging();
    NES_INFO("Setup E2e test class.");
  }

  static void TearDownTestCase() {
    std::cout << "Tear down ActorCoordinatorWorkerTest test class."
              << std::endl;
  }

 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout(
            "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "WindowManager.log");
    log4cxx::FileAppenderPtr file(
        new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(
        new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    // logger->setLevel(log4cxx::Level::getDebug());
    NESLogger->setLevel(log4cxx::Level::getInfo());

    // add appenders and other will inherit the settings
    NESLogger->addAppender(file);
    NESLogger->addAppender(console);
  }

};

std::string GetCurrentWorkingDir(void) {
  char buff[FILENAME_MAX];
  GetCurrentDir(buff, FILENAME_MAX);
  std::string current_working_dir(buff);
  return current_working_dir;
}


TEST_F(E2eRestTest, testExecutingValidUserQueryWithPrintOutput) {
  cout << " start coordinator" << endl;
  string path = "./nesCoordinator";
  bp::child coordinatorProc(path.c_str());

  cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
  sleep(2);

  string path2 = "./nesWorker";
  bp::child workerProc(path2.c_str());
  coordinatorPid = workerProc.id();
  workerPid = coordinatorProc.id();
  sleep(3);

  std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << "\"InputQuery::from(default_logical).print(std::cout);\"";
    ss << ",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
  cout << "string submit=" << ss.str();
  string body = ss.str();

  web::json::value json_return;

  web::http::client::http_client client(
      "http://localhost:8081/v1/nes/query/execute-query");
  client.request(web::http::methods::POST, U("/"), body).then(
      [](const web::http::http_response& response) {
        cout << "get first then with response" <<  endl;
        return response.extract_json();
      }).then([&json_return](const pplx::task<web::json::value>& task) {
    try {
      cout << "set return" << endl;
      json_return = task.get();
      cout << "ret is=" << json_return << endl;
    }
    catch (const web::http::http_exception& e) {
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

#if 0
TEST_F(E2eRestTest, testExecutingValidUserQueryWithFileOutput) {
  cout << " start coordinator" << endl;
  remove(outputFilePath.c_str());

  string path = "./nesCoordinator";
  bp::child coordinatorProc(path.c_str());

  cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
  sleep(2);

  string path2 = "./nesWorker";
  bp::child workerProc(path2.c_str());
  cout << "started worker with pid = " << workerProc.id() << endl;
  coordinatorPid = workerProc.id();
  workerPid = coordinatorProc.id();
  sleep(3);

  std::stringstream ss;
  ss << "{\"userQuery\" : ";
  ss <<  "\"InputQuery::from(default_logical).writeToFile(\\\"";
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
    }
    catch (const web::http::http_exception& e) {
      cout << "error while setting return" << endl;
      std::cout << "error " << e.what() << std::endl;
    }
  }).wait();

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
#endif
}
