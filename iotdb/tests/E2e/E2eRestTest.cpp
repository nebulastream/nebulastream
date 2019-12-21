#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <string>
#include <thread>

#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <boost/process.hpp>
#include <err.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
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

namespace iotdb {

class E2eRestTest : public testing::Test {
 public:
  string host = "localhost";
  int port = 8081;
  string url = "http://localhost:8081/v1/iotdb/service/execute-query";
  int coordinatorPid;
  int workerPid;

  static void SetUpTestCase() {
    setupLogging();
    IOTDB_INFO("Setup E2e test class.");
  }

  static void TearDownTestCase() {
    std::cout << "Tear down ActorCoordinatorWorkerTest test class." << std::endl;

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
    logger->setLevel(log4cxx::Level::getInfo());

    // add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
  }

  static void startProcess(string cmd) {

  }
};

TEST_F(E2eRestTest, testExecutingValidUserQuery) {


  cout << " start coordinator" << endl;
   bp::child coordinatorProc("../nesCoordinator");
   cout << "started coordinator with pid = " << coordinatorProc.id() << endl;
   bp::child workerProc("../nesWorker");
   cout << "started worker with pid = " << workerProc.id() << endl;
   coordinatorPid = workerProc.id();
   workerPid = coordinatorProc.id();
   sleep(3);
  std::string outputFilePath = "blob.txt";
//  string body =
//      "{\"userQuery\" : \"Schema schema = Schema::create().addField(\\\"id\\\", BasicType::UINT32).addField(\\\"value\\\", BasicType::UINT64);Stream stream = Stream(\\\"default\\\", schema);InputQuery inputQuery = InputQuery::from(stream).writeToFile(\\\""
//          + outputFilePath
//          + "\\\"); return inputQuery;\",\"strategyName\" : \"BottomUp\"}";
//
  std::stringstream ss;
  ss << "{\"userQuery\" : \"Schema schema = Schema::create().addField(\\\"id\\\", BasicType::UINT32).addField(\\\"value\\\", BasicType::UINT64);";
  ss << "Stream stream = Stream(\\\"default\\\", schema);InputQuery inputQuery = InputQuery::from(stream).print(std::cout);";
  ss << "return inputQuery;\",\"strategyName\" : \"BottomUp\"}";
  string body = ss.str();

  web::json::value json_return;

  web::http::client::http_client client(
      "http://localhost:8081/v1/iotdb/service/execute-query");
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

  ifstream my_file(outputFilePath);
  EXPECT_TRUE(my_file.good());

  int response = remove(outputFilePath.c_str());

  EXPECT_TRUE(response == 0);
  sleep(3);
  cout << "Killing worker process->PID: " << workerPid << endl;
  workerProc.terminate();

  cout << "Killing coordinator process->PID: " << coordinatorPid << endl;
  coordinatorProc.terminate();


}

//TEST_F(E2eRestTest, testExecutingValidUserQuery) {
//  return;
//    int pidC, pidW;
//    bool terminated = false;
//    // first we fork the process
//    if (pidC = fork()) {
//        sleep(2);
//        if (pidW = fork()) {
//            sleep(2);
//
//            std::string outputFilePath = "blob.txt";
//            string body = "{\"userQuery\" : \"Schema schema = Schema::create().addField(\\\"id\\\", BasicType::UINT32).addField(\\\"value\\\", BasicType::UINT64);Stream stream = Stream(\\\"default\\\", schema);InputQuery inputQuery = InputQuery::from(stream).writeToFile(\\\"" + outputFilePath +"\\\"); return inputQuery;\",\"strategyName\" : \"BottomUp\"}";
//            web::json::value json_return;
//
//            web::http::client::http_client client("http://localhost:8081/v1/iotdb/service/execute-query");
//            client.request(web::http::methods::POST, U("/"), body)
//                .then([](const web::http::http_response& response) {
//                  cout << "get first then" << endl;
//                  return response.extract_json();
//                })
//                .then([&json_return](const pplx::task<web::json::value>& task) {
//                  try {
//                      cout << "set return" << endl;
//                      json_return = task.get();
//                  }
//                  catch (const web::http::http_exception& e) {
//                    cout << "error while setting return" << endl;
//                      std::cout << "error " << e.what() << std::endl;
//                  }
//                })
//                .wait();
//
//            std::cout << "try to acc return" << std::endl;
//
//            string queryId = json_return.at("queryId").as_string();
//            std::cout << "Query ID: " << queryId << std::endl;
//            EXPECT_TRUE(!queryId.empty());
//
//            ifstream my_file(outputFilePath);
//            EXPECT_TRUE(my_file.good());
//
//            int response = remove(outputFilePath.c_str());
//
//            EXPECT_TRUE(response == 0);
//
//            cout << "Killing worker process->PID: " << pidW << endl;
//            kill(pidW, SIGKILL);
//
//            cout << "Killing coordinator process->PID: " << pidC << endl;
//            kill(pidC, SIGKILL);
//        } else {
//            cout << "start worker" << endl;
//            const char executableC[] = "../nesWorker";
//            execlp(executableC, executableC, NULL);
//        }
//    } else {
//      cout << " start coordinator" << endl;
//        const char executableC[] = "../nesCoordinator";
//        execlp(executableC, executableC, NULL);
//    }
//      sleep(5);
//}
}
