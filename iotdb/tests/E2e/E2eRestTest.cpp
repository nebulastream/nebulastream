#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <string>
#include <thread>

#include <cpprest/http_client.h>
#include <cpprest/filestream.h>

#include <err.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

using namespace std;
using namespace utility;                    // Common utilities like string conversions
using namespace web;                        // Common features like URIs.
using namespace web::http;                  // Common HTTP functionality
using namespace web::http::client;          // HTTP client features
using namespace concurrency::streams;       // Asynchronous streams

namespace iotdb {

class E2eRestTest : public testing::Test {
 public:
  string host = "localhost";
  int port = 8081;
  string url = "http://localhost:8081/v1/iotdb/service/execute-query";

  static void SetUpTestCase() {
    setupLogging();
    IOTDB_INFO("Setup E2e test class.");
  }

  static void TearDownTestCase() { std::cout << "Tear down ActorCoordinatorWorkerTest test class." << std::endl; }

 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "WindowManager.log");
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    // logger->setLevel(log4cxx::Level::getDebug());
    logger->setLevel(log4cxx::Level::getInfo());

    // add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
  }
};

TEST_F(E2eRestTest, setupREST) {
  int pidC, pidW, status;
  // first we fork the process
  if (pidC = fork()) {
    sleep(2);
    if (pidW = fork()) {
      sleep(2);

      string body = "{\"userQuery\": \"example\",\n"
                    " \"strategyName\": \"BottomUp\"\n"
                    "}";
      web::json::value json_par, json_return;
      json_par.parse("{\"userQuery\": \"example\", \"strategyName\": \"BottomUp\"}");

      web::http::client::http_client client("http://localhost:8081/v1/iotdb/service/execute-query");
      client.request(web::http::methods::POST, U("/"), body)
          .then([](const web::http::http_response &response) {
            return response.extract_json();
          })
          .then([&json_return](const pplx::task<web::json::value> &task) {
            try {
              json_return = task.get();
            }
            catch (const web::http::http_exception &e) {
              std::cout << "error " << e.what() << std::endl;
            }
          })
          .wait();

      string queryId = json_return.at("queryId").as_string();
      std::cout << "Query ID: " << queryId << std::endl;
      EXPECT_TRUE(!queryId.empty());

      cout << "Killing worker process->PID: " << pidW << endl;
      kill(pidW, SIGKILL);

      cout << "Killing coordinator process->PID: " << pidC << endl;
      kill(pidC, SIGKILL);
    } else {
      const char executableC[] = "../nesWorker";
      execlp(executableC, executableC, NULL);
    }
  } else {
    const char executableC[] = "../nesCoordinator";
    execlp(executableC, executableC, NULL);
  }
}

}