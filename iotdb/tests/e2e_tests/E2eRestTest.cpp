#include <gtest/gtest.h>
#include <Util/Logger.hpp>


namespace iotdb {

class E2eRestTest : public testing::Test {
 public:
  std::string host = "localhost";

  static void SetUpTestCase() {
    setupLogging();
    IOTDB_INFO("Setup ActorCoordinatorWorkerTest test class.");
  }

  static void TearDownTestCase() { std::cout << "Tear down ActorCoordinatorWorkerTest test class." << std::endl; }
 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

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

}