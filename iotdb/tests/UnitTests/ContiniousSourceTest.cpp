#include <cassert>
#include <iostream>

#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <sstream>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>

using namespace std;

#define DEBUG_OUTPUT
namespace NES {

class ContiniousSourceTest : public testing::Test {
 public:
  static void SetUpTestCase() {
#ifdef DEBUG_OUTPUT
    setupLogging();
#endif
    NES_INFO("Setup ContiniousSourceTest test class.");
  }
  static void TearDownTestCase() {
    std::cout << "Tear down ContiniousSourceTest class." << std::endl;
  }

 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout(
            "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "ContiniousSourceTest.log");
    log4cxx::FileAppenderPtr file(
        new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(
        new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    NESLogger->setLevel(log4cxx::Level::getDebug());
//    logger->setLevel(log4cxx::Level::getInfo());

// add appenders and other will inherit the settings
    NESLogger->addAppender(file);
    NESLogger->addAppender(console);
  }

};

TEST_F(ContiniousSourceTest, startWithInputFile) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  bool retStart = wrk->start(/**blocking**/false, /**withConnect**/false, port);
  EXPECT_TRUE(retStart);
  cout << "worker started successfully" << endl;

  bool retConWrk = wrk->connect();
  EXPECT_TRUE(retConWrk);
  cout << "worker connected " << endl;

  //register logical stream
  std::string testSchema =
      "Schema schema = Schema::create().addField(createField(\"campaign_id\", UINT64));";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();
  wrk->registerLogicalStream("testStream", testSchemaFileName);

  //register physical stream
  PhysicalStreamConfig conf;
  conf.logicalStreamName = "testStream";
  conf.physicalStreamName = "physical_test";
  conf.sourceType = "DefaultSource";
  conf.sourceConfig = "10";
  wrk->registerPhysicalStream(conf.sourceType, conf.sourceConfig, conf.physicalStreamName,
                conf.logicalStreamName);


  //register query
  std::string queryString =
        "InputQuery::from(testStream).filter(testStream[\"campaign_id\"] > 42).print(std::cout); ";
  std::string id = crd->executeQuery(queryString, "BottomUp");
  EXPECT_NE(id, "");

  sleep(10);
  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}


}
