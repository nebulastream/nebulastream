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

TEST_F(ContiniousSourceTest, DISABLED_testMultipleOutputBufferFromDefaultSourcePrint) {
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
  conf.numberOfBuffersToProduce = 5;
  conf.sourceFrequency = 1;
  wrk->registerPhysicalStream(conf);

  //register query
  std::string queryString =
      "InputQuery::from(testStream).filter(testStream[\"campaign_id\"] > 42).print(std::cout); ";
  std::string id = crd->executeQuery(queryString, "BottomUp");
  EXPECT_NE(id, "");

  sleep(2);
  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(ContiniousSourceTest, DISABLED_testMultipleOutputBufferFromDefaultSourceWriteFile) {
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
  conf.numberOfBuffersToProduce = 5;
  conf.sourceFrequency = 1;
  wrk->registerPhysicalStream(conf);

  std::string outputFilePath = "blob.txt";
  remove(outputFilePath.c_str());

  //register query
  std::string queryString =
      "InputQuery::from(testStream).filter(testStream[\"campaign_id\"] > 42).writeToFile(\""
          + outputFilePath + "\"); ";
  std::string id = crd->executeQuery(queryString, "BottomUp");
  EXPECT_NE(id, "");

  sleep(2);
  std::ifstream ifs(outputFilePath.c_str());
  EXPECT_TRUE(ifs.good());
  std::string content((std::istreambuf_iterator<char>(ifs)),
                      (std::istreambuf_iterator<char>()));

  string expectedContent =
      "+----------------------------------------------------+\n"
          "|campaign_id:UINT64|\n"
          "+----------------------------------------------------+\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "+----------------------------------------------------++----------------------------------------------------+\n"
          "|campaign_id:UINT64|\n"
          "+----------------------------------------------------+\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "+----------------------------------------------------++----------------------------------------------------+\n"
          "|campaign_id:UINT64|\n"
          "+----------------------------------------------------+\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "+----------------------------------------------------++----------------------------------------------------+\n"
          "|campaign_id:UINT64|\n"
          "+----------------------------------------------------+\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "+----------------------------------------------------++----------------------------------------------------+\n"
          "|campaign_id:UINT64|\n"
          "+----------------------------------------------------+\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "|1|\n"
          "+----------------------------------------------------+";
  cout << "content=" << content << endl;
  cout << "expContent=" << expectedContent << endl;
  EXPECT_EQ(content, expectedContent);

  int response = remove(outputFilePath.c_str());
  EXPECT_TRUE(response == 0);

  sleep(2);
  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(ContiniousSourceTest, DISABLED_testMultipleOutputBufferFromCSVSourcePrint) {
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
      "Schema schema = Schema::create().addField(createField(\"val1\", UINT64))."
          "addField(createField(\"val2\", UINT64))."
          "addField(createField(\"val3\", UINT64));";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();
  wrk->registerLogicalStream("testStream", testSchemaFileName);

  std::string testCSV = "1,2,3\n"
      "1,2,4\n"
      "4,3,6";
  std::string testCSVFileName = "testCSV.csv";
  std::ofstream outCsv(testCSVFileName);
  outCsv << testCSV;
  outCsv.close();

  //register physical stream
  PhysicalStreamConfig conf;
  conf.logicalStreamName = "testStream";
  conf.physicalStreamName = "physical_test";
  conf.sourceType = "CSVSource";
  conf.sourceConfig = "testCSV.csv";
  conf.numberOfBuffersToProduce = 3;
  conf.sourceFrequency = 1;
  wrk->registerPhysicalStream(conf);

  //register query
  std::string queryString =
      "InputQuery::from(testStream).filter(testStream[\"val1\"] < 2).print(std::cout); ";
  std::string id = crd->executeQuery(queryString, "BottomUp");
  EXPECT_NE(id, "");

  sleep(2);
  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromCSVSourceWrite) {
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
      "Schema schema = Schema::create().addField(createField(\"val1\", UINT64))."
          "addField(createField(\"val2\", UINT64))."
          "addField(createField(\"val3\", UINT64));";
  std::string testSchemaFileName = "testSchema.hpp";
  std::ofstream out(testSchemaFileName);
  out << testSchema;
  out.close();
  wrk->registerLogicalStream("testStream", testSchemaFileName);

  std::string testCSV = "1,2,3\n"
      "1,2,4\n"
      "4,3,6";
  std::string testCSVFileName = "testCSV.csv";
  std::ofstream outCsv(testCSVFileName);
  outCsv << testCSV;
  outCsv.close();

  //register physical stream
  PhysicalStreamConfig conf;
  conf.logicalStreamName = "testStream";
  conf.physicalStreamName = "physical_test";
  conf.sourceType = "CSVSource";
  conf.sourceConfig = "testCSV.csv,3";
  wrk->registerPhysicalStream(conf);

  std::string outputFilePath =
      "testMultipleOutputBufferFromCSVSourceWriteTest.out";
  remove(outputFilePath.c_str());

  //register query
  std::string queryString =
      "InputQuery::from(testStream).filter(testStream[\"val1\"] < 10).writeToFile(\""
          + outputFilePath + "\"); ";

  std::string id = crd->executeQuery(queryString, "BottomUp");
  EXPECT_NE(id, "");
  sleep(6);

  std::ifstream ifs(outputFilePath.c_str());
  EXPECT_TRUE(ifs.good());
  std::string content((std::istreambuf_iterator<char>(ifs)),
                      (std::istreambuf_iterator<char>()));

  string expectedContent =
      "+----------------------------------------------------+\n"
          "|val1:UINT64|val2:UINT64|val3:UINT64|\n"
          "+----------------------------------------------------+\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "+----------------------------------------------------++----------------------------------------------------+\n"
          "|val1:UINT64|val2:UINT64|val3:UINT64|\n"
          "+----------------------------------------------------+\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "+----------------------------------------------------++----------------------------------------------------+\n"
          "|val1:UINT64|val2:UINT64|val3:UINT64|\n"
          "+----------------------------------------------------+\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "|4|3|6|\n"
          "|1|2|3|\n"
          "|1|2|4|\n"
          "+----------------------------------------------------+";
  cout << "content=" << content << endl;
  cout << "expContent=" << expectedContent << endl;
  EXPECT_EQ(content, expectedContent);

//  int response = remove(outputFilePath.c_str());
//  EXPECT_TRUE(response == 0);

  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

}
