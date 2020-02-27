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
//    NESLogger->setLevel(log4cxx::Level::getInfo());

// add appenders and other will inherit the settings
    NESLogger->addAppender(file);
    NESLogger->addAppender(console);
  }
};

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromDefaultSourcePrintForExdra) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  EXPECT_NE(port, 0);
  cout << "coordinator started successfully" << endl;
  sleep(1);

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  bool retStart = wrk->start(/**blocking**/false, /**withConnect**/false, port);
  EXPECT_TRUE(retStart);
  cout << "worker started successfully" << endl;

  bool retConWrk = wrk->connect();
  EXPECT_TRUE(retConWrk);
  cout << "worker connected " << endl;

  //register physical stream
  PhysicalStreamConfig conf;
  conf.logicalStreamName = "exdra";
  conf.physicalStreamName = "test_stream";
  conf.sourceType = "CSVSource";
  conf.sourceConfig = "../tests/test_data/exdra.csv";
  conf.numberOfBuffersToProduce = 1;
  conf.sourceFrequency = 1;
  wrk->registerPhysicalStream(conf);

  std::string filePath = "contTestOut.csv";
  remove(filePath.c_str());

  //register query
  std::string queryString = "InputQuery::from(exdra).writeToCSVFile(\""
      + filePath + "\");";

  std::string id = crd->executeQuery(queryString, "BottomUp");
  sleep(2);
  EXPECT_NE(id, "");

  string expectedContent =
      "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n";

  std::ifstream ifs(filePath.c_str());
  EXPECT_TRUE(ifs.good());
  std::string content((std::istreambuf_iterator<char>(ifs)),
                      (std::istreambuf_iterator<char>()));

  cout << "content=" << content << endl;
  cout << "expContent=" << expectedContent << endl;

  EXPECT_EQ(content, expectedContent);


  bool retStopWrk = wrk->stop();
  EXPECT_TRUE(retStopWrk);

  sleep(1);
  bool retStopCord = crd->stopCoordinator();
  EXPECT_TRUE(retStopCord);
}

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromDefaultSourcePrint) {
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
  conf.numberOfBuffersToProduce = 3;
  conf.sourceFrequency = 1;
  wrk->registerPhysicalStream(conf);

  std::string outputFilePath =
      "testMultipleOutputBufferFromDefaultSourceWriteFileOutput.txt";
  remove(outputFilePath.c_str());

  //register query
  std::string queryString =
      "InputQuery::from(testStream).filter(testStream[\"campaign_id\"] < 42).writeToFile(\""
          + outputFilePath + "\"); ";
  std::string id = crd->executeQuery(queryString, "BottomUp");
  EXPECT_NE(id, "");

  sleep(5);
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
          "+----------------------------------------------------+";

  cout << "content=" << content << endl;
  cout << "expContent=" << expectedContent << endl;

  std::string testOut = "expect.txt";
  std::ofstream outT(testOut);
  outT << expectedContent;
  outT.close();

  EXPECT_EQ(content, expectedContent);

//  int response = remove(outputFilePath.c_str());
//  EXPECT_TRUE(response == 0);

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

TEST_F(ContiniousSourceTest, DISABLED_testMultipleOutputBufferFromCSVSourceWrite) {
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
