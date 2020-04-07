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
    NES::setupLogging("ContiniousSourceTest.log", NES::LOG_DEBUG);
    NES_INFO("Setup ContiniousSourceTest test class.");
  }
  static void TearDownTestCase() {
    std::cout << "Tear down ContiniousSourceTest class." << std::endl;
  }

};

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteToCSVFileForExdra) {
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
      "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
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
      "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
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
      "Schema::create()->addField(createField(\"val1\", UINT64))."
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
      "Schema::create()->addField(createField(\"val1\", UINT64))."
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

/**
 * TODO: the test itself is running in isolation but the network stack has a problem
 * Mar 05 2020 12:13:49 NES:116 [0x7fc7a87e0700] [ERROR] : ZMQSOURCE: Address already in use
 * Mar 05 2020 12:13:49 NES:124 [0x7fc7a87e0700] [DEBUG] : Exception: ZMQSOURCE  0x7fc7e0005b50: NOT connected
 * Mar 05 2020 12:13:49 NES:89 [0x7fc7a87e0700] [ERROR] : ZMQSOURCE: Not connected!
 * Once we fixed this we can activate this tests
 * */

TEST_F(ContiniousSourceTest, DISABLED_testExdraUseCaseWithOutput) {
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

  PhysicalStreamConfig conf;
  conf.logicalStreamName = "exdra";
  conf.physicalStreamName = "test_stream";
  conf.sourceType = "CSVSource";
  conf.sourceConfig = "tests/test_data/exdra.csv";
  conf.numberOfBuffersToProduce = 15;
  conf.sourceFrequency = 0;
  wrk->registerPhysicalStream(conf);

  std::string outputFilePath = "testExdraUseCaseWithOutput.csv";
  remove(outputFilePath.c_str());

  //register query
  std::string queryString = "InputQuery::from(exdra).writeToCSVFile(\""
      + outputFilePath + "\"); ";
  std::string id = crd->executeQuery(queryString, "BottomUp");
  EXPECT_NE(id, "");

  sleep(5);
  std::ifstream ifs(outputFilePath.c_str());
  EXPECT_TRUE(ifs.good());
  std::string content((std::istreambuf_iterator<char>(ifs)),
                      (std::istreambuf_iterator<char>()));

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
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f3c1611f-db1c-49bf-9376-3ccae7248644,Features,3593,0.586449,597841.062500,1262300400000,1262308953634,electricityGeneration,Point,13.546017,47.870770,2ab9f413-848c-4ab4-a386-8615426e5c47\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,a72db07d-5c8c-4924-aeb3-e28c0bff1b2f,Features,3889,0.166697,56208.464844,1262300400000,1262308957774,electricityGeneration,Point,11.567608,48.489555,467c65ac-7679-4727-a05b-256571b91a46\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,84f00be9-c353-442d-9356-ff0fb1caa2fb,Features,2235,0.166961,299295.375000,1262300400000,0,electricityGeneration,Point,12.368753,52.965977,9792b6cf-63cf-4baa-ab28-5bd6ff04189f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e43f634d-9869-4d96-89db-0dc66a2b5134,Features,2764,0.444815,724771.125000,1262300400000,0,electricityGeneration,Point,11.567235,50.497688,a2db5a94-3bde-48f8-b730-ed74c7308db3\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,4efb382a-03d6-42a0-b55c-a033cc7209e2,Features,4140,0.369035,1123561.250000,1262300400000,0,electricityGeneration,Point,9.361658,51.083794,3c3cd5d7-4f19-4f89-bc14-f9328fce6b5e\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,33f13223-a0db-4246-9abb-2bf48ca78d94,Features,2338,0.610958,84045.023438,1262300400000,1262309011585,electricityGeneration,Point,7.109556,49.864002,d10f3a8e-8acd-453a-9961-b135104d4998\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,524d29d6-4d8f-4f8b-b3b4-9c55dd2cdcb4,Features,611,0.709108,46676.929688,1262300400000,1262308305059,electricityGeneration,Point,9.864325,48.852226,9d6d6d98-da5c-4d54-94ee-6686cabc7b74\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,68430660-21bf-42c5-affa-5827530fb389,Features,4526,0.463146,1461850.875000,1262300400000,1262310834053,electricityGeneration,Point,8.018541,47.056126,05a44117-f261-4fed-90c9-3211c4324b14\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,1bfe930f-8aeb-449f-88b4-ec668aaadfbe,Features,555,0.726571,24144.589844,1262300400000,0,electricityGeneration,Point,10.453132,47.310814,ff331964-4c3f-465e-8bba-f6f399d51fd1\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,0100f145-af35-4c8e-bde7-7e044460cc95,Features,4102,0.707496,2553631.000000,1262300400000,0,electricityGeneration,Point,11.338407,50.952404,a56a07d4-fc60-48ba-9566-3378abec4254\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,97c55e13-d012-4915-985f-8e4a06ba1b1c,Features,3545,0.504641,1398080.750000,1262300400000,1262307112121,electricityGeneration,Point,10.352886,48.668373,a5243a27-95e0-48da-8545-fe73e86df85f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,79bb3cbb-33ee-4987-b84e-868d84879f1a,Features,1375,0.197728,9817.749023,1262300400000,1262308899035,electricityGeneration,Point,12.566253,51.012245,7685e8e1-6093-46d2-9e48-e132947ee4b5\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,1253878c-6a26-4b6a-8d22-e69b435e4e2d,Features,743,0.307614,207831.921875,1262300400000,0,electricityGeneration,Point,9.744993,53.149342,d5307405-8eae-42b8-99c4-f25b58a8b657\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,0dd3d392-c263-47c3-bee9-51a63ac5db94,Features,3726,0.511317,1555320.875000,1262300400000,0,electricityGeneration,Point,14.751818,48.002819,2ea64ac9-b555-470d-9ab2-84203f466c51\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b2d13711-677d-4741-8a57-ddba4bba4b92,Features,3918,0.462302,671934.312500,1262300400000,1262308605591,electricityGeneration,Point,6.650259,51.014507,d178b86b-1a6b-4ce3-9e9e-a7997655037b\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d560707b-9fda-48ce-9064-847510c52110,Features,696,0.444002,57064.187500,1262300400000,0,electricityGeneration,Point,6.165246,49.883900,d8ca6dac-26e7-45fd-906e-4f0d390910e9\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,7c32ce92-273f-4527-bc91-85aa02a6208a,Features,3939,0.391640,757719.812500,1262300400000,1262308344641,electricityGeneration,Point,13.307511,50.248199,f9d975cd-8dae-4784-b008-2e12dc0bf1f9\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b4709bd0-4731-461c-885d-b68653c1f756,Features,583,0.747267,26190.484375,1262300400000,0,electricityGeneration,Point,8.332189,49.580208,92a7aa0f-442c-42bd-b7f6-78ef456dc154\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,406ceb11-6f75-45bf-af71-94e53429168e,Features,1835,0.175036,174939.437500,1262300400000,0,electricityGeneration,Point,10.509096,47.012241,029254c7-e10e-4ec7-a680-7b8293e25673\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,ecc5b92b-c7ad-4cc2-9a46-1504d5e97680,Features,3921,0.700885,378533.375000,1262300400000,0,electricityGeneration,Point,13.868662,51.350586,776fd1a1-22e5-47d2-870b-ba4f65bdf2b4\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,fb874982-bb70-4e32-809a-37bcec534ace,Features,1085,0.566170,515900.656250,1262300400000,0,electricityGeneration,Point,8.887708,48.364704,6965897b-d862-4696-bb4f-14f02e67bb5a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,44a74993-38f6-4cb8-a490-3ea45f5624c5,Features,4621,0.734452,1895833.875000,1262300400000,0,electricityGeneration,Point,9.767467,48.254379,be954fc0-951e-46c9-ac5f-6879ffe17a3b\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,976fd9e0-a92e-4f70-a7d3-b30a64775b8a,Features,2699,0.680642,1557769.875000,1262300400000,0,electricityGeneration,Point,6.493205,47.853325,9879d282-0e69-4380-99eb-efb970732240\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,c793b51a-ade5-44c7-9cb7-ea53e700e909,Features,3740,0.492986,315925.000000,1262300400000,0,electricityGeneration,Point,14.829474,50.472370,4708c63c-a810-4706-95e3-3325ea784191\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d21a9feb-34e5-41ab-b760-bc4b9ebd0f5b,Features,994,0.179512,152478.765625,1262300400000,1262308193143,electricityGeneration,Point,6.928825,47.989059,70b3198e-0506-43fc-8945-31ad55ae94ad\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6283f5a4-2521-454f-be07-4906948332ae,Features,3518,0.644202,519540.968750,1262300400000,0,electricityGeneration,Point,7.623646,51.626190,a75f2670-7a3d-4312-b83a-349fa2f92ba9\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f1699072-bad2-4cdd-8ae8-23e9a2c67bee,Features,3399,0.524546,182178.984375,1262300400000,1262310176867,electricityGeneration,Point,12.102494,47.198128,bc53dce6-6c9d-47b5-bbf1-ac4e82376018\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e9dbcd00-81df-4aac-bc09-599cc0834037,Features,3465,0.431836,749369.937500,1262300400000,1262308476304,electricityGeneration,Point,9.898040,50.971878,64091332-18e1-4470-9297-3779630f8879\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,389158af-ee0f-483c-919a-0c2ee79fd8b2,Features,1944,0.370617,302534.343750,1262300400000,0,electricityGeneration,Point,13.977706,51.996136,3ec97a0a-05c0-4d85-a3fa-7744279bdd2c\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bde5f5b9-97bc-4562-a767-4b919ff937fa,Features,4533,0.650272,2762389.250000,1262300400000,1262309669595,electricityGeneration,Point,14.582145,51.912815,c6661366-6f3a-4953-b052-fe0c89b58013\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,c91eccc9-ddb9-4128-9d18-131b2c48e695,Features,901,0.373627,298923.625000,1262300400000,0,electricityGeneration,Point,9.940279,51.576477,07cd5e06-4ce1-475c-b504-7730fa113d9f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bf7786ad-dcbb-444c-9a99-a19c8465e5ed,Features,3007,0.706310,499785.343750,1262300400000,1262307493613,electricityGeneration,Point,8.272306,52.517452,e1abec7b-ffc3-460a-985e-d6e761ba579f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,a0e8fb98-7d9c-4c01-92f6-bbc0f1aec3f1,Features,3806,0.699387,466838.625000,1262300400000,1262311546817,electricityGeneration,Point,8.695134,52.011230,bc16682d-50f0-4153-a7ea-67ae3dc71b8e\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,2d201cc7-af8e-4969-8e69-6c37ae1ed7d0,Features,3076,0.523115,1391626.750000,1262300400000,1262307025541,electricityGeneration,Point,6.590214,53.908199,737554dc-245d-4759-a2f6-a34a95eebf3a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,55eb25bd-df5f-4ab4-8932-385362fe573c,Features,3906,0.258830,813787.125000,1262300400000,1262310287664,electricityGeneration,Point,6.550814,47.035313,8b2e1875-f1e5-4ee6-905a-197f0a8ad85b\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e3c5226e-9e52-4d37-acff-7d8d80964c4c,Features,4965,0.629812,1196946.625000,1262300400000,1262309439602,electricityGeneration,Point,6.693268,48.838722,54e5bfb8-0262-41ba-8810-3b55e7b89a47\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,8bfd6ef9-2c29-483a-9d3a-d925211ed504,Features,4543,0.556731,1897101.375000,1262300400000,0,electricityGeneration,Point,9.791286,52.406342,58ec269d-aefc-4bd8-a443-dc4d40c36c94\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,0609dfe6-2831-4bbf-a889-06dfb1311048,Features,3202,0.373316,183261.390625,1262300400000,1262309963720,electricityGeneration,Point,13.295279,49.204369,7fdd95bc-388f-4d7c-90b1-b6c48db95719\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f8884b58-e72d-476d-9ef3-be2c87d8ace7,Features,3386,0.619704,941008.937500,1262300400000,1262310067980,electricityGeneration,Point,14.851712,48.742081,292af9eb-07a1-41c5-a924-98f881649f67\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bdd443b2-f41a-4b72-b7cc-412c35191e36,Features,4328,0.531274,2089989.875000,1262300400000,0,electricityGeneration,Point,9.861691,53.746456,d628528d-2be3-48a4-b4ed-3124907e4aa0\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,7e22071d-b581-413b-af32-57684cc102ea,Features,762,0.514914,207737.578125,1262300400000,1262311328129,electricityGeneration,Point,14.216938,47.631836,cae16bc0-b1a2-4363-ad61-d2399b607b62\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,4d131ca6-70db-4839-90c1-22790f14cb13,Features,2018,0.599085,1139665.875000,1262300400000,1262307361988,electricityGeneration,Point,8.132144,48.391132,9d29683f-fa8c-45b7-a560-b1fb03dd411b\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,fd9332da-ced9-4133-9ff9-32d0bce3885b,Features,2939,0.211631,183620.531250,1262300400000,0,electricityGeneration,Point,8.960206,50.237034,75b0ff8b-4262-4af3-9cab-4519ecd786b0\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,22f5331a-43d5-45b4-bd4a-0664513abdd7,Features,1495,0.246575,38610.152344,1262300400000,1262310941173,electricityGeneration,Point,7.891750,48.062241,e62a23ff-92bb-4583-8074-e5c8759610ea\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,78e5aa43-1a97-4584-81fb-536cfc1199f6,Features,1763,0.239781,114225.710938,1262300400000,1262308361741,electricityGeneration,Point,13.638403,51.911674,c6c7b9bd-3396-48b3-9d91-beb5843e1c40\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,1a35c44b-0c4f-485b-9924-caa37d7f9dab,Features,762,0.563246,82437.242188,1262300400000,1262306739201,electricityGeneration,Point,13.871878,49.272213,c07fdb9f-5b60-42ae-afaa-28b37032bff6\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,00610af0-5377-4405-b578-527854330585,Features,3199,0.413862,1243197.875000,1262300400000,1262309526062,electricityGeneration,Point,10.860796,50.605858,e4a800cb-feb1-4669-84ee-2f3e1dfb98f9\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,3f219f2b-0edc-4b69-a4bb-bced0134af67,Features,1214,0.732087,680409.375000,1262300400000,0,electricityGeneration,Point,13.707614,50.203930,aa5098b9-ff8a-4702-813a-b7e6ccc9990b\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,2cb7f819-bedd-4eb9-80f5-06122e9ce3f3,Features,2016,0.503426,131663.906250,1262300400000,1262310443638,electricityGeneration,Point,12.776254,49.438873,a0923be7-c6af-496b-b309-8325ef2760da\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,82c33e7f-ab89-4bf3-ae2e-68d1fe0ce931,Features,2807,0.435685,1109229.375000,1262300400000,0,electricityGeneration,Point,6.651717,48.822651,ee8ca78e-1b97-499e-80c4-d7ab1bd0076d\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,cc408021-ab1e-405a-a2b1-6c51f61c7583,Features,4005,0.532582,785022.812500,1262300400000,0,electricityGeneration,Point,9.439747,52.405857,22d13e55-a086-459d-b5f8-1ffcefcb4876\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e08403e5-20e7-4fc3-8156-a767a078e30c,Features,3384,0.305542,968553.625000,1262300400000,0,electricityGeneration,Point,9.073061,48.951935,7ca2e409-c456-41f9-acd0-081a2cb269b8\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,8a8c9304-c0ba-4a18-afc4-2b1beea6d51a,Features,1574,0.415040,605328.687500,1262300400000,1262308871955,electricityGeneration,Point,6.617554,52.175682,1e3e5cea-6d47-4a5f-92cf-84cd30f54e5a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,2e8ea438-a480-4297-82a8-f31041a9a5a5,Features,2096,0.720224,986658.500000,1262300400000,0,electricityGeneration,Point,11.620898,49.461037,3a8982b4-6dab-4ba2-8180-7b86535b57fb\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,8ea43833-419b-4365-b764-69766b420646,Features,3444,0.338585,262592.718750,1262300400000,0,electricityGeneration,Point,12.794129,52.073780,4a1064e6-155c-4fed-a3b4-5ed047626b5d\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,abfc641c-2f99-46ba-bef2-82258796cab4,Features,3424,0.198125,155096.765625,1262300400000,0,electricityGeneration,Point,8.030875,47.599319,704651b8-b08b-4d17-aac9-525dddfa7e3d\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f83fa64c-53a6-4969-9b93-ebb1c782b3e6,Features,4416,0.618156,1906904.750000,1262300400000,0,electricityGeneration,Point,14.851306,52.440098,814e0638-648e-46ae-8ce4-4f0617050724\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,fb19332a-8651-4e4f-b469-8cef89a35fa2,Features,1656,0.316497,255964.234375,1262300400000,0,electricityGeneration,Point,14.350515,48.770107,960d9f48-8063-4300-8667-e05781927648\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d1f39f13-8bbb-45a7-af77-e1486182a76d,Features,2498,0.736275,446404.968750,1262300400000,0,electricityGeneration,Point,8.600543,49.024426,6881feb5-ad5d-4dd0-bf66-7bdae0866e42\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,2a82879d-690d-41d9-8a0a-c4b4b06d23ae,Features,4220,0.532366,2147106.500000,1262300400000,0,electricityGeneration,Point,10.272394,47.566257,30fb28d5-71da-4f28-805f-35fa95bf01e8\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,3653295c-e21f-42b4-90d1-37470fb2e882,Features,4035,0.429390,1514562.500000,1262300400000,0,electricityGeneration,Point,11.302599,52.728168,18c07163-519f-4215-b70e-4f2a663f5adc\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,4f96ed02-f939-443a-8402-04489dcebd67,Features,1435,0.362225,354742.156250,1262300400000,0,electricityGeneration,Point,8.258130,51.880680,00f63765-e613-4acb-9a31-fa1a9cf63cf3\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,5b608634-d5fe-4427-aa17-089768d7ffb9,Features,4357,0.555992,708950.875000,1262300400000,1262308221374,electricityGeneration,Point,6.673089,52.674149,3f047bf5-7bc9-4267-a18a-1311a78965b9\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,74da2845-5d4a-44b5-a67f-56570ff52edc,Features,4919,0.567656,1684772.250000,1262300400000,0,electricityGeneration,Point,14.166148,48.159855,ee36ff17-0608-46de-9f39-7a02846f45fd\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,ee528055-082c-443d-acd1-8db77f1bafa6,Features,4251,0.700234,1860075.500000,1262300400000,0,electricityGeneration,Point,10.980513,49.872688,bef76478-20eb-46d1-af74-7b81d2b5c599\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,1257eb6d-5d03-45fc-8dc9-7ff15a906fce,Features,3685,0.310321,994094.750000,1262300400000,0,electricityGeneration,Point,11.910775,48.092022,d1317a8b-66e2-4987-bf4d-24ea653830cd\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,35b1cc44-e0cc-4ce6-b5ec-f3dcf03ad8d2,Features,2221,0.446961,739070.250000,1262300400000,0,electricityGeneration,Point,7.195913,47.872860,08205342-4c1b-4be7-bcd6-16b56a0104f2\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,13b1c3a7-94bf-46ea-808b-eecc16b7a1fc,Features,2283,0.689515,591981.437500,1262300400000,0,electricityGeneration,Point,8.155056,48.508060,d0f1ac76-c9ad-4c5c-8c2a-b92f4d0eb353\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f3438251-e497-4563-bb1a-06e1773531ba,Features,3201,0.440628,301367.625000,1262300400000,1262309218750,electricityGeneration,Point,10.719861,51.325111,ace56bec-304a-48d0-ae26-c475f3431f82\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,7692ea1f-fa1a-45de-bc06-76cf99a4897b,Features,2002,0.294733,291851.906250,1262300400000,0,electricityGeneration,Point,7.859082,51.883984,f8815c08-daeb-44a1-bc49-6f0d2239b241\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,dd31f3ee-dbe9-438e-a368-c50480b1f918,Features,1713,0.305907,37421.101562,1262300400000,1262309768913,electricityGeneration,Point,8.972678,53.336201,62120fb0-47b8-46e7-adca-0aacb9a9bb26\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,df525aea-09d6-4ab3-a9aa-7024b2db9b80,Features,768,0.550902,351724.968750,1262300400000,0,electricityGeneration,Point,8.319209,53.896843,c831b602-5e09-4098-a375-64966047edc4\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,9485f5c0-5afb-4107-aa00-6ee52c316d8f,Features,2877,0.155313,181713.109375,1262300400000,0,electricityGeneration,Point,14.077276,52.798340,7f04c5b5-98d5-4b73-95c1-b254988f6c9e\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,1d2d9d17-86e1-44de-9f98-313f5a3c6973,Features,2975,0.735917,1527066.625000,1262300400000,0,electricityGeneration,Point,9.671958,47.821461,d673dae3-7030-459e-a553-65f914064baa\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,3b0db418-e709-4c76-85da-6d5b6a7043b1,Features,4441,0.508482,2024312.250000,1262300400000,1262309547362,electricityGeneration,Point,7.051106,48.066406,0336f063-c860-42d1-9a8f-d2d28719bec7\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,367c2c33-3c31-4bac-a966-589722c83a15,Features,3443,0.409850,1203534.500000,1262300400000,0,electricityGeneration,Point,11.630854,48.724480,531d6847-7cf9-4d37-ae4c-fc39c510f9ec\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,2a4b9ae3-3490-40ab-a06b-d14ab5207363,Features,2634,0.692026,481841.156250,1262300400000,0,electricityGeneration,Point,13.935438,51.721245,d391a4af-9c98-45ab-beed-c55b50b3c4a1\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,a7445bc7-ce7f-4598-bec0-e1a950435bc4,Features,3862,0.390571,2760.349854,1262300400000,0,electricityGeneration,Point,14.067351,50.993282,e2ee176a-236d-4b39-8077-d447651fa638\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,4d58ae12-b91f-4be7-827b-479a44f93d9c,Features,2989,0.172930,475641.750000,1262300400000,1262308041841,electricityGeneration,Point,10.232992,51.083351,49d8775d-934f-4bfe-9822-c19f04ecf092\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f4a2ab7c-41f9-4e78-911d-f5f3792930fa,Features,3695,0.728941,1258865.500000,1262300400000,1262306412419,electricityGeneration,Point,14.153470,50.655006,80161373-d3d1-4dfc-8c2c-d3dfa6d5a269\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e4a89bb2-cefb-4ad7-aed1-a97ae84ccde1,Features,835,0.571583,419273.562500,1262300400000,1262307797771,electricityGeneration,Point,7.115444,53.308601,b43752e6-2260-4946-afa4-b3e16c4bb823\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,c090d8a6-1d2f-4bdb-9d0b-8b51c6b25577,Features,2250,0.654423,657537.000000,1262300400000,1262311310933,electricityGeneration,Point,12.069178,53.135468,4a7ba5a7-ab21-4aad-b27d-f6794775c72f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,65e69a43-29d5-4e42-85c7-78c994f07b00,Features,1449,0.405131,247325.937500,1262300400000,0,electricityGeneration,Point,9.916399,49.725361,9224cbe0-cbf4-4ec3-93ea-612f7de8a977\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,5b017793-ea9d-4977-aa60-a97ceaa6b74f,Features,1274,0.156279,51367.062500,1262300400000,0,electricityGeneration,Point,14.665415,50.566204,f36d5057-6f4d-46e3-9777-50591c26f06a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,8d1709ad-0bc0-484c-a8ef-127f6afcbaeb,Features,2640,0.170270,391733.687500,1262300400000,0,electricityGeneration,Point,9.300010,53.038376,d676c1e0-93a0-443d-abce-2b54658eefad\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b047c787-20f3-4428-bf29-aaee6346b815,Features,1393,0.606852,28700.644531,1262300400000,0,electricityGeneration,Point,6.794758,49.562744,1c15eaad-fa36-40ad-a8c4-3e2f219952de\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,2064d130-28be-48de-b1b3-63d339538f8f,Features,1193,0.704903,227967.921875,1262300400000,0,electricityGeneration,Point,9.902015,52.466663,394d3e52-33ab-4814-babc-764676a1f5f1\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,a5930f69-57fe-420b-ad81-5d2f03f77090,Features,1105,0.515403,216150.734375,1262300400000,0,electricityGeneration,Point,12.196718,53.856037,418a81f0-f9df-4f08-b87f-deda4cc8946\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f3c1611f-db1c-49bf-9376-3ccae7248644,Features,3593,0.586449,597841.062500,1262300400000,1262308953634,electricityGeneration,Point,13.546017,47.870770,2ab9f413-848c-4ab4-a386-8615426e5c47\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,a72db07d-5c8c-4924-aeb3-e28c0bff1b2f,Features,3889,0.166697,56208.464844,1262300400000,1262308957774,electricityGeneration,Point,11.567608,48.489555,467c65ac-7679-4727-a05b-256571b91a46\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,84f00be9-c353-442d-9356-ff0fb1caa2fb,Features,2235,0.166961,299295.375000,1262300400000,0,electricityGeneration,Point,12.368753,52.965977,9792b6cf-63cf-4baa-ab28-5bd6ff04189f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e43f634d-9869-4d96-89db-0dc66a2b5134,Features,2764,0.444815,724771.125000,1262300400000,0,electricityGeneration,Point,11.567235,50.497688,a2db5a94-3bde-48f8-b730-ed74c7308db3\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,4efb382a-03d6-42a0-b55c-a033cc7209e2,Features,4140,0.369035,1123561.250000,1262300400000,0,electricityGeneration,Point,9.361658,51.083794,3c3cd5d7-4f19-4f89-bc14-f9328fce6b5e\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,33f13223-a0db-4246-9abb-2bf48ca78d94,Features,2338,0.610958,84045.023438,1262300400000,1262309011585,electricityGeneration,Point,7.109556,49.864002,d10f3a8e-8acd-453a-9961-b135104d4998\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,524d29d6-4d8f-4f8b-b3b4-9c55dd2cdcb4,Features,611,0.709108,46676.929688,1262300400000,1262308305059,electricityGeneration,Point,9.864325,48.852226,9d6d6d98-da5c-4d54-94ee-6686cabc7b74\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,68430660-21bf-42c5-affa-5827530fb389,Features,4526,0.463146,1461850.875000,1262300400000,1262310834053,electricityGeneration,Point,8.018541,47.056126,05a44117-f261-4fed-90c9-3211c4324b14\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,1bfe930f-8aeb-449f-88b4-ec668aaadfbe,Features,555,0.726571,24144.589844,1262300400000,0,electricityGeneration,Point,10.453132,47.310814,ff331964-4c3f-465e-8bba-f6f399d51fd1\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,0100f145-af35-4c8e-bde7-7e044460cc95,Features,4102,0.707496,2553631.000000,1262300400000,0,electricityGeneration,Point,11.338407,50.952404,a56a07d4-fc60-48ba-9566-3378abec4254\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,97c55e13-d012-4915-985f-8e4a06ba1b1c,Features,3545,0.504641,1398080.750000,1262300400000,1262307112121,electricityGeneration,Point,10.352886,48.668373,a5243a27-95e0-48da-8545-fe73e86df85f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,79bb3cbb-33ee-4987-b84e-868d84879f1a,Features,1375,0.197728,9817.749023,1262300400000,1262308899035,electricityGeneration,Point,12.566253,51.012245,7685e8e1-6093-46d2-9e48-e132947ee4b5\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,1253878c-6a26-4b6a-8d22-e69b435e4e2d,Features,743,0.307614,207831.921875,1262300400000,0,electricityGeneration,Point,9.744993,53.149342,d5307405-8eae-42b8-99c4-f25b58a8b657\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,0dd3d392-c263-47c3-bee9-51a63ac5db94,Features,3726,0.511317,1555320.875000,1262300400000,0,electricityGeneration,Point,14.751818,48.002819,2ea64ac9-b555-470d-9ab2-84203f466c51\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b2d13711-677d-4741-8a57-ddba4bba4b92,Features,3918,0.462302,671934.312500,1262300400000,1262308605591,electricityGeneration,Point,6.650259,51.014507,d178b86b-1a6b-4ce3-9e9e-a7997655037b\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d560707b-9fda-48ce-9064-847510c52110,Features,696,0.444002,57064.187500,1262300400000,0,electricityGeneration,Point,6.165246,49.883900,d8ca6dac-26e7-45fd-906e-4f0d390910e9\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,7c32ce92-273f-4527-bc91-85aa02a6208a,Features,3939,0.391640,757719.812500,1262300400000,1262308344641,electricityGeneration,Point,13.307511,50.248199,f9d975cd-8dae-4784-b008-2e12dc0bf1f9\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,b4709bd0-4731-461c-885d-b68653c1f756,Features,583,0.747267,26190.484375,1262300400000,0,electricityGeneration,Point,8.332189,49.580208,92a7aa0f-442c-42bd-b7f6-78ef456dc154\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,406ceb11-6f75-45bf-af71-94e53429168e,Features,1835,0.175036,174939.437500,1262300400000,0,electricityGeneration,Point,10.509096,47.012241,029254c7-e10e-4ec7-a680-7b8293e25673\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,ecc5b92b-c7ad-4cc2-9a46-1504d5e97680,Features,3921,0.700885,378533.375000,1262300400000,0,electricityGeneration,Point,13.868662,51.350586,776fd1a1-22e5-47d2-870b-ba4f65bdf2b4\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,fb874982-bb70-4e32-809a-37bcec534ace,Features,1085,0.566170,515900.656250,1262300400000,0,electricityGeneration,Point,8.887708,48.364704,6965897b-d862-4696-bb4f-14f02e67bb5a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,44a74993-38f6-4cb8-a490-3ea45f5624c5,Features,4621,0.734452,1895833.875000,1262300400000,0,electricityGeneration,Point,9.767467,48.254379,be954fc0-951e-46c9-ac5f-6879ffe17a3b\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,976fd9e0-a92e-4f70-a7d3-b30a64775b8a,Features,2699,0.680642,1557769.875000,1262300400000,0,electricityGeneration,Point,6.493205,47.853325,9879d282-0e69-4380-99eb-efb970732240\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,c793b51a-ade5-44c7-9cb7-ea53e700e909,Features,3740,0.492986,315925.000000,1262300400000,0,electricityGeneration,Point,14.829474,50.472370,4708c63c-a810-4706-95e3-3325ea784191\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,d21a9feb-34e5-41ab-b760-bc4b9ebd0f5b,Features,994,0.179512,152478.765625,1262300400000,1262308193143,electricityGeneration,Point,6.928825,47.989059,70b3198e-0506-43fc-8945-31ad55ae94ad\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,6283f5a4-2521-454f-be07-4906948332ae,Features,3518,0.644202,519540.968750,1262300400000,0,electricityGeneration,Point,7.623646,51.626190,a75f2670-7a3d-4312-b83a-349fa2f92ba9\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f1699072-bad2-4cdd-8ae8-23e9a2c67bee,Features,3399,0.524546,182178.984375,1262300400000,1262310176867,electricityGeneration,Point,12.102494,47.198128,bc53dce6-6c9d-47b5-bbf1-ac4e82376018\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e9dbcd00-81df-4aac-bc09-599cc0834037,Features,3465,0.431836,749369.937500,1262300400000,1262308476304,electricityGeneration,Point,9.898040,50.971878,64091332-18e1-4470-9297-3779630f8879\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,389158af-ee0f-483c-919a-0c2ee79fd8b2,Features,1944,0.370617,302534.343750,1262300400000,0,electricityGeneration,Point,13.977706,51.996136,3ec97a0a-05c0-4d85-a3fa-7744279bdd2c\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bde5f5b9-97bc-4562-a767-4b919ff937fa,Features,4533,0.650272,2762389.250000,1262300400000,1262309669595,electricityGeneration,Point,14.582145,51.912815,c6661366-6f3a-4953-b052-fe0c89b58013\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,c91eccc9-ddb9-4128-9d18-131b2c48e695,Features,901,0.373627,298923.625000,1262300400000,0,electricityGeneration,Point,9.940279,51.576477,07cd5e06-4ce1-475c-b504-7730fa113d9f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bf7786ad-dcbb-444c-9a99-a19c8465e5ed,Features,3007,0.706310,499785.343750,1262300400000,1262307493613,electricityGeneration,Point,8.272306,52.517452,e1abec7b-ffc3-460a-985e-d6e761ba579f\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,a0e8fb98-7d9c-4c01-92f6-bbc0f1aec3f1,Features,3806,0.699387,466838.625000,1262300400000,1262311546817,electricityGeneration,Point,8.695134,52.011230,bc16682d-50f0-4153-a7ea-67ae3dc71b8e\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,2d201cc7-af8e-4969-8e69-6c37ae1ed7d0,Features,3076,0.523115,1391626.750000,1262300400000,1262307025541,electricityGeneration,Point,6.590214,53.908199,737554dc-245d-4759-a2f6-a34a95eebf3a\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,55eb25bd-df5f-4ab4-8932-385362fe573c,Features,3906,0.258830,813787.125000,1262300400000,1262310287664,electricityGeneration,Point,6.550814,47.035313,8b2e1875-f1e5-4ee6-905a-197f0a8ad85b\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e3c5226e-9e52-4d37-acff-7d8d80964c4c,Features,4965,0.629812,1196946.625000,1262300400000,1262309439602,electricityGeneration,Point,6.693268,48.838722,54e5bfb8-0262-41ba-8810-3b55e7b89a47\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,8bfd6ef9-2c29-483a-9d3a-d925211ed504,Features,4543,0.556731,1897101.375000,1262300400000,0,electricityGeneration,Point,9.791286,52.406342,58ec269d-aefc-4bd8-a443-dc4d40c36c94\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,0609dfe6-2831-4bbf-a889-06dfb1311048,Features,3202,0.373316,183261.390625,1262300400000,1262309963720,electricityGeneration,Point,13.295279,49.204369,7fdd95bc-388f-4d7c-90b1-b6c48db95719\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f8884b58-e72d-476d-9ef3-be2c87d8ace7,Features,3386,0.619704,941008.937500,1262300400000,1262310067980,electricityGeneration,Point,14.851712,48.742081,292af9eb-07a1-41c5-a924-98f881649f67\n"
          "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bdd443b2-f41a-4b72-b7cc-412c35191e36,Features,4328,0.531274,2089989.875000,1262300400000,0,electricityGeneration,Point,9.861691,53.746456,d628528d-2be3-48a4-b4ed-3124907e4aa0\n";

  cout << "content=" << content << endl;
  cout << "expContent=" << expectedContent << endl;

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

}
