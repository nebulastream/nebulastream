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
#include <Util/TestUtils.hpp>

using namespace std;

#define DEBUG_OUTPUT
namespace NES {

class ContiniousSourceTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("ContiniousSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ContiniousSourceTest test class.");
    }
    void TearDown() {
        std::cout << "Tear down ContiniousSourceTest class." << std::endl;
    }
};

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteToCSVFileForExdra) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+1), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    //register physical stream
    PhysicalStreamConfig conf;
    conf.logicalStreamName = "exdra";
    conf.physicalStreamName = "test_stream";
    conf.sourceType = "CSVSource";
    conf.sourceConfig = "../tests/test_data/exdra.csv";
    conf.numberOfBuffersToProduce = 1;
    conf.sourceFrequency = 1;
    wrk1->registerPhysicalStream(conf);

    std::string filePath = "contTestOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString = "InputQuery::from(exdra).writeToCSVFile(\""
        + filePath + "\" , \"truncate\");";

    std::string queryId = crd->addQuery(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));

    ASSERT_TRUE(crd->removeQuery(queryId));

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

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromDefaultSourcePrint) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+1), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;


    //register logical stream
    std::string testSchema =
        "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("testStream", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfig conf;
    conf.logicalStreamName = "testStream";
    conf.physicalStreamName = "physical_test";
    conf.sourceType = "DefaultSource";
    conf.numberOfBuffersToProduce = 3;
    conf.sourceFrequency = 1;
    wrk1->registerPhysicalStream(conf);

    //register query
    std::string queryString =
        "InputQuery::from(testStream).filter(testStream[\"campaign_id\"] < 42).print(std::cout); ";

    std::string queryId = crd->addQuery(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 3));
    crd->addQuery(queryString, "BottomUp");
    ASSERT_TRUE(crd->removeQuery(queryId));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteFile) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+1), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    //register logical stream
    std::string testSchema =
        "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("testStream", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfig conf;
    conf.logicalStreamName = "testStream";
    conf.physicalStreamName = "physical_test";
    conf.sourceType = "DefaultSource";
    conf.numberOfBuffersToProduce = 3;
    conf.sourceFrequency = 1;
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testMultipleOutputBufferFromDefaultSourceWriteFileOutput.txt";
    remove(outputFilePath.c_str());

    //register query
    std::string queryString =
        "InputQuery::from(testStream).filter(testStream[\"campaign_id\"] < 42).writeToFile(\""
            + outputFilePath + "\"); ";

    std::string queryId = crd->addQuery(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 3));

    ASSERT_TRUE(crd->removeQuery(queryId));

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

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromCSVSourcePrint) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+1), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    //register logical stream
    std::string testSchema =
        "Schema::create()->addField(createField(\"val1\", UINT64))->"
        "addField(createField(\"val2\", UINT64))->"
        "addField(createField(\"val3\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("testStream", testSchemaFileName);

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
    wrk1->registerPhysicalStream(conf);

    //register query
    std::string queryString =
        "InputQuery::from(testStream).filter(testStream[\"val1\"] < 2).print(std::cout); ";

    std::string queryId = crd->addQuery(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 3));

    ASSERT_TRUE(crd->removeQuery(queryId));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContiniousSourceTest, testMultipleOutputBufferFromCSVSourceWrite) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+1), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    //register logical stream
    std::string testSchema =
        "Schema::create()->addField(createField(\"val1\", UINT64))->"
        "addField(createField(\"val2\", UINT64))->"
        "addField(createField(\"val3\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("testStream", testSchemaFileName);

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
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testMultipleOutputBufferFromCSVSourceWriteTest.out";
    remove(outputFilePath.c_str());

    //register query
    std::string queryString =
        "InputQuery::from(testStream).filter(testStream[\"val1\"] < 10).writeToFile(\""
            + outputFilePath + "\"); ";

    std::string queryId = crd->addQuery(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 3));

    ASSERT_TRUE(crd->removeQuery(queryId));

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
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|val1:UINT64|val2:UINT64|val3:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "|1|2|3|\n"
        "|1|2|4|\n"
        "|4|3|6|\n"
        "+----------------------------------------------------+";
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/**
 * TODO: the test itself is running in isolation but the network stack has a problem
 * Mar 05 2020 12:13:49 NES:116 [0x7fc7a87e0700] [ERROR] : ZMQSOURCE: Address already in use
 * Mar 05 2020 12:13:49 NES:124 [0x7fc7a87e0700] [DEBUG] : Exception: ZMQSOURCE  0x7fc7e0005b50: NOT connected
 * Mar 05 2020 12:13:49 NES:89 [0x7fc7a87e0700] [ERROR] : ZMQSOURCE: Not connected!
 * Once we fixed this we can activate this tests
 * */

TEST_F(ContiniousSourceTest, testExdraUseCaseWithOutput) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+1), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/false, /**withConnect**/true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    PhysicalStreamConfig conf;
    conf.logicalStreamName = "exdra";
    conf.physicalStreamName = "test_stream";
    conf.sourceType = "CSVSource";
    conf.sourceConfig = "../tests/test_data/exdra.csv";
    conf.numberOfBuffersToProduce = 5;
    conf.sourceFrequency = 0;
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testExdraUseCaseWithOutput.csv";
    remove(outputFilePath.c_str());

    //register query
    std::string queryString = "InputQuery::from(exdra).writeToCSVFile(\""
        + outputFilePath + "\" , \"truncate\");";

    cout << "deploy query" << endl;
    std::string queryId = crd->addQuery(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    cout << "wait on result" << endl;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 5));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 5));

    cout << "undeploy query" << endl;
    ASSERT_TRUE(crd->removeQuery(queryId));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bde5f5b9-97bc-4562-a767-4b919ff937fa,Features,4533,0.650272,2762389.250000,1262300400000,1262309669595,electricityGeneration,Point,14.582145,51.912815,c6661366-6f3a-4953-b052-fe0c89b58013\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,c91eccc9-ddb9-4128-9d18-131b2c48e695,Features,901,0.373627,298923.625000,1262300400000,0,electricityGeneration,Point,9.940279,51.576477,07cd5e06-4ce1-475c-b504-7730fa113d9f\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,bf7786ad-dcbb-444c-9a99-a19c8465e5ed,Features,3007,0.706310,499785.343750,1262300400000,1262307493613,electricityGeneration,Point,8.272306,52.517452,e1abec7b-ffc3-460a-985e-d6e761ba579f\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,a0e8fb98-7d9c-4c01-92f6-bbc0f1aec3f1,Features,3806,0.699387,466838.625000,1262300400000,1262311546817,electricityGeneration,Point,8.695134,52.011230,bc16682d-50f0-4153-a7ea-67ae3dc71b8e\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,2d201cc7-af8e-4969-8e69-6c37ae1ed7d0,Features,3076,0.523115,1391626.750000,1262300400000,1262307025541,electricityGeneration,Point,6.590214,53.908199,737554dc-245d-4759-a2f6-a34a95eebf3a\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,55eb25bd-df5f-4ab4-8932-385362fe573c,Features,3906,0.258830,813787.125000,1262300400000,1262310287664,electricityGeneration,Point,6.550814,47.035313,8b2e1875-f1e5-4ee6-905a-197f0a8ad85b\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,e3c5226e-9e52-4d37-acff-7d8d80964c4c,Features,4965,0.629812,1196946.625000,1262300400000,1262309439602,electricityGeneration,Point,6.693268,48.838722,54e5bfb8-0262-41ba-8810-3b55e7b89a47\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,8bfd6ef9-2c29-483a-9d3a-d925211ed504,Features,4543,0.556731,1897101.375000,1262300400000,0,electricityGeneration,Point,9.791286,52.406342,58ec269d-aefc-4bd8-a443-dc4d40c36c94\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,0609dfe6-2831-4bbf-a889-06dfb1311048,Features,3202,0.373316,183261.390625,1262300400000,1262309963720,electricityGeneration,Point,13.295279,49.204369,7fdd95bc-388f-4d7c-90b1-b6c48db95719\nFeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f8884b58-e72d-476d-9ef3-be2c87d8ace7,Features,3386,0.619704,941008.937500,1262300400000,1262310067980,electricityGeneration,Point,14.851712,48.742081,292af9eb-07a1-41c5-a924-98f881649f67\n";
    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}
