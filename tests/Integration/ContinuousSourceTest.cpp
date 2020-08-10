#include <iostream>

#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

using namespace std;

#define DEBUG_OUTPUT
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;

class ContinuousSourceTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("ContinuousSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ContinuousSourceTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort +2;
    }

    void TearDown() {
        std::cout << "Tear down ContinuousSourceTest class." << std::endl;
    }

    std::string ipAddress = "127.0.0.1";
    uint64_t restPort = 8081;
};

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteToCSVFileForExdra) {
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress,
                                                    port + 10, port + 11, NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

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

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = R"(Query::from("exdra").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    std::string queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent =
        "type:Char,metadata_generated:INTEGER,metadata_title:Char,metadata_id:Char,features_type:Char,features_properties_capacity:INTEGER,features_properties_efficiency:(Float),features_properties_mag:(Float),features_properties_time:INTEGER,features_properties_updated:INTEGER,features_properties_type:Char,features_geometry_type:Char,features_geometry_coordinates_longitude:(Float),features_geometry_coordinates_latitude:(Float),features_eventId :Char\n"
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

    NES_INFO("ContinuousSourceTest: content=" << content);
    NES_INFO("ContinuousSourceTest: expContent=" << expectedContent);

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourcePrint) {
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1",
                                                    port + 10, port + 11, NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    //register logical stream
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
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

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(PrintSinkDescriptor::create());";

    std::string queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourcePrintWithLargerFrequency) {
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1",
                                                    port + 10, port + 11, NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    //register logical stream
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
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
    conf.sourceFrequency = 3;
    wrk1->registerPhysicalStream(conf);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(PrintSinkDescriptor::create());";

    std::string queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteFile) {
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1",
                                                    port + 10, port + 11, NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

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

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\")); ";
    std::string queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteFileWithLargerFrequency) {
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1",
                                                    port + 10, port + 11, NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

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
    conf.sourceFrequency = 3;
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testMultipleOutputBufferFromDefaultSourceWriteFileOutput.txt";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\")); ";
    std::string queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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


TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromCSVSourcePrint) {
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1",
                                                    port + 10, port + 11, NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

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

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"val1\") < 2).sink(PrintSinkDescriptor::create()); ";
    std::string queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromCSVSourceWrite) {
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1",
                                                    port + 10, port + 11, NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

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

    std::string outputFilePath = "testMultipleOutputBufferFromCSVSourceWriteTest.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"val1\") < 10).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\")); ";
    std::string queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    NES_INFO("ContinuousSourceTest: content=" << content);
    NES_INFO("ContinuousSourceTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/**
 * TODO: the test itself is running in isolation but the ZMQ SOURCE has problems
 * Mar 05 2020 12:13:49 NES:116 [0x7fc7a87e0700] [ERROR] : ZMQSOURCE: Address already in use
 * Mar 05 2020 12:13:49 NES:124 [0x7fc7a87e0700] [DEBUG] : Exception: ZMQSOURCE  0x7fc7e0005b50: NOT connected
 * Mar 05 2020 12:13:49 NES:89 [0x7fc7a87e0700] [ERROR] : ZMQSOURCE: Not connected!
 * Once we fixed this we can activate this tests
 * */

TEST_F(ContinuousSourceTest, testExdraUseCaseWithOutput) {
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1",
                                                    port + 10, port + 11, NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

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

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    NES_INFO("ContinuousSourceTest: Deploy query");
    std::string queryString = "Query::from(\"exdra\").sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\" , \"CSV_FORMAT\", \"APPEND\"));";
    std::string queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, "");
    NES_INFO("ContinuousSourceTest: Wait on result");
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 5));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 5));

    NES_INFO("ContinuousSourceTest: Undeploy query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "type:Char,metadata_generated:INTEGER,metadata_title:Char,metadata_id:Char,features_type:Char,features_properties_capacity:INTEGER,features_properties_efficiency:(Float),features_properties_mag:(Float),features_properties_time:INTEGER,features_properties_updated:INTEGER,features_properties_type:Char,features_geometry_type:Char,features_geometry_coordinates_longitude:(Float),features_geometry_coordinates_latitude:(Float),features_eventId :Char\n"
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
        "FeatureCollection,1262343600000,Wind Turbine Data Generated for Nebula Stream,f8884b58-e72d-476d-9ef3-be2c87d8ace7,Features,3386,0.619704,941008.937500,1262300400000,1262310067980,electricityGeneration,Point,14.851712,48.742081,292af9eb-07a1-41c5-a924-98f881649f67\n";

    NES_INFO("Content=" << content);
    NES_INFO("ExpContent=" << expectedContent);

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES
