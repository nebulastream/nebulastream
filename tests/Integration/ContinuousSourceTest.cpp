/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <gtest/gtest.h>

#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/QueryService.hpp>
#include <Sources/YSBSource.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

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
        rpcPort = rpcPort + 30;
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
                                                    port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "CSVSource", /**Source Config**/ "../tests/test_data/exdra.csv",
        /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
        /**Number of Buffers To Produce**/ 1, /**Physical Stream Name**/ "test_stream",
        /**Logical Stream Name**/ "exdra", true);
    wrk1->registerPhysicalStream(conf);

    std::string filePath = "contTestOut.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = R"(Query::from("exdra").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent =
        "id:INTEGER,metadata_generated:INTEGER,metadata_title:Char,metadata_id:Char,features_type:Char,features_properties_capacity:INTEGER,features_properties_efficiency:(Float),features_properties_mag:(Float),features_properties_time:INTEGER,features_properties_updated:INTEGER,features_properties_type:Char,features_geometry_type:Char,features_geometry_coordinates_longitude:(Float),features_geometry_coordinates_latitude:(Float),features_eventId :Char\n"
        "1,1262343610000,Wind Turbine Data Generated for Nebula Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
        "2,1262343620010,Wind Turbine Data Generated for Nebula Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
        "3,1262343630020,Wind Turbine Data Generated for Nebula Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
        "4,1262343640030,Wind Turbine Data Generated for Nebula Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
        "5,1262343650040,Wind Turbine Data Generated for Nebula Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
        "6,1262343660050,Wind Turbine Data Generated for Nebula Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
        "7,1262343670060,Wind Turbine Data Generated for Nebula Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
        "8,1262343680070,Wind Turbine Data Generated for Nebula Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
        "9,1262343690080,Wind Turbine Data Generated for Nebula Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
        "10,1262343700090,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
        "11,1262343710100,Wind Turbine Data Generated for Nebula Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n";
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
                                                    port + 10, port + 11, NodeType::Sensor);
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

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "../tests/test_data/exdra.csv",
                                                                /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 1,
                                                                /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "physical_test",
                                                                /**Logical Stream Name**/ "testStream", true);

    //register physical stream
    wrk1->registerPhysicalStream(conf);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(PrintSinkDescriptor::create());";

    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

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
                                                    port + 10, port + 11, NodeType::Sensor);
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
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "../tests/test_data/exdra.csv",
                                                                /**Source Frequence**/ 3, /**Number Of Tuples To Produce Per Buffer**/ 1,
                                                                /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "physical_test",
                                                                /**Logical Stream Name**/ "testStream", true);
    wrk1->registerPhysicalStream(conf);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(PrintSinkDescriptor::create());";

    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

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
                                                    port + 10, port + 11, NodeType::Sensor);
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
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "../tests/test_data/exdra.csv",
                                                                /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 1,
                                                                /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "physical_test",
                                                                /**Logical Stream Name**/ "testStream", true);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testMultipleOutputBufferFromDefaultSourceWriteFile.txt";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\")); ";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

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
                                                    port + 10, port + 11, NodeType::Sensor);
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
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "../tests/test_data/exdra.csv",
                                                                /**Source Frequence**/ 3, /**Number Of Tuples To Produce Per Buffer**/ 1,
                                                                /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "physical_test",
                                                                /**Logical Stream Name**/ "testStream", true);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testMultipleOutputBufferFromDefaultSourceWriteFileWithLargerFrequency.txt";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\")); ";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

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
                                                    port + 10, port + 11, NodeType::Sensor);
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
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "CSVSource", /**Source Config**/ "testCSV.csv",
                                                                /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                                                /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "physical_test",
                                                                /**Logical Stream Name**/ "testStream", true);
    wrk1->registerPhysicalStream(conf);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"val1\") < 2).sink(PrintSinkDescriptor::create()); ";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

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
                                                    port + 10, port + 11, NodeType::Sensor);
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
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "CSVSource", /**Source Config**/ "testCSV.csv",
                                                                /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                                                /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "physical_test",
                                                                /**Logical Stream Name**/ "testStream", true);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testMultipleOutputBufferFromCSVSourceWriteTest.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = "Query::from(\"testStream\").filter(Attribute(\"val1\") < 10).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\")); ";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

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
                                                    port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "CSVSource", /**Source Config**/ "../tests/test_data/exdra.csv",
        /**Source Frequency**/ 0, /**Number Of Tuples To Produce Per Buffer**/ 0,
        /**Number of Buffers To Produce**/ 5, /**Physical Stream Name**/ "test_stream",
        /**Logical Stream Name**/ "exdra", true);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testExdraUseCaseWithOutput.csv";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    NES_INFO("ContinuousSourceTest: Deploy query");
    std::string queryString = "Query::from(\"exdra\").sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\" , \"CSV_FORMAT\", \"APPEND\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    NES_INFO("ContinuousSourceTest: Wait on result");
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 5));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 5));

    NES_INFO("ContinuousSourceTest: Undeploy query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "id:INTEGER,metadata_generated:INTEGER,metadata_title:Char,metadata_id:Char,features_type:Char,features_properties_capacity:INTEGER,features_properties_efficiency:(Float),features_properties_mag:(Float),features_properties_time:INTEGER,features_properties_updated:INTEGER,features_properties_type:Char,features_geometry_type:Char,features_geometry_coordinates_longitude:(Float),features_geometry_coordinates_latitude:(Float),features_eventId :Char\n"
        "1,1262343610000,Wind Turbine Data Generated for Nebula Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
        "2,1262343620010,Wind Turbine Data Generated for Nebula Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
        "3,1262343630020,Wind Turbine Data Generated for Nebula Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
        "4,1262343640030,Wind Turbine Data Generated for Nebula Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
        "5,1262343650040,Wind Turbine Data Generated for Nebula Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
        "6,1262343660050,Wind Turbine Data Generated for Nebula Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
        "7,1262343670060,Wind Turbine Data Generated for Nebula Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
        "8,1262343680070,Wind Turbine Data Generated for Nebula Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
        "9,1262343690080,Wind Turbine Data Generated for Nebula Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
        "10,1262343700090,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
        "11,1262343710100,Wind Turbine Data Generated for Nebula Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n"
        "12,1262343720110,Wind Turbine Data Generated for Nebula Stream,f3c1611f-db1c-49bf-9376-3ccae7248644,Features,3593,0.586449,597841.062500,1262300400000,1262308953634,electricityGeneration,Point,13.546017,47.870770,2ab9f413-848c-4ab4-a386-8615426e5c47\n"
        "13,1262343730120,Wind Turbine Data Generated for Nebula Stream,a72db07d-5c8c-4924-aeb3-e28c0bff1b2f,Features,3889,0.166697,56208.464844,1262300400000,1262308957774,electricityGeneration,Point,11.567608,48.489555,467c65ac-7679-4727-a05b-256571b91a46\n"
        "14,1262343740130,Wind Turbine Data Generated for Nebula Stream,84f00be9-c353-442d-9356-ff0fb1caa2fb,Features,2235,0.166961,299295.375000,1262300400000,0,electricityGeneration,Point,12.368753,52.965977,9792b6cf-63cf-4baa-ab28-5bd6ff04189f\n"
        "15,1262343750140,Wind Turbine Data Generated for Nebula Stream,e43f634d-9869-4d96-89db-0dc66a2b5134,Features,2764,0.444815,724771.125000,1262300400000,0,electricityGeneration,Point,11.567235,50.497688,a2db5a94-3bde-48f8-b730-ed74c7308db3\n"
        "16,1262343760150,Wind Turbine Data Generated for Nebula Stream,4efb382a-03d6-42a0-b55c-a033cc7209e2,Features,4140,0.369035,1123561.250000,1262300400000,0,electricityGeneration,Point,9.361658,51.083794,3c3cd5d7-4f19-4f89-bc14-f9328fce6b5e\n"
        "17,1262343770160,Wind Turbine Data Generated for Nebula Stream,33f13223-a0db-4246-9abb-2bf48ca78d94,Features,2338,0.610958,84045.023438,1262300400000,1262309011585,electricityGeneration,Point,7.109556,49.864002,d10f3a8e-8acd-453a-9961-b135104d4998\n"
        "18,1262343780170,Wind Turbine Data Generated for Nebula Stream,524d29d6-4d8f-4f8b-b3b4-9c55dd2cdcb4,Features,611,0.709108,46676.929688,1262300400000,1262308305059,electricityGeneration,Point,9.864325,48.852226,9d6d6d98-da5c-4d54-94ee-6686cabc7b74\n"
        "19,1262343790180,Wind Turbine Data Generated for Nebula Stream,68430660-21bf-42c5-affa-5827530fb389,Features,4526,0.463146,1461850.875000,1262300400000,1262310834053,electricityGeneration,Point,8.018541,47.056126,05a44117-f261-4fed-90c9-3211c4324b14\n"
        "20,1262343800190,Wind Turbine Data Generated for Nebula Stream,1bfe930f-8aeb-449f-88b4-ec668aaadfbe,Features,555,0.726571,24144.589844,1262300400000,0,electricityGeneration,Point,10.453132,47.310814,ff331964-4c3f-465e-8bba-f6f399d51fd1\n"
        "21,1262343810200,Wind Turbine Data Generated for Nebula Stream,0100f145-af35-4c8e-bde7-7e044460cc95,Features,4102,0.707496,2553631.000000,1262300400000,0,electricityGeneration,Point,11.338407,50.952404,a56a07d4-fc60-48ba-9566-3378abec4254\n"
        "1,1262343610000,Wind Turbine Data Generated for Nebula Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
        "2,1262343620010,Wind Turbine Data Generated for Nebula Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
        "3,1262343630020,Wind Turbine Data Generated for Nebula Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
        "4,1262343640030,Wind Turbine Data Generated for Nebula Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
        "5,1262343650040,Wind Turbine Data Generated for Nebula Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
        "6,1262343660050,Wind Turbine Data Generated for Nebula Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
        "7,1262343670060,Wind Turbine Data Generated for Nebula Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
        "8,1262343680070,Wind Turbine Data Generated for Nebula Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
        "9,1262343690080,Wind Turbine Data Generated for Nebula Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
        "10,1262343700090,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
        "11,1262343710100,Wind Turbine Data Generated for Nebula Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n"
        "12,1262343720110,Wind Turbine Data Generated for Nebula Stream,f3c1611f-db1c-49bf-9376-3ccae7248644,Features,3593,0.586449,597841.062500,1262300400000,1262308953634,electricityGeneration,Point,13.546017,47.870770,2ab9f413-848c-4ab4-a386-8615426e5c47\n"
        "13,1262343730120,Wind Turbine Data Generated for Nebula Stream,a72db07d-5c8c-4924-aeb3-e28c0bff1b2f,Features,3889,0.166697,56208.464844,1262300400000,1262308957774,electricityGeneration,Point,11.567608,48.489555,467c65ac-7679-4727-a05b-256571b91a46\n"
        "14,1262343740130,Wind Turbine Data Generated for Nebula Stream,84f00be9-c353-442d-9356-ff0fb1caa2fb,Features,2235,0.166961,299295.375000,1262300400000,0,electricityGeneration,Point,12.368753,52.965977,9792b6cf-63cf-4baa-ab28-5bd6ff04189f\n"
        "15,1262343750140,Wind Turbine Data Generated for Nebula Stream,e43f634d-9869-4d96-89db-0dc66a2b5134,Features,2764,0.444815,724771.125000,1262300400000,0,electricityGeneration,Point,11.567235,50.497688,a2db5a94-3bde-48f8-b730-ed74c7308db3\n"
        "16,1262343760150,Wind Turbine Data Generated for Nebula Stream,4efb382a-03d6-42a0-b55c-a033cc7209e2,Features,4140,0.369035,1123561.250000,1262300400000,0,electricityGeneration,Point,9.361658,51.083794,3c3cd5d7-4f19-4f89-bc14-f9328fce6b5e\n"
        "17,1262343770160,Wind Turbine Data Generated for Nebula Stream,33f13223-a0db-4246-9abb-2bf48ca78d94,Features,2338,0.610958,84045.023438,1262300400000,1262309011585,electricityGeneration,Point,7.109556,49.864002,d10f3a8e-8acd-453a-9961-b135104d4998\n"
        "18,1262343780170,Wind Turbine Data Generated for Nebula Stream,524d29d6-4d8f-4f8b-b3b4-9c55dd2cdcb4,Features,611,0.709108,46676.929688,1262300400000,1262308305059,electricityGeneration,Point,9.864325,48.852226,9d6d6d98-da5c-4d54-94ee-6686cabc7b74\n"
        "19,1262343790180,Wind Turbine Data Generated for Nebula Stream,68430660-21bf-42c5-affa-5827530fb389,Features,4526,0.463146,1461850.875000,1262300400000,1262310834053,electricityGeneration,Point,8.018541,47.056126,05a44117-f261-4fed-90c9-3211c4324b14\n"
        "20,1262343800190,Wind Turbine Data Generated for Nebula Stream,1bfe930f-8aeb-449f-88b4-ec668aaadfbe,Features,555,0.726571,24144.589844,1262300400000,0,electricityGeneration,Point,10.453132,47.310814,ff331964-4c3f-465e-8bba-f6f399d51fd1\n"
        "21,1262343810200,Wind Turbine Data Generated for Nebula Stream,0100f145-af35-4c8e-bde7-7e044460cc95,Features,4102,0.707496,2553631.000000,1262300400000,0,electricityGeneration,Point,11.338407,50.952404,a56a07d4-fc60-48ba-9566-3378abec4254\n"
        "1,1262343610000,Wind Turbine Data Generated for Nebula Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
        "2,1262343620010,Wind Turbine Data Generated for Nebula Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
        "3,1262343630020,Wind Turbine Data Generated for Nebula Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
        "4,1262343640030,Wind Turbine Data Generated for Nebula Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
        "5,1262343650040,Wind Turbine Data Generated for Nebula Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
        "6,1262343660050,Wind Turbine Data Generated for Nebula Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
        "7,1262343670060,Wind Turbine Data Generated for Nebula Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
        "8,1262343680070,Wind Turbine Data Generated for Nebula Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
        "9,1262343690080,Wind Turbine Data Generated for Nebula Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
        "10,1262343700090,Wind Turbine Data Generated for Nebula Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
        "11,1262343710100,Wind Turbine Data Generated for Nebula Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n"
        "12,1262343720110,Wind Turbine Data Generated for Nebula Stream,f3c1611f-db1c-49bf-9376-3ccae7248644,Features,3593,0.586449,597841.062500,1262300400000,1262308953634,electricityGeneration,Point,13.546017,47.870770,2ab9f413-848c-4ab4-a386-8615426e5c47\n"
        "13,1262343730120,Wind Turbine Data Generated for Nebula Stream,a72db07d-5c8c-4924-aeb3-e28c0bff1b2f,Features,3889,0.166697,56208.464844,1262300400000,1262308957774,electricityGeneration,Point,11.567608,48.489555,467c65ac-7679-4727-a05b-256571b91a46\n";
    NES_INFO("Content=" << content);
    NES_INFO("ExpContent=" << expectedContent);

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testYSB) {
    size_t producedBuffers = 2;
    size_t producedTuples = 5;

    //TODO: writing of csv file works, now make test green
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("ContinuousSourceTest: Coordinator started successfully");

    NES_INFO("ContinuousSourceTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress,
                                                    port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "YSBSource", /**Source Config**/ "",
        /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ producedTuples,
        /**Number of Buffers To Produce**/ producedBuffers, /**Physical Stream Name**/ "test_stream",
        /**Logical Stream Name**/ "ysb", true);
    wrk1->registerPhysicalStream(conf);

    std::string filePath = "contTestOut.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = R"(Query::from("ysb").windowByKey(Attribute("campaign_id"), TumblingWindow::of(EventTime(Attribute("current_ms")), Milliseconds(1)), Sum(Attribute("user_id"))).sink(FileSinkDescriptor::create(")"
        + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));
    
    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    NES_INFO("ContinuousSourceTest: content=" << content);
    EXPECT_TRUE(!content.empty());

    std::ifstream infile(filePath.c_str());
    std::string line;
    int lineCnt = 0;
    while (std::getline(infile, line))
    {
        if (lineCnt > 0) {
            auto token = UtilityFunctions::split(line, ',');
            YSBSource::YsbRecord rec{};
            rec.campaignId = stoi(token[2]);
            EXPECT_TRUE(rec.campaignId >= 0 && rec.campaignId < 10000);
        }
        lineCnt++;
    }

    EXPECT_EQ(sizeof(YSBSource::YsbRecord), 78);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES
