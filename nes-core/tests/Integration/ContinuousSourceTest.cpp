/*
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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "../util/NesBaseTest.hpp"
#pragma clang diagnostic pop

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

#define DEBUG_OUTPUT
namespace NES {

using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case


class ContinuousSourceTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("ContinuousSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ContinuousSourceTest test class.");
    }

};

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteToCSVFileForExdra) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "exdra.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource1 = PhysicalSource::create("exdra", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "contTestOut.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString =
        R"(Query::from("exdra").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    // XXX;
    string const expectedContent =
        "exdra$id:INTEGER,exdra$metadata_generated:INTEGER,exdra$metadata_title:ArrayType,exdra$metadata_id:ArrayType,exdra$"
        "features_type:"
        "ArrayType,exdra$features_properties_"
        "capacity:INTEGER,exdra$features_properties_efficiency:(Float),exdra$features_properties_mag:(Float),exdra$features_"
        "properties_time:"
        "INTEGER,exdra$features_properties_updated:INTEGER,exdra$features_properties_type:ArrayType,exdra$features_geometry_type:"
        "ArrayType,"
        "exdra$features_geometry_"
        "coordinates_longitude:(Float),exdra$features_geometry_coordinates_latitude:(Float),exdra$features_eventId :ArrayType\n"
        "1,1262343610000,Wind Turbine Data Generated for Nebula "
        "Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,"
        "Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
        "2,1262343620010,Wind Turbine Data Generated for Nebula "
        "Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,"
        "Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
        "3,1262343630020,Wind Turbine Data Generated for Nebula "
        "Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,"
        "electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
        "4,1262343640030,Wind Turbine Data Generated for Nebula "
        "Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,"
        "Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
        "5,1262343650040,Wind Turbine Data Generated for Nebula "
        "Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,"
        "electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
        "6,1262343660050,Wind Turbine Data Generated for Nebula "
        "Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,"
        "electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
        "7,1262343670060,Wind Turbine Data Generated for Nebula "
        "Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,"
        "electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
        "8,1262343680070,Wind Turbine Data Generated for Nebula "
        "Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,"
        "electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
        "9,1262343690080,Wind Turbine Data Generated for Nebula "
        "Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,"
        "Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
        "10,1262343700090,Wind Turbine Data Generated for Nebula "
        "Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,"
        "Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
        "11,1262343710100,Wind Turbine Data Generated for Nebula "
        "Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,"
        "Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n";
    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("ContinuousSourceTest: content=" << content);
    NES_INFO("ContinuousSourceTest: expContent=" << expectedContent);

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourcePrint) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    auto defaultSourceType1 = DefaultSourceType::create();
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("testStream", "test_stream", defaultSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString =
        R"(Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(PrintSinkDescriptor::create());)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourcePrintWithLargerFrequency) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto defaultSourceType1 = DefaultSourceType::create();
    defaultSourceType1->setSourceFrequency(3);
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("testStream", "test_stream", defaultSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString =
        R"(Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(PrintSinkDescriptor::create());)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteFile) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto defaultSourceType1 = DefaultSourceType::create();
    defaultSourceType1->setSourceFrequency(1);
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("testStream", "test_stream", defaultSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testMultipleOutputBufferFromDefaultSourceWriteFile.txt";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString =
        R"(Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(FileSinkDescriptor::create(")" + outputFilePath
        + "\")); ";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|testStream$campaign_id:UINT64|\n"
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
        "|testStream$campaign_id:UINT64|\n"
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
        "|testStream$campaign_id:UINT64|\n"
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
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto defaultSourceType1 = DefaultSourceType::create();
    defaultSourceType1->setSourceFrequency(1);
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("testStream", "test_stream", defaultSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testMultipleOutputBufferFromDefaultSourceWriteFileWithLargerFrequency.txt";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString =
        R"(Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(FileSinkDescriptor::create(")" + outputFilePath
        + "\")); ";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|testStream$campaign_id:UINT64|\n"
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
        "|testStream$campaign_id:UINT64|\n"
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
        "|testStream$campaign_id:UINT64|\n"
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
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string testSchema = "Schema::create()->addField(createField(\"val1\", UINT64))->"
                             "addField(createField(\"val2\", UINT64))->"
                             "addField(createField(\"val3\", UINT64));";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    std::string testCSV = "1,2,3\n"
                          "1,2,4\n"
                          "4,3,6";
    std::string testCSVFileName = "testCSV.csv";
    std::ofstream outCsv(testCSVFileName);
    outCsv << testCSV;
    outCsv.close();
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("testCSV.csv");
    csvSourceType1->setSourceFrequency(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("testStream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = R"(Query::from("testStream").filter(Attribute("val1") < 2).sink(PrintSinkDescriptor::create()); )";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromCSVSourceWrite) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    std::string testSchema = "Schema::create()->addField(createField(\"val1\", UINT64))->"
                             "addField(createField(\"val2\", UINT64))->"
                             "addField(createField(\"val3\", UINT64));";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    std::string testCSV = "1,2,3\n"
                          "1,2,4\n"
                          "4,3,6";
    std::string testCSVFileName = "testCSV.csv";
    std::ofstream outCsv(testCSVFileName);
    outCsv << testCSV;
    outCsv.close();
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("testCSV.csv");
    csvSourceType1->setSourceFrequency(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    auto physicalSource1 = PhysicalSource::create("testStream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testMultipleOutputBufferFromCSVSourceWriteTest.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString = R"(Query::from("testStream").filter(Attribute("val1") < 10).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|testStream$val1:UINT64|testStream$val2:UINT64|testStream$val3:UINT64|\n"
                             "+----------------------------------------------------+\n"
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
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "exdra.csv");
    csvSourceType1->setSourceFrequency(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(5);
    auto physicalSource1 = PhysicalSource::create("exdra", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testExdraUseCaseWithOutput.csv";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    NES_INFO("ContinuousSourceTest: Deploy query");
    std::string queryString =
        R"(Query::from("exdra").sink(FileSinkDescriptor::create(")" + outputFilePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    NES_INFO("ContinuousSourceTest: Wait on result");
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("ContinuousSourceTest: Undeploy query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string const expectedContent =// XXX
        "exdra$id:INTEGER,exdra$metadata_generated:INTEGER,exdra$metadata_title:ArrayType,exdra$metadata_id:ArrayType,exdra$"
        "features_type:"
        "ArrayType,exdra$features_properties_"
        "capacity:INTEGER,exdra$features_properties_efficiency:(Float),exdra$features_properties_mag:(Float),exdra$features_"
        "properties_time:"
        "INTEGER,exdra$features_properties_updated:INTEGER,exdra$features_properties_type:ArrayType,exdra$features_geometry_type:"
        "ArrayType,"
        "exdra$features_geometry_"
        "coordinates_longitude:(Float),exdra$features_geometry_coordinates_latitude:(Float),exdra$features_eventId :ArrayType\n"
        "1,1262343610000,Wind Turbine Data Generated for Nebula "
        "Stream,b94c4bbf-6bab-47e3-b0f6-92acac066416,Features,736,0.363738,112464.007812,1262300400000,0,electricityGeneration,"
        "Point,8.221581,52.322945,982050ee-a8cb-4a7a-904c-a4c45e0c9f10\n"
        "2,1262343620010,Wind Turbine Data Generated for Nebula "
        "Stream,5a0aed66-c2b4-4817-883c-9e6401e821c5,Features,1348,0.508514,634415.062500,1262300400000,0,electricityGeneration,"
        "Point,13.759639,49.663155,a57b07e5-db32-479e-a273-690460f08b04\n"
        "3,1262343630020,Wind Turbine Data Generated for Nebula "
        "Stream,d3c88537-287c-4193-b971-d5ff913e07fe,Features,4575,0.163805,166353.078125,1262300400000,1262307581080,"
        "electricityGeneration,Point,7.799886,53.720783,049dc289-61cc-4b61-a2ab-27f59a7bfb4a\n"
        "4,1262343640030,Wind Turbine Data Generated for Nebula "
        "Stream,6649de13-b03d-43eb-83f3-6147b45c4808,Features,1358,0.584981,490703.968750,1262300400000,0,electricityGeneration,"
        "Point,7.109831,53.052448,4530ad62-d018-4017-a7ce-1243dbe01996\n"
        "5,1262343650040,Wind Turbine Data Generated for Nebula "
        "Stream,65460978-46d0-4b72-9a82-41d0bc280cf8,Features,1288,0.610928,141061.406250,1262300400000,1262311476342,"
        "electricityGeneration,Point,13.000446,48.636589,4a151bb1-6285-436f-acbd-0edee385300c\n"
        "6,1262343660050,Wind Turbine Data Generated for Nebula "
        "Stream,3724e073-7c9b-4bff-a1a8-375dd5266de5,Features,3458,0.684913,935073.625000,1262300400000,1262307294972,"
        "electricityGeneration,Point,10.876766,53.979465,e0769051-c3eb-4f14-af24-992f4edd2b26\n"
        "7,1262343670060,Wind Turbine Data Generated for Nebula "
        "Stream,413663f8-865f-4037-856c-45f6576f3147,Features,1128,0.312527,141904.984375,1262300400000,1262308626363,"
        "electricityGeneration,Point,13.480940,47.494038,5f374fac-94b3-437a-a795-830c2f1c7107\n"
        "8,1262343680070,Wind Turbine Data Generated for Nebula "
        "Stream,6a389efd-e7a4-44ff-be12-4544279d98ef,Features,1079,0.387814,15024.874023,1262300400000,1262312065773,"
        "electricityGeneration,Point,9.240296,52.196987,1fb1ade4-d091-4045-a8e6-254d26a1b1a2\n"
        "9,1262343690080,Wind Turbine Data Generated for Nebula "
        "Stream,93c78002-0997-4caf-81ef-64e5af550777,Features,2071,0.707438,70102.429688,1262300400000,0,electricityGeneration,"
        "Point,10.191643,51.904530,d2c6debb-c47f-4ca9-a0cc-ba1b192d3841\n"
        "10,1262343700090,Wind Turbine Data Generated for Nebula "
        "Stream,bef6b092-d1e7-4b93-b1b7-99f4d6b6a475,Features,2632,0.190165,66921.140625,1262300400000,0,electricityGeneration,"
        "Point,10.573558,52.531281,419bcfb4-b89b-4094-8990-e46a5ee533ff\n"
        "11,1262343710100,Wind Turbine Data Generated for Nebula "
        "Stream,6eaafae1-475c-48b7-854d-4434a2146eef,Features,4653,0.733402,758787.000000,1262300400000,0,electricityGeneration,"
        "Point,6.627055,48.164005,d8fe578e-1e92-40d2-83bf-6a72e024d55a\n"
        "12,1262343720110,Wind Turbine Data Generated for Nebula "
        "Stream,f3c1611f-db1c-49bf-9376-3ccae7248644,Features,3593,0.586449,597841.062500,1262300400000,1262308953634,"
        "electricityGeneration,Point,13.546017,47.870770,2ab9f413-848c-4ab4-a386-8615426e5c47\n"
        "13,1262343730120,Wind Turbine Data Generated for Nebula "
        "Stream,a72db07d-5c8c-4924-aeb3-e28c0bff1b2f,Features,3889,0.166697,56208.464844,1262300400000,1262308957774,"
        "electricityGeneration,Point,11.567608,48.489555,467c65ac-7679-4727-a05b-256571b91a46\n"
        "14,1262343740130,Wind Turbine Data Generated for Nebula "
        "Stream,84f00be9-c353-442d-9356-ff0fb1caa2fb,Features,2235,0.166961,299295.375000,1262300400000,0,electricityGeneration,"
        "Point,12.368753,52.965977,9792b6cf-63cf-4baa-ab28-5bd6ff04189f\n"
        "15,1262343750140,Wind Turbine Data Generated for Nebula "
        "Stream,e43f634d-9869-4d96-89db-0dc66a2b5134,Features,2764,0.444815,724771.125000,1262300400000,0,electricityGeneration,"
        "Point,11.567235,50.497688,a2db5a94-3bde-48f8-b730-ed74c7308db3\n"
        "16,1262343760150,Wind Turbine Data Generated for Nebula "
        "Stream,4efb382a-03d6-42a0-b55c-a033cc7209e2,Features,4140,0.369035,1123561.250000,1262300400000,0,electricityGeneration,"
        "Point,9.361658,51.083794,3c3cd5d7-4f19-4f89-bc14-f9328fce6b5e\n"
        "17,1262343770160,Wind Turbine Data Generated for Nebula "
        "Stream,33f13223-a0db-4246-9abb-2bf48ca78d94,Features,2338,0.610958,84045.023438,1262300400000,1262309011585,"
        "electricityGeneration,Point,7.109556,49.864002,d10f3a8e-8acd-453a-9961-b135104d4998\n"
        "18,1262343780170,Wind Turbine Data Generated for Nebula "
        "Stream,524d29d6-4d8f-4f8b-b3b4-9c55dd2cdcb4,Features,611,0.709108,46676.929688,1262300400000,1262308305059,"
        "electricityGeneration,Point,9.864325,48.852226,9d6d6d98-da5c-4d54-94ee-6686cabc7b74\n"
        "19,1262343790180,Wind Turbine Data Generated for Nebula "
        "Stream,68430660-21bf-42c5-affa-5827530fb389,Features,4526,0.463146,1461850.875000,1262300400000,1262310834053,"
        "electricityGeneration,Point,8.018541,47.056126,05a44117-f261-4fed-90c9-3211c4324b14\n"
        "20,1262343800190,Wind Turbine Data Generated for Nebula "
        "Stream,1bfe930f-8aeb-449f-88b4-ec668aaadfbe,Features,555,0.726571,24144.589844,1262300400000,0,electricityGeneration,"
        "Point,10.453132,47.310814,ff331964-4c3f-465e-8bba-f6f399d51fd1\n"
        "21,1262343810200,Wind Turbine Data Generated for Nebula "
        "Stream,0100f145-af35-4c8e-bde7-7e044460cc95,Features,4102,0.707496,2553631.000000,1262300400000,0,electricityGeneration,"
        "Point,11.338407,50.952404,a56a07d4-fc60-48ba-9566-3378abec4254\n";
    NES_INFO("Content=" << content);
    NES_INFO("ExpContent=" << expectedContent);

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/*
 * Testing test harness CSV source
 */
TEST_F(ContinuousSourceTest, testWithManyInputBuffer) {
    uint64_t numBufferToProduce = 1000;

    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "long_running.csv");
    csvSourceType->setSourceFrequency(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(numBufferToProduce);
    csvSourceType->setSkipHeader(false);

    std::string queryWithFilterOperator = R"(Query::from("car"))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("car", csvSourceType)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1ULL);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };
    std::vector<Output> expectedOutput = {};
    for (uint64_t i = 0; i < numBufferToProduce; i++) {
        expectedOutput.push_back({1, 1, (i + 1) * 100});
    }
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES
