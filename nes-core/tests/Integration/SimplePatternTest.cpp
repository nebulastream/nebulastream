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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>
#include "../util/NesBaseTest.hpp"

//used tests: QueryCatalogTest, QueryTest
namespace fs = std::filesystem;
namespace NES {

using namespace Configurations;


class SimplePatternTest : public Testing::NESBaseTest {
  public:
    CoordinatorConfigurationPtr coConf;
    static void SetUpTestCase() {
        NES::setupLogging("SimplePatternTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SimplePatternTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        coConf = CoordinatorConfiguration::create();

        coConf->rpcPort=(*rpcCoordinatorPort);
        coConf->restPort = *restPort;
    }



    string removeRandomKey(string contentString) {
        std::regex r1("cep_leftkey([0-9]+)");
        std::regex r2("cep_rightkey([0-9]+)");
        contentString = std::regex_replace(contentString, r1, "cep_leftkey");
        contentString = std::regex_replace(contentString, r2, "cep_rightkey");
        return contentString;
    }
};

/* 1.Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
 * TODO: Ariane
 */
TEST_F(SimplePatternTest, DISABLED_testPatternWithTestStreamSingleOutput) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    //TODO: update CHAR (sensor id is in result set )
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV", qnv);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort=(port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("../tests/test_data/QnV_short.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    //register physical stream
    PhysicalSourcePtr conf70 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    worker1Configuration->physicalSources.add(conf70);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(")).selectionPolicy("Single_Output"); )";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|QnV$sensor_id:CHAR|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|\n"
                             "+----------------------------------------------------+\n"
                             "|R2000073|1543624020000|102.629631|8|\n"
                             "|R2000070|1543625280000|108.166664|5|\n"
                             "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content=" << content);
    NES_DEBUG("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 2.Test
  * Iteration Operator
 */
TEST_F(SimplePatternTest, testPatternWithIterationOperator) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", UINT64)->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV", qnv);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort=(port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("../tests/test_data/QnV_short_intID.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    //register physical stream
    PhysicalSourcePtr conf70 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    worker1Configuration->physicalSources.add(conf70);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithIterationOperator.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV").filter(Attribute("velocity") > 80).times(3,10).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|QnV$sensor_id:UINT64|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|\n"
                             "+----------------------------------------------------+\n"
                             "|2000070|1543625280000|108.166664|5|\n"
                             "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content=" << content);
    NES_DEBUG("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES
