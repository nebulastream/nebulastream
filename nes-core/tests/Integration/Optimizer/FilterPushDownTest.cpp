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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>//for timing execution
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

namespace NES {

using namespace Configurations;

class FilterPushDownTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coConf;
    CSVSourceTypePtr srcConf1;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("AndOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AndOperatorTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        coConf = CoordinatorConfiguration::createDefault();
        srcConf1 = CSVSourceType::create();

        coConf->rpcPort = (*rpcCoordinatorPort);
        coConf->restPort = *restPort;
    }

    string removeRandomKey(string contentString) {
        std::regex r2("cep_rightKey([0-9]+)");
        contentString = std::regex_replace(contentString, r2, "cep_rightKey");

        uint64_t start = contentString.find("|QnV1QnV2$start:UINT64");
        uint64_t end = contentString.find("QnV2$cep_rightKey:INT32|\n");
        // Repeat till end is reached
        while (start != std::string::npos) {
            // Replace this occurrence of Sub String
            contentString = contentString.replace(start, end, "");
            // Get the next occurrence from the current position
            start = contentString.find("|QnV1QnV2$start:UINT64", end);
        }

        std::regex r3("\\+?[-]+\\+\\n?");
        contentString = std::regex_replace(contentString, r3, "");
        return contentString;
    }
};

/* 1.Test
 * This test checks if the filter push down below map keeps the correct order of operations when we apply a map with a substractions
 * followed by a map with a multiplication
 */
TEST_F(FilterPushDownTest, testCorrectResultsForFilterPushDownBelowTwoMaps) {
    // Setup Coordinator
    std::string qnv = R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                                         ->addField(createField("timestamp", BasicType::UINT64))->addField(createField("velocity", BasicType::FLOAT32))
                                         ->addField(createField("quantity", BasicType::UINT64));)";
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("FilterPushDownTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("FilterPushDownTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    auto windowSource1 = PhysicalSource::create("QnV1", "test_stream_QnV1", srcConf1);
    workerConfig1->physicalSources.add(windowSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("FilterPushDownTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "filterPushDownTest.out";
    remove(outputFilePath.c_str());

    NES_INFO("FilterPushDownTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").map(Attribute("velocity") = Attribute("velocity") - 5)
        .map(Attribute("velocity") = 5 * Attribute("velocity"))
        .filter(Attribute("velocity") < 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:UINT64|\n|R2000070|1543624260000|95."
        "000000|2|\n|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:UINT64|\n|R2000070|"
        "1543625520000|98.333328|3|\n|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:UINT64|\n|"
        "R2000070|1543625940000|80.952377|1|\n|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:"
        "UINT64|\n|R2000070|1543626120000|77.380951|1|\n|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$"
        "quantity:UINT64|\n|R2000070|1543626420000|78.571426|2|\n";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 2.Test
 * This test checks if the filter push down below map keeps the correct order of operations when we apply a map with a subtraction
 * in parentheses and a multiplication
 */
TEST_F(FilterPushDownTest, testSameResultsForPushDownBelowMapWithMul) {
    // Setup Coordinator
    std::string qnv = R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                                         ->addField(createField("timestamp", BasicType::UINT64))->addField(createField("velocity", BasicType::FLOAT32))
                                         ->addField(createField("quantity", BasicType::UINT64));)";
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("FilterPushDownTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("FilterPushDownTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    auto windowSource1 = PhysicalSource::create("QnV1", "test_stream_QnV1", srcConf1);
    workerConfig1->physicalSources.add(windowSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("FilterPushDownTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "filterPushDownTest.out";
    remove(outputFilePath.c_str());

    NES_INFO("FilterPushDownTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1")
        .map(Attribute("velocity") = 5 * (Attribute("velocity") - 5)).filter(Attribute("velocity") < 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:UINT64|\n|R2000070|1543624260000|95."
        "000000|2|\n|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:UINT64|\n|R2000070|"
        "1543625520000|98.333328|3|\n|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:UINT64|\n|"
        "R2000070|1543625940000|80.952377|1|\n|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:"
        "UINT64|\n|R2000070|1543626120000|77.380951|1|\n|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$"
        "quantity:UINT64|\n|R2000070|1543626420000|78.571426|2|\n";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES