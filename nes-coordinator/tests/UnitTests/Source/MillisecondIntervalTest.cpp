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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/TestSink.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <thread>

using namespace NES::Runtime;
using namespace NES::Runtime::Execution;

namespace NES {

using namespace Configurations;

struct __attribute__((packed)) YSBRecordTuple {
    char user_id[16] = {" "};
    char page_id[16] = {" "};
    char campaign_id[16] = {" "};
    char ad_type[9] = {" "};
    char event_type[9] = {" "};
    uint64_t timestamp;
    uint32_t ip;

    YSBRecordTuple(const std::string& user_id,
                   const std::string& page_id,
                   const std::string& campaign_id,
                   const std::string& ad_type,
                   const std::string& event_type,
                   uint64_t timestamp,
                   uint32_t ip) {
#define STR_TO_FIX_CHAR(field_name)                                                                                              \
    std::memcpy(this->field_name, field_name.data(), std::min(field_name.size(), sizeof(this->field_name)));
        STR_TO_FIX_CHAR(user_id);
        STR_TO_FIX_CHAR(page_id);
        STR_TO_FIX_CHAR(campaign_id);
        STR_TO_FIX_CHAR(ad_type);
        STR_TO_FIX_CHAR(event_type);
#undef STR_TO_FIX_CHAR
        this->timestamp = timestamp;
        this->ip = ip;
    }
    friend bool operator==(const YSBRecordTuple& lhs, const YSBRecordTuple& rhs) {
#define FIX_CHAR_CMP(field) (!std::strncmp(lhs.field, rhs.field, sizeof(lhs.field)))
        return FIX_CHAR_CMP(user_id) && FIX_CHAR_CMP(page_id) && FIX_CHAR_CMP(campaign_id) && FIX_CHAR_CMP(ad_type)
            && FIX_CHAR_CMP(event_type) && lhs.timestamp == rhs.timestamp && lhs.ip == rhs.ip;
#undef FIX_CHAR_CMP
    }
    friend bool operator!=(const YSBRecordTuple& lhs, const YSBRecordTuple& rhs) { return !(lhs == rhs); }
};

/**
 * This test set holds the corner cases for moving our sampling frequencies to
 * sub-second intervals. Before, NES was sampling every second and was checking
 * every second if that future timestamp is now stale (older).
 *
 * First we check for sub-second unit-tests on a soruce and its behavior. Then,
 * we include an E2Etest with a source that samples at sub-second interval.
 */
class MillisecondIntervalTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr wrkConf;
    CSVSourceTypePtr csvSourceType;
    std::string defaultPhysicalStreamName = "defaultPhysicalStreamName";

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MillisecondIntervalTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MillisecondIntervalTest test class.");
    }

    void SetUp() override {

        Testing::BaseIntegrationTest::SetUp();

        csvSourceType = CSVSourceType::create("testStream", "physical_test");
        csvSourceType->setGatheringInterval(550);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
        csvSourceType->setNumberOfBuffersToProduce(3);
        auto workerConfigurations = WorkerConfiguration::create();
        workerConfigurations->physicalSourceTypes.add(csvSourceType);
        this->nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                               .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                               .build();

        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort = *rpcCoordinatorPort;

        path_to_file = std::filesystem::path(TEST_DATA_DIRECTORY) / "ysb-tuples-100-campaign-100.csv";

        NES_INFO("Setup MillisecondIntervalTest class.");
    }

    void TearDown() override {
        ASSERT_TRUE(nodeEngine->stop());
        NES_INFO("Tear down MillisecondIntervalTest test case.");
        Testing::BaseIntegrationTest::TearDown();
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    std::string path_to_file;
};// MillisecondIntervalTest

TEST_F(MillisecondIntervalTest, testPipelinedCSVSource) {
    // Related to https://github.com/nebulastream/nebulastream/issues/2035
    auto queryId = 0;
    double frequency = 550;
    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", BasicType::UINT64)
                           ->addField("ip", BasicType::INT32);
    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 1;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    auto sink = CollectTestSink<YSBRecordTuple>::create(schema, this->nodeEngine);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create("testStream", "physical_test");
    csvSourceType->setFilePath(this->path_to_file);
    csvSourceType->setNumberOfBuffersToProduce(numberOfBuffers);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProcess);
    csvSourceType->setGatheringInterval(frequency);

    auto source = createCSVFileSource(schema,
                                      this->nodeEngine->getBufferManager(),
                                      this->nodeEngine->getQueryManager(),
                                      csvSourceType,
                                      OperatorId(1),
                                      INVALID_ORIGIN_ID,
                                      INVALID_STATISTIC_ID,
                                      12,
                                      defaultPhysicalStreamName,
                                      {sink},
                                      false);

    auto executionPlan = ExecutableQueryPlan::create(SharedQueryId(queryId),
                                                     DecomposedQueryId(queryId),
                                                     DecomposedQueryPlanVersion(0),
                                                     {source},
                                                     {sink},
                                                     {},
                                                     this->nodeEngine->getQueryManager(),
                                                     this->nodeEngine->getBufferManager());
    EXPECT_TRUE(this->nodeEngine->registerExecutableQueryPlan(executionPlan));
    EXPECT_TRUE(
        this->nodeEngine->startDecomposedQueryPlan(executionPlan->getSharedQueryId(), executionPlan->getDecomposedQueryId()));
    EXPECT_EQ(this->nodeEngine->getQueryStatus(SharedQueryId(queryId)), ExecutableQueryPlanStatus::Running);
    sink->waitTillCompleted(numberOfBuffers * numberOfTuplesToProcess);
    auto theThing = sink->getResult();
    EXPECT_EQ(theThing.size(), numberOfTuplesToProcess);
    EXPECT_EQ(theThing[0], YSBRecordTuple("0", "0", "0", "banner78", "view", 1554420890327, 16909060));
    std::vector<long> userIds;
    std::transform(theThing.begin(), theThing.end(), std::back_inserter(userIds), [](auto& t) {
        char* end_of_user_id = t.user_id + sizeof(t.user_id);
        return std::strtol(t.user_id, &end_of_user_id, 10);
    });

    std::vector<long> expectedUserIds(numberOfTuplesToProcess);
    std::iota(expectedUserIds.begin(), expectedUserIds.end(), 0);

    EXPECT_EQ(userIds, expectedUserIds);
}

TEST_F(MillisecondIntervalTest, testMultipleOutputBufferFromDefaultSourcePrintSubSecond) {
    NES_INFO("MillisecondIntervalTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0u);
    //register logical source
    auto testSchema = Schema::create()->addField(createField("campaign_id", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("testStream", testSchema);
    NES_INFO("MillisecondIntervalTest: Coordinator started successfully");

    NES_INFO("MillisecondIntervalTest: Start worker 1");
    wrkConf->coordinatorPort = port;
    auto defaultSourceType = DefaultSourceType::create("testStream", "x1");
    defaultSourceType->setNumberOfBuffersToProduce(3);
    wrkConf->physicalSourceTypes.add(defaultSourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MillisecondIntervalTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString =
        R"(Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(PrintSinkDescriptor::create());)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("MillisecondIntervalTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}//namespace NES
