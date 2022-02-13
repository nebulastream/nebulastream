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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>
#include "../../util/NesBaseTest.hpp"

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Services/QueryService.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/TestUtils.hpp>
using namespace NES::Runtime;
using namespace NES::Runtime::Execution;

namespace NES {

using namespace Configurations;

struct __attribute__((packed)) ysbRecord {
    char user_id[16]{};
    char page_id[16]{};
    char campaign_id[16]{};
    char ad_type[9]{};
    char event_type[9]{};
    int64_t current_ms;
    uint32_t ip;

    ysbRecord() {
        event_type[0] = '-';// invalid record
        event_type[1] = '\0';
        current_ms = 0;
        ip = 0;
    }

    ysbRecord(const ysbRecord& rhs) {
        memcpy(&user_id, &rhs.user_id, 16);
        memcpy(&page_id, &rhs.page_id, 16);
        memcpy(&campaign_id, &rhs.campaign_id, 16);
        memcpy(&ad_type, &rhs.ad_type, 9);
        memcpy(&event_type, &rhs.event_type, 9);
        current_ms = rhs.current_ms;
        ip = rhs.ip;
    }
};
// size 78 bytes

/**
 * This test set holds the corner cases for moving our sampling frequencies to
 * sub-second intervals. Before, NES was sampling every second and was checking
 * every second if that future timestamp is now stale (older).
 *
 * First we check for sub-second unit-tests on a soruce and its behavior. Then,
 * we include an E2Etest with a stream that samples at sub-second interval.
 */
class MillisecondIntervalTest : public Testing::NESBaseTest {
  public:
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr wrkConf;
    CSVSourceTypePtr csvSourceType;

    static void SetUpTestCase() {
        NES::setupLogging("MillisecondIntervalTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MillisecondIntervalTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MillisecondIntervalTest test class."); }

    void SetUp() override {

        Testing::NESBaseTest::SetUp();

        csvSourceType = CSVSourceType::create();
        csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "exdra.csv");
        csvSourceType->setSourceFrequency(550);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
        csvSourceType->setNumberOfBuffersToProduce(3);
        PhysicalSourcePtr streamConf = PhysicalSource::create("testStream", "physical_test", csvSourceType);
        this->nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 0, {streamConf});

        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort = *rpcCoordinatorPort;

        path_to_file = std::string(TEST_DATA_DIRECTORY) + "ysb-tuples-100-campaign-100.csv";

        NES_INFO("Setup MillisecondIntervalTest class.");
    }

    void TearDown() override {
        ASSERT_TRUE(nodeEngine->stop());
        NES_INFO("Tear down MillisecondIntervalTest test case.");
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    std::string path_to_file;
};// MillisecondIntervalTest

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(Runtime::QueryManagerPtr queryManager, DataSinkPtr sink)
        : PipelineExecutionContext(
            0,
            std::move(queryManager),
            [sink](TupleBuffer& buffer, Runtime::WorkerContextRef worker) {
                sink->writeData(buffer, worker);
            },
            [sink](TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>()){
            // nop
        };
};

class MockedExecutablePipeline : public ExecutablePipelineStage {
  public:
    std::atomic<uint64_t> count = 0;
    std::promise<bool> completedPromise;

    ExecutionResult
    execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& wctx) override {
        count += inputTupleBuffer.getNumberOfTuples();

        TupleBuffer outputBuffer = wctx.allocateTupleBuffer();
        auto arr = outputBuffer.getBuffer<uint32_t>();
        arr[0] = static_cast<uint32_t>(count.load());
        outputBuffer.setNumberOfTuples(count);
        pipelineExecutionContext.emitBuffer(outputBuffer, wctx);
        completedPromise.set_value(true);
        return ExecutionResult::Ok;
    }
};

TEST_F(MillisecondIntervalTest, testPipelinedCSVSource) {
    // Related to https://github.com/nebulastream/nebulastream/issues/2035
    auto queryId = 1;
    double frequency = 550;
    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);
    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 1;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    auto sink = createCSVFileSink(schema, 0, this->nodeEngine, "qep1.txt", false);
    auto context = std::make_shared<MockedPipelineExecutionContext>(this->nodeEngine->getQueryManager(), sink);
    auto executableStage = std::make_shared<MockedExecutablePipeline>();
    auto pipeline = ExecutablePipeline::create(0, queryId, context, executableStage, 1, {sink});

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(this->path_to_file);
    csvSourceType->setNumberOfBuffersToProduce(numberOfBuffers);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProcess);
    csvSourceType->setSourceFrequency(frequency);

    auto source = createCSVFileSource(schema,
                                      this->nodeEngine->getBufferManager(),
                                      this->nodeEngine->getQueryManager(),
                                      csvSourceType,
                                      1,
                                      12,
                                      {pipeline});

    auto executionPlan = ExecutableQueryPlan::create(queryId,
                                                     queryId,
                                                     {source},
                                                     {sink},
                                                     {pipeline},
                                                     this->nodeEngine->getQueryManager(),
                                                     this->nodeEngine->getBufferManager());
    EXPECT_TRUE(this->nodeEngine->registerQueryInNodeEngine(executionPlan));
    EXPECT_TRUE(this->nodeEngine->startQuery(1));
    EXPECT_EQ(this->nodeEngine->getQueryStatus(1), ExecutableQueryPlanStatus::Running);
    executableStage->completedPromise.get_future().get();
}

TEST_F(MillisecondIntervalTest, DISABLED_testCSVSourceWithOneLoopOverFileSubSecond) {
    auto nodeEngine = this->nodeEngine;

    double frequency = 550;
    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);

    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 1;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(this->path_to_file);
    csvSourceType->setNumberOfBuffersToProduce(numberOfBuffers);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProcess);
    csvSourceType->setSourceFrequency(frequency);

    const DataSourcePtr source =
        createCSVFileSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), csvSourceType, 1, 12, {});
    source->start();
    while (source->getNumberOfGeneratedBuffers() < numberOfBuffers) {
        auto optBuf = source->receiveData();
        // will be handled by issue #1612, test is disabled
        // use WindowDeploymentTest->testYSBWindow, where getBuffer is cast to ysbRecord
        uint64_t i = 0;
        while (i * tuple_size < buffer_size - tuple_size && optBuf.has_value()) {
            auto* record = optBuf->getBuffer<ysbRecord>() + i;
            std::cout << "i=" << i << " record.ad_type: " << record->ad_type << ", record.event_type: " << record->event_type
                      << std::endl;
            EXPECT_STREQ(record->ad_type, "banner78");
            EXPECT_TRUE((!strcmp(record->event_type, "view") || !strcmp(record->event_type, "click")
                         || !strcmp(record->event_type, "purchase")));
            i++;
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedTuples(), numberOfTuplesToProcess);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numberOfBuffers);
    EXPECT_EQ(source->getGatheringIntervalCount(), frequency);
}

TEST_F(MillisecondIntervalTest, testMultipleOutputBufferFromDefaultSourcePrintSubSecond) {

    coordinatorConfig->clear();
    wrkConf->clear();

    NES_INFO("MillisecondIntervalTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0u);
    //register logical stream
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    crd->getStreamCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_INFO("MillisecondIntervalTest: Coordinator started successfully");

    NES_INFO("MillisecondIntervalTest: Start worker 1");
    wrkConf->coordinatorPort = port;
    auto defaultSourceType = DefaultSourceType::create();
    defaultSourceType->setNumberOfBuffersToProduce(3);
    auto physicalSource = PhysicalSource::create("testStream", "x1", defaultSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MillisecondIntervalTest: Worker1 started successfully");

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

    NES_INFO("MillisecondIntervalTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}//namespace NES