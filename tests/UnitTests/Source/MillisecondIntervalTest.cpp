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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/QueryCatalog.hpp>
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

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

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
class MillisecondIntervalTest : public testing::Test {
  public:
    CoordinatorConfigPtr crdConf;
    WorkerConfigPtr wrkConf;
    SourceConfigPtr srcConf;

    static void SetUpTestCase() {
        NES::setupLogging("MillisecondIntervalTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MillisecondIntervalTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MillisecondIntervalTest test class."); }

    void SetUp() override {

        restPort = restPort + 3;
        rpcPort = rpcPort + 40;

        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        this->nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);

        crdConf = CoordinatorConfig::create();
        crdConf->setRpcPort(rpcPort);
        crdConf->setRestPort(restPort);

        wrkConf = WorkerConfig::create();
        wrkConf->setCoordinatorPort(rpcPort);

        srcConf = SourceConfig::create();
        srcConf->setSourceType("DefaultSource");
        srcConf->setSourceConfig("../tests/test_data/exdra.csv");
        srcConf->setSourceFrequency(550);
        srcConf->setNumberOfTuplesToProducePerBuffer(1);
        srcConf->setNumberOfBuffersToProduce(3);
        srcConf->setPhysicalStreamName("physical_test");
        srcConf->setLogicalStreamName("testStream");

        path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.csv";

        NES_INFO("Setup MillisecondIntervalTest class.");
    }

    void TearDown() override {
        nodeEngine->stop();
        nodeEngine = nullptr;
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
    const std::string del = ",";
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
    auto source = createCSVFileSource(schema,
                                      this->nodeEngine->getBufferManager(),
                                      this->nodeEngine->getQueryManager(),
                                      this->path_to_file,
                                      del,
                                      numberOfTuplesToProcess,
                                      numberOfBuffers,
                                      frequency,
                                      false,
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
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = this->nodeEngine;

    const std::string& del = ",";
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

    const DataSourcePtr source = createCSVFileSource(schema,
                                                     nodeEngine->getBufferManager(),
                                                     nodeEngine->getQueryManager(),
                                                     this->path_to_file,
                                                     del,
                                                     numberOfTuplesToProcess,
                                                     numberOfBuffers,
                                                     frequency,
                                                     false,
                                                     1,
                                                     12,
                                                     {});
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

    crdConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();

    NES_INFO("MillisecondIntervalTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0u);
    NES_INFO("MillisecondIntervalTest: Coordinator started successfully");

    NES_INFO("MillisecondIntervalTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MillisecondIntervalTest: Worker1 started successfully");

    //register logical stream
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("testStream", testSchemaFileName);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);

    //register physical stream
    wrk1->registerPhysicalStream(conf);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString =
        R"(Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(PrintSinkDescriptor::create());)";

    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
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