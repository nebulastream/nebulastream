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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <csignal>
#include <future>
#include <gtest/gtest.h>
#include <iostream>
#include <utility>

using namespace std;
using namespace NES::Windowing;
using namespace NES::Runtime;
using namespace NES::Runtime::Execution;

#define DEBUG_OUTPUT
namespace NES {

SharedQueryId testQueryId = SharedQueryId(123);

std::string expectedOutput = "sum:INTEGER(32 bits)\n10\n";

std::string joinedExpectedOutput = "sum:INTEGER(32 bits)\n10\n10\n";

std::string joinedExpectedOutput10 =
    "+----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|20|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|30|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|40|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|50|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|60|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|70|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|80|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|90|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|100|\n"
    "+----------------------------------------------------+";

template<typename MockedNodeEngine>
std::shared_ptr<MockedNodeEngine>
createMockedEngine(const std::string& hostname, uint16_t port, uint64_t bufferSize = 8192, uint64_t numBuffers = 1024) {
    try {

        class DummyQueryListener : public AbstractQueryStatusListener {
          public:
            virtual ~DummyQueryListener() {}

            bool canTriggerEndOfStream(SharedQueryId, DecomposedQueryId, OperatorId, Runtime::QueryTerminationType) override {
                return true;
            }
            bool notifySourceTermination(SharedQueryId, DecomposedQueryId, OperatorId, Runtime::QueryTerminationType) override {
                return true;
            }
            bool notifyQueryFailure(SharedQueryId, DecomposedQueryId, std::string) override { return true; }
            bool
            notifyQueryStatusChange(SharedQueryId, DecomposedQueryId, Runtime::Execution::ExecutableQueryPlanStatus) override {
                return true;
            }
            bool propagateNeighbourInformation(std::vector<std::pair<WorkerId, std::string>> neighbourInfo) override {
                return true;
            }
        };

        DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
        std::vector<PhysicalSourceTypePtr> physicalSources{defaultSourceType};

        auto partitionManager = std::make_shared<Network::PartitionManager>();
        std::vector bufferManager = {std::make_shared<Runtime::BufferManager>(bufferSize, numBuffers)};

        auto queryManager = std::make_shared<Runtime::DynamicQueryManager>(std::make_shared<DummyQueryListener>(),
                                                                           bufferManager,
                                                                           INVALID_WORKER_NODE_ID,
                                                                           1,
                                                                           nullptr,
                                                                           100);
        auto networkManagerCreator = [=](const Runtime::NodeEnginePtr& engine) {
            return Network::NetworkManager::create(INVALID_WORKER_NODE_ID,
                                                   hostname,
                                                   port,
                                                   Network::ExchangeProtocol(partitionManager, engine),
                                                   bufferManager[0]);
        };
        auto compilerOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto operatorHandlerStore = OperatorHandlerStore::create();
        auto queryCompiler = QueryCompilation::NautilusQueryCompiler::create(compilerOptions, phaseFactory, operatorHandlerStore);

        auto mockEngine = std::make_shared<MockedNodeEngine>(std::move(physicalSources),
                                                             std::make_shared<HardwareManager>(),
                                                             std::move(bufferManager),
                                                             std::move(queryManager),
                                                             std::move(networkManagerCreator),
                                                             std::move(partitionManager),
                                                             std::move(operatorHandlerStore),
                                                             std::move(queryCompiler),
                                                             INVALID_WORKER_NODE_ID,
                                                             1024,
                                                             12,
                                                             12);
        NES::Exceptions::installGlobalErrorListener(mockEngine);
        return mockEngine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine {}", err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
    return nullptr;
}

class TextExecutablePipeline : public ExecutablePipelineStage {
  public:
    virtual ~TextExecutablePipeline() = default;
    std::atomic<uint64_t> count = 0;
    std::atomic<uint64_t> sum = 0;
    std::promise<bool> completedPromise;

    ExecutionResult
    execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& wctx) override {
        auto* tuples = inputTupleBuffer.getBuffer<uint64_t>();

        NES_INFO("Test: Start execution");

        uint64_t psum = 0;
        for (uint64_t i = 0; i < inputTupleBuffer.getNumberOfTuples(); ++i) {
            psum += tuples[i];
        }
        count += inputTupleBuffer.getNumberOfTuples();
        sum += psum;

        NES_INFO("Test: query result = Processed Block:{} count: {} psum: {} sum: {}",
                 inputTupleBuffer.getNumberOfTuples(),
                 count,
                 psum,
                 sum);

        if (sum == 10) {
            NES_DEBUG("TEST: result correct");

            //TupleBuffer outputBuffer = pipelineExecutionContext.allocateTupleBuffer(); WAS THIS CODE
            TupleBuffer outputBuffer = wctx.allocateTupleBuffer();

            NES_DEBUG("TEST: got buffer");
            auto* arr = outputBuffer.getBuffer<uint32_t>();
            arr[0] = static_cast<uint32_t>(sum.load());
            outputBuffer.setNumberOfTuples(1);
            std::stringstream thisAsString;
            thisAsString << this;
            NES_DEBUG("TEST: {} written {}", thisAsString.str(), arr[0]);
            pipelineExecutionContext.emitBuffer(outputBuffer, wctx);
            completedPromise.set_value(true);
        } else {
            NES_DEBUG("TEST: result wrong ");
            completedPromise.set_value(false);
        }

        NES_DEBUG("TEST: return");
        return ExecutionResult::Ok;
    }
};

/**
 * @brief test for the engine
 * TODO: add more test cases
 *  - More complex queryIdAndCatalogEntryMapping
 *  - concurrent queryIdAndCatalogEntryMapping
 *  - long running queryIdAndCatalogEntryMapping
 *
 */
class NodeEngineTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("EngineTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("Setup EngineTest test class.");
    }

    void SetUp() override {
        NES_DEBUG("Setup OperatorOperatorCodeGenerationTest test case.");
        Testing::BaseIntegrationTest::SetUp();
        dataPort = Testing::BaseIntegrationTest::getAvailablePort();
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("Tear down OperatorOperatorCodeGenerationTest test case.");
        dataPort.reset();
        Testing::BaseIntegrationTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down OperatorOperatorCodeGenerationTest test class."); }

  protected:
    Testing::BorrowedPortPtr dataPort;
};
/**
 * Helper Methods
 */
void testOutput(const std::string& path) {
    ifstream testFile(path.c_str());
    ASSERT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    ASSERT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    ASSERT_TRUE(response == 0);
}

void testOutput(const std::string& path, const std::string& expectedOutput) {
    ifstream testFile(path.c_str());
    ASSERT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    ASSERT_TRUE(response == 0);
}

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(Runtime::QueryManagerPtr queryManager, const DataSinkPtr& sink)
        : PipelineExecutionContext(
            INVALID_PIPELINE_ID,             // mock pipeline id
            INVALID_DECOMPOSED_QUERY_PLAN_ID,// mock query id
            queryManager->getBufferManager(),
            queryManager->getNumberOfWorkerThreads(),
            [sink](TupleBuffer& buffer, Runtime::WorkerContextRef worker) {
                sink->writeData(buffer, worker);
            },
            [sink](TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>{}){
            // nop
        };
};

auto setupQEP(const NodeEnginePtr& engine, SharedQueryId sharedQueryId, const std::string& outPath) {
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);

    DataSinkPtr sink = createCSVFileSink(sch,
                                         sharedQueryId,
                                         DecomposedQueryId(sharedQueryId.getRawValue()),
                                         DecomposedQueryPlanVersion(0),
                                         engine,
                                         1,
                                         outPath,
                                         false, FaultToleranceType::NONE);
    auto context = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink);
    auto executable = std::make_shared<TextExecutablePipeline>();
    auto pipeline = ExecutablePipeline::create(INVALID_PIPELINE_ID,
                                               INVALID_SHARED_QUERY_ID,
                                               DecomposedQueryId(sharedQueryId.getRawValue()),
                                               DecomposedQueryPlanVersion(0),
                                               engine->getQueryManager(),
                                               context,
                                               executable,
                                               1,
                                               {sink});
    auto source = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                               engine->getQueryManager(),
                                                               OperatorId(1),
                                                               INVALID_ORIGIN_ID,
                                                               INVALID_STATISTIC_ID,
                                                               12,
                                                               "defaultPhysicalSourceName",
                                                               {pipeline});
    auto executionPlan = ExecutableQueryPlan::create(SharedQueryId(sharedQueryId.getRawValue()),
                                                     DecomposedQueryId(sharedQueryId.getRawValue()),
                                                     DecomposedQueryPlanVersion(0),
                                                     {source},
                                                     {sink},
                                                     {pipeline},
                                                     engine->getQueryManager(),
                                                     engine->getBufferManager());

    return std::make_tuple(executionPlan, executable);
}

//TODO: add test for register and start only

/**
 * Test methods
 *     cout << "Stats=" << ptr->getStatistics() << endl;
 */
TEST_F(NodeEngineTest, testStartStopEngineEmpty) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    ASSERT_TRUE(engine->stop());
}

TEST_F(NodeEngineTest, teststartDeployStop) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    auto [qep, pipeline] = setupQEP(engine, testQueryId, getTestResourceFolder() / "test.out");
    ASSERT_TRUE(engine->deployExecutableQueryPlan(qep));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    pipeline->completedPromise.get_future().get();
    ASSERT_TRUE(engine->stopDecomposedQueryPlan(qep->getSharedQueryId(), qep->getDecomposedQueryId()));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "test.out");
}

TEST_F(NodeEngineTest, testStartDeployUndeployStop) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    auto [qep, pipeline] = setupQEP(engine, testQueryId, getTestResourceFolder() / "test.out");
    ASSERT_TRUE(engine->deployExecutableQueryPlan(qep));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    pipeline->completedPromise.get_future().get();
    while (engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Finished) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    ASSERT_TRUE(engine->undeployDecomposedQueryPlan(testQueryId, qep->getDecomposedQueryId()));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "test.out");
}

TEST_F(NodeEngineTest, testStartRegisterStartStopDeregisterStop) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    auto [qep, pipeline] = setupQEP(engine, testQueryId, getTestResourceFolder() / "test.out");
    ASSERT_TRUE(engine->registerExecutableQueryPlan(qep));
    const auto& decomposedQueryId = qep->getDecomposedQueryId();
    ASSERT_TRUE(engine->startDecomposedQueryPlan(testQueryId, decomposedQueryId));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    pipeline->completedPromise.get_future().get();
    ASSERT_TRUE(engine->stopDecomposedQueryPlan(testQueryId, decomposedQueryId));

    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Stopped);

    ASSERT_TRUE(engine->unregisterDecomposedQueryPlan(testQueryId, decomposedQueryId));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "test.out");
}
//
TEST_F(NodeEngineTest, testParallelDifferentSource) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    //  GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);

    auto sink1 = createCSVFileSink(sch1,
                                   SharedQueryId(1),
                                   DecomposedQueryId(1),
                                   DecomposedQueryPlanVersion(0),
                                   engine,
                                   1,
                                   getTestResourceFolder() / "qep1.csv",
                                   false, FaultToleranceType::NONE);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(PipelineId(0),
                                                SharedQueryId(1),
                                                DecomposedQueryId(1),
                                                DecomposedQueryPlanVersion(0),
                                                engine->getQueryManager(),
                                                context1,
                                                executable1,
                                                1,
                                                {sink1});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                OperatorId(1),
                                                                INVALID_ORIGIN_ID,
                                                                INVALID_STATISTIC_ID,
                                                                12,
                                                                "defaultPhysicalSourceName",
                                                                {pipeline1});
    auto executionPlan = ExecutableQueryPlan::create(SharedQueryId(1),
                                                     DecomposedQueryId(1),
                                                     DecomposedQueryPlanVersion(0),
                                                     {source1},
                                                     {sink1},
                                                     {pipeline1},
                                                     engine->getQueryManager(),
                                                     engine->getBufferManager());

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sink2 = createCSVFileSink(sch2,
                                   SharedQueryId(2),
                                   DecomposedQueryId(2),
                                   DecomposedQueryPlanVersion(0),
                                   engine,
                                   1,
                                   getTestResourceFolder() / "qep2.csv",
                                   false, FaultToleranceType::NONE);
    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink2);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(PipelineId(0),
                                                SharedQueryId(2),
                                                DecomposedQueryId(2),
                                                DecomposedQueryPlanVersion(0),
                                                engine->getQueryManager(),
                                                context2,
                                                executable2,
                                                1,
                                                {sink2});
    auto source2 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                OperatorId(2),
                                                                INVALID_ORIGIN_ID,
                                                                INVALID_STATISTIC_ID,
                                                                12,
                                                                "defaultPhysicalSourceName",
                                                                {pipeline2});
    auto executionPlan2 = ExecutableQueryPlan::create(SharedQueryId(2),
                                                      DecomposedQueryId(2),
                                                      DecomposedQueryPlanVersion(0),
                                                      {source2},
                                                      {sink2},
                                                      {pipeline2},
                                                      engine->getQueryManager(),
                                                      engine->getBufferManager());

    ASSERT_TRUE(engine->registerExecutableQueryPlan(executionPlan));
    ASSERT_TRUE(engine->registerExecutableQueryPlan(executionPlan2));

    ASSERT_TRUE(engine->startDecomposedQueryPlan(SharedQueryId(1), executionPlan->getDecomposedQueryId()));
    ASSERT_TRUE(engine->startDecomposedQueryPlan(SharedQueryId(2), executionPlan2->getDecomposedQueryId()));

    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(1)) == ExecutableQueryPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(2)) == ExecutableQueryPlanStatus::Running);

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->stopDecomposedQueryPlan(SharedQueryId(1),
                                                executionPlan->getDecomposedQueryId(),
                                                Runtime::QueryTerminationType::HardStop));
    ASSERT_TRUE(engine->stopDecomposedQueryPlan(SharedQueryId(2),
                                                executionPlan2->getDecomposedQueryId(),
                                                Runtime::QueryTerminationType::HardStop));

    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(1)) == ExecutableQueryPlanStatus::Stopped);
    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(2)) == ExecutableQueryPlanStatus::Stopped);

    ASSERT_TRUE(engine->stop());

    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(1)) == ExecutableQueryPlanStatus::Invalid);
    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(2)) == ExecutableQueryPlanStatus::Invalid);

    testOutput(getTestResourceFolder() / "qep1.csv");
    testOutput(getTestResourceFolder() / "qep2.csv");
}
//
TEST_F(NodeEngineTest, testParallelSameSource) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);

    auto sink1 = createCSVFileSink(sch1,
                                   SharedQueryId(1),
                                   DecomposedQueryId(1),
                                   DecomposedQueryPlanVersion(0),
                                   engine,
                                   1,
                                   getTestResourceFolder() / "qep1.txt",
                                   true, , FaultToleranceType::NONE);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(PipelineId(0),
                                                SharedQueryId(1),
                                                DecomposedQueryId(1),
                                                DecomposedQueryPlanVersion(0),
                                                engine->getQueryManager(),
                                                context1,
                                                executable1,
                                                1,
                                                {sink1});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                OperatorId(1),
                                                                INVALID_ORIGIN_ID,
                                                                INVALID_STATISTIC_ID,
                                                                12,
                                                                "defaultPhysicalSourceName",
                                                                {pipeline1});
    auto executionPlan = ExecutableQueryPlan::create(SharedQueryId(1),
                                                     DecomposedQueryId(1),
                                                     DecomposedQueryPlanVersion(0),
                                                     {source1},
                                                     {sink1},
                                                     {pipeline1},
                                                     engine->getQueryManager(),
                                                     engine->getBufferManager());

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink2 = createCSVFileSink(sch2,
                                          SharedQueryId(2),
                                          DecomposedQueryId(2),
                                          DecomposedQueryPlanVersion(0),
                                          engine,
                                          1,
                                          getTestResourceFolder() / "qep2.txt",
                                          true, FaultToleranceType::NONE);

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink2);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(PipelineId(1),
                                                SharedQueryId(2),
                                                DecomposedQueryId(2),
                                                DecomposedQueryPlanVersion(0),
                                                engine->getQueryManager(),
                                                context2,
                                                executable2,
                                                1,
                                                {sink2});
    DataSourcePtr source2 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                         engine->getQueryManager(),
                                                                         OperatorId(2),
                                                                         OriginId(1),
                                                                         INVALID_STATISTIC_ID,
                                                                         12,
                                                                         "defaultPhysicalSourceName",
                                                                         {pipeline2});
    auto executionPlan2 = ExecutableQueryPlan::create(SharedQueryId(2),
                                                      DecomposedQueryId(2),
                                                      DecomposedQueryPlanVersion(0),
                                                      {source2},
                                                      {sink2},
                                                      {pipeline2},
                                                      engine->getQueryManager(),
                                                      engine->getBufferManager());

    ASSERT_TRUE(engine->registerExecutableQueryPlan(executionPlan));
    ASSERT_TRUE(engine->registerExecutableQueryPlan(executionPlan2));

    ASSERT_TRUE(engine->startDecomposedQueryPlan(SharedQueryId(1), executionPlan->getDecomposedQueryId()));
    ASSERT_TRUE(engine->startDecomposedQueryPlan(SharedQueryId(2), executionPlan2->getDecomposedQueryId()));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    while (engine->getQueryStatus(SharedQueryId(2)) != Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    while (engine->getQueryStatus(SharedQueryId(1)) != Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    ASSERT_TRUE(engine->undeployDecomposedQueryPlan(SharedQueryId(1), executionPlan->getDecomposedQueryId()));
    ASSERT_TRUE(engine->undeployDecomposedQueryPlan(SharedQueryId(2), executionPlan2->getDecomposedQueryId()));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "qep1.txt");
    testOutput(getTestResourceFolder() / "qep2.txt");
}
//
TEST_F(NodeEngineTest, DISABLED_testParallelSameSink) {// shared sinks are not supported
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    // create two executable query plans, which emit to the same sink
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sharedSink = createCSVFileSink(sch1,
                                        SharedQueryId(0),
                                        INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                        INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                        engine,
                                        1,
                                        getTestResourceFolder() / "qep12.txt",
                                        false,
                                        FaultToleranceType::NONE);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sharedSink);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(PipelineId(1),
                                                SharedQueryId(1),
                                                DecomposedQueryId(1),
                                                DecomposedQueryPlanVersion(0),
                                                engine->getQueryManager(),
                                                context1,
                                                executable1,
                                                1,
                                                {sharedSink});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                OperatorId(1),
                                                                INVALID_ORIGIN_ID,
                                                                INVALID_STATISTIC_ID,
                                                                12,
                                                                "defaultPhysicalSourceName",
                                                                {pipeline1});
    auto executionPlan = ExecutableQueryPlan::create(SharedQueryId(1),
                                                     DecomposedQueryId(1),
                                                     DecomposedQueryPlanVersion(0),
                                                     {source1},
                                                     {sharedSink},
                                                     {pipeline1},
                                                     engine->getQueryManager(),
                                                     engine->getBufferManager());

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sharedSink);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(PipelineId(2),
                                                SharedQueryId(2),
                                                DecomposedQueryId(2),
                                                DecomposedQueryPlanVersion(0),
                                                engine->getQueryManager(),
                                                context2,
                                                executable2,
                                                1,
                                                {sharedSink});

    DataSourcePtr source2 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                         engine->getQueryManager(),
                                                                         OperatorId(2),
                                                                         OriginId(3),
                                                                         INVALID_STATISTIC_ID,
                                                                         12,
                                                                         "defaultPhysicalSourceName",
                                                                         {pipeline2});

    auto executionPlan2 = ExecutableQueryPlan::create(SharedQueryId(2),
                                                      DecomposedQueryId(2),
                                                      DecomposedQueryPlanVersion(0),
                                                      {source2},
                                                      {sharedSink},
                                                      {pipeline2},
                                                      engine->getQueryManager(),
                                                      engine->getBufferManager());

    ASSERT_TRUE(engine->registerExecutableQueryPlan(executionPlan));
    ASSERT_TRUE(engine->registerExecutableQueryPlan(executionPlan2));

    ASSERT_TRUE(engine->startDecomposedQueryPlan(SharedQueryId(1), executionPlan->getDecomposedQueryId()));
    ASSERT_TRUE(engine->startDecomposedQueryPlan(SharedQueryId(2), executionPlan2->getDecomposedQueryId()));

    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(1)) == ExecutableQueryPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(2)) == ExecutableQueryPlanStatus::Running);

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->undeployDecomposedQueryPlan(SharedQueryId(1), executionPlan->getDecomposedQueryId()));
    ASSERT_TRUE(engine->undeployDecomposedQueryPlan(SharedQueryId(2), executionPlan2->getDecomposedQueryId()));
    ASSERT_TRUE(engine->stop());
    testOutput(getTestResourceFolder() / "qep12.txt", joinedExpectedOutput);
}
//
TEST_F(NodeEngineTest, DISABLED_testParallelSameSourceAndSinkRegstart) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sink1 = createCSVFileSink(sch1,
                                   SharedQueryId(0),
                                   INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                   INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                   engine,
                                   1,
                                   getTestResourceFolder() / "qep3.txt",
                                   true,
                                   FaultToleranceType::NONE);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(PipelineId(0),
                                                SharedQueryId(1),
                                                DecomposedQueryId(1),
                                                DecomposedQueryPlanVersion(0),
                                                engine->getQueryManager(),
                                                context1,
                                                executable1,
                                                1,
                                                {sink1});

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(PipelineId(1),
                                                SharedQueryId(2),
                                                DecomposedQueryId(2),
                                                DecomposedQueryPlanVersion(0),
                                                engine->getQueryManager(),
                                                context2,
                                                executable2,
                                                1,
                                                {sink1});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                OperatorId(1),
                                                                OriginId(4),
                                                                INVALID_STATISTIC_ID,
                                                                12,
                                                                "defaultPhysicalSourceName",
                                                                {pipeline1, pipeline2});
    auto executionPlan = ExecutableQueryPlan::create(SharedQueryId(1),
                                                     DecomposedQueryId(1),
                                                     DecomposedQueryPlanVersion(0),
                                                     {source1},
                                                     {sink1},
                                                     {pipeline1},
                                                     engine->getQueryManager(),
                                                     engine->getBufferManager());

    auto executionPlan2 = ExecutableQueryPlan::create(SharedQueryId(2),
                                                      DecomposedQueryId(2),
                                                      DecomposedQueryPlanVersion(0),
                                                      {source1},
                                                      {sink1},
                                                      {pipeline2},
                                                      engine->getQueryManager(),
                                                      engine->getBufferManager());

    //GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source1 =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    /*
    DataSinkPtr sink1 = createTextFileSink(sch1, 0, engine, 1, getTestResourceFolder() / "qep3.txt", true);
    //builder1.addSource(source1);
    builder1.addSink(sink1);
    builder1.setQueryId(1);
    builder1.setQuerySubPlanId(1);
    builder1.setQueryManager(engine->getQueryManager());
    builder1.setBufferManager(engine->getBufferManager());
    //builder1.setCompiler(engine->getCompiler());
    auto context1 =
        std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    //auto pipeline1 = ExecutablePipeline::create(0, 1, executable1, context1, 1, nullptr, source1->getSchema(), sch1);
    //builder1.addPipeline(pipeline1);

    GeneratedQueryExecutionPlanBuilder builder2 = GeneratedQueryExecutionPlanBuilder::create();

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    //builder2.addSource(source1);
    builder2.addSink(sink1);
    builder2.setQueryId(2);
    builder2.setQuerySubPlanId(2);
    builder2.setQueryManager(engine->getQueryManager());
    builder2.setBufferManager(engine->getBufferManager());
    //builder2.setCompiler(engine->getCompiler());

    auto context2 =
        std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
        */
    // auto pipeline2 = ExecutablePipeline::create(0, 2, executable2, context2, 1, nullptr, source1->getSchema(), sch2);
    // builder2.addPipeline(pipeline2);

    ASSERT_TRUE(engine->registerExecutableQueryPlan(executionPlan));
    ASSERT_TRUE(engine->registerExecutableQueryPlan(executionPlan2));

    ASSERT_TRUE(engine->startDecomposedQueryPlan(SharedQueryId(1), executionPlan->getDecomposedQueryId()));
    ASSERT_TRUE(engine->startDecomposedQueryPlan(SharedQueryId(2), executionPlan2->getDecomposedQueryId()));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(1)) == ExecutableQueryPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(SharedQueryId(2)) == ExecutableQueryPlanStatus::Running);

    ASSERT_TRUE(engine->undeployDecomposedQueryPlan(SharedQueryId(1), executionPlan->getDecomposedQueryId()));
    ASSERT_TRUE(engine->undeployDecomposedQueryPlan(SharedQueryId(2), executionPlan2->getDecomposedQueryId()));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "qep3.txt", joinedExpectedOutput);
}
//
TEST_F(NodeEngineTest, testStartStopStartStop) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    auto [qep, pipeline] = setupQEP(engine, testQueryId, getTestResourceFolder() / "test.out");
    ASSERT_TRUE(engine->deployExecutableQueryPlan(qep));
    pipeline->completedPromise.get_future().get();
    while (engine->getQueryStatus(testQueryId) != ExecutableQueryPlanStatus::Finished) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    ASSERT_TRUE(engine->undeployDecomposedQueryPlan(testQueryId, qep->getDecomposedQueryId()));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Invalid);

    ASSERT_TRUE(engine->stop());
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Invalid);
    testOutput(getTestResourceFolder() / "test.out");
}

TEST_F(NodeEngineTest, testBufferData) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);
    workerConfiguration->dataPort.setValue(*dataPort);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();
    EXPECT_FALSE(
        engine->bufferData(INVALID_DECOMPOSED_QUERY_PLAN_ID, INVALID_DECOMPOSED_QUERY_PLAN_VERSION, INVALID_OPERATOR_ID));
}

TEST_F(NodeEngineTest, testReconfigureSink) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);
    workerConfiguration->dataPort.setValue(*dataPort);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();
    EXPECT_FALSE(engine->updateNetworkSink(INVALID_WORKER_NODE_ID,
                                           "test",
                                           0,
                                           INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                           INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                           INVALID_OPERATOR_ID));
}

namespace detail {
void segkiller() { raise(SIGSEGV); }

void assertKiller() {
    class MockedNodeEngine : public NodeEngine {
      public:
        using NodeEngine::NodeEngine;

        explicit MockedNodeEngine(std::vector<PhysicalSourceTypePtr> physicalSources,
                                  HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  OperatorHandlerStorePtr operatorHandlerStore,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  WorkerId nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(physicalSources),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(operatorHandlerStore),
                         std::move(compiler),
                         std::weak_ptr<NesWorker>(),
                         std::make_shared<OpenCLManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker,
                         false) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string callstack) override {
            ASSERT_TRUE(stop(false));
            ASSERT_TRUE(
                strcmp(exception->what(), "Failed assertion on false error message: this will fail now with a RuntimeException")
                == 0);
            NodeEngine::onFatalException(exception, std::move(callstack));
        }
    };
    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 31340);
    NES_ASSERT(false, "this will fail now with a RuntimeException");
}
}// namespace detail

TEST_F(NodeEngineTest, DISABLED_testExceptionCrash) { EXPECT_EXIT(detail::assertKiller(), testing::ExitedWithCode(1), ""); }

TEST_F(NodeEngineTest, DISABLED_testSemiUnhandledExceptionCrash) {
    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        std::promise<bool> completedPromise;
        explicit MockedNodeEngine(std::vector<PhysicalSourceTypePtr> physicalSources,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  OperatorHandlerStorePtr operatorHandlerStore,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  WorkerId nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(physicalSources),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(operatorHandlerStore),
                         std::move(compiler),
                         std::weak_ptr<NesWorker>(),
                         std::make_shared<OpenCLManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker,
                         false) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string) override {
            const auto* str = exception->what();
            NES_ERROR("{}", str);
            ASSERT_TRUE(strcmp(str, "Got fatal error on thread 0: Catch me if you can!") == 0);
            completedPromise.set_value(true);
            ASSERT_TRUE(stop(true));
        }
    };
    class FailingTextExecutablePipeline : public ExecutablePipelineStage {
      public:
        virtual ~FailingTextExecutablePipeline() = default;
        ExecutionResult execute(TupleBuffer&, PipelineExecutionContext&, WorkerContext&) override {
            NES_DEBUG("Going to throw exception");
            throw std::runtime_error("Catch me if you can!");// :P
        }
    };

    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 0);

    //GeneratedQueryExecutionPlanBuilder builder = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createCSVFileSink(sch,
                                         INVALID_SHARED_QUERY_ID,
                                         INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                         INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                         engine,
                                         1,
                                         getTestResourceFolder() / "test.out",
                                         true,
                                         FaultToleranceType::NONE);
    // builder.addSource(source);
    // builder.addSink(sink);
    // builder.setQueryId(testQueryId);
    // builder.setQuerySubPlanId(testQueryId);
    // builder.setQueryManager(engine->getQueryManager());
    // builder.setBufferManager(engine->getBufferManager());
    //builder.setCompiler(engine->getCompiler());

    auto context = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink);
    auto executable = std::make_shared<FailingTextExecutablePipeline>();
    //auto pipeline = ExecutablePipeline::create(0, testQueryId, executable, context, 1, nullptr, source->getSchema(), sch);
    //builder.addPipeline(pipeline);
    //auto qep = builder.build();
    //ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    ASSERT_TRUE(engine->completedPromise.get_future().get());
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::ErrorState);
    ASSERT_TRUE(engine->stop());
}

TEST_F(NodeEngineTest, DISABLED_testFullyUnhandledExceptionCrash) {
    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        std::promise<bool> completedPromise;

        explicit MockedNodeEngine(std::vector<PhysicalSourceTypePtr> physicalSources,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  OperatorHandlerStorePtr operatorHandlerStore,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  WorkerId nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(physicalSources),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(operatorHandlerStore),
                         std::move(compiler),
                         std::weak_ptr<NesWorker>(),
                         std::make_shared<OpenCLManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker,
                         false) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string) override {
            const auto* str = exception->what();
            NES_ERROR("{}", str);
            ASSERT_TRUE(strcmp(str, "Unknown exception caught") == 0);
            completedPromise.set_value(true);
        }
    };
    class FailingTextExecutablePipeline : public ExecutablePipelineStage {
      public:
        ExecutionResult execute(TupleBuffer&, PipelineExecutionContext&, WorkerContext&) override {
            NES_DEBUG("Going to throw exception");
            throw 1;
        }
    };
    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 0);

    //GeneratedQueryExecutionPlanBuilder builder = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createCSVFileSink(sch,
                                         INVALID_SHARED_QUERY_ID,
                                         INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                         INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                         engine,
                                         1,
                                         getTestResourceFolder() / "test.out",
                                         true,
                                         FaultToleranceType::NONE);
    //builder.addSource(source);
    // builder.addSink(sink);
    //builder.setQueryId(testQueryId);
    //builder.setQuerySubPlanId(testQueryId);
    //builder.setQueryManager(engine->getQueryManager());
    //builder.setBufferManager(engine->getBufferManager());
    //builder.setCompiler(engine->getCompiler());

    auto context = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink);
    auto executable = std::make_shared<FailingTextExecutablePipeline>();
    //auto pipeline = ExecutablePipeline::create(0, testQueryId, executable, context, 1, nullptr, source->getSchema(), sch);
    //builder.addPipeline(pipeline);
    // auto qep = builder.build();
    //ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    engine->completedPromise.get_future().get();
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    ASSERT_TRUE(engine->stop());
}

TEST_F(NodeEngineTest, DISABLED_testFatalCrash) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("test", "test1");
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSourceTypes.add(defaultSourceType);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();
    EXPECT_EXIT(detail::segkiller(), testing::ExitedWithCode(1), "Runtime failed fatally");
}

}// namespace NES
