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

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
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

uint64_t testQueryId = 123;

std::string expectedOutput = "+----------------------------------------------------+\n"
                             "|sum:UINT32|\n"
                             "+----------------------------------------------------+\n"
                             "|10|\n"
                             "+----------------------------------------------------+";

std::string joinedExpectedOutput =
    "+----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------+";

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
    "+----------------------------https://app.diagrams.net/#G1uTDrT32L0Aep6CnvBZHbJJ1LsHDyZSZr------------------------+\n"
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

std::string filePath = "file.txt";
namespace Runtime {
extern void installGlobalErrorListener(std::shared_ptr<ErrorListener> const& listener);
}
template<typename MockedNodeEngine>
std::shared_ptr<MockedNodeEngine>
createMockedEngine(const std::string& hostname, uint16_t port, uint64_t bufferSize = 8192, uint64_t numBuffers = 1024) {
    try {
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        std::vector<BufferManagerPtr> bufferManager = {std::make_shared<Runtime::BufferManager>(bufferSize, numBuffers)};
        auto queryManager = std::make_shared<Runtime::QueryManager>(bufferManager, 0, 1, nullptr);
        auto bufferStorage = std::make_shared<BufferStorage>();
        auto networkManagerCreator = [=](const Runtime::NodeEnginePtr& engine) {
            return Network::NetworkManager::create(hostname,
                                                   port,
                                                   Network::ExchangeProtocol(partitionManager, engine),
                                                   bufferManager[0]);
        };
        auto compilerOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryCompiler = QueryCompilation::DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

        auto mockEngine = std::make_shared<MockedNodeEngine>(std::move(streamConf),
                                                             std::make_shared<HardwareManager>(),
                                                             std::move(bufferManager),
                                                             std::move(queryManager),
                                                             std::move(bufferStorage),
                                                             std::move(networkManagerCreator),
                                                             std::move(partitionManager),
                                                             std::move(queryCompiler),
                                                             0,
                                                             1024,
                                                             12,
                                                             12);
        NES::Runtime::installGlobalErrorListener(mockEngine);
        return mockEngine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
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

        NES_INFO("Test: query result = Processed Block:" << inputTupleBuffer.getNumberOfTuples() << " count: " << count
                                                         << " psum: " << psum << " sum: " << sum);

        if (sum == 10) {
            NES_DEBUG("TEST: result correct");

            //TupleBuffer outputBuffer = pipelineExecutionContext.allocateTupleBuffer(); WAS THIS CODE
            TupleBuffer outputBuffer = wctx.allocateTupleBuffer();

            NES_DEBUG("TEST: got buffer");
            auto* arr = outputBuffer.getBuffer<uint32_t>();
            arr[0] = static_cast<uint32_t>(sum.load());
            outputBuffer.setNumberOfTuples(1);
            NES_DEBUG("TEST: " << this << " written " << arr[0]);
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
 *  - More complex queries
 *  - concurrent queries
 *  - long running queries
 *
 */
class EngineTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("EngineTest.log", NES::LOG_DEBUG);
        remove(filePath.c_str());
        NES_INFO("Setup EngineTest test class.");
    }
    static void TearDownTestCase() {
        remove(filePath.c_str());
        remove("qep1.txt");
        remove("qep2.txt");
        std::cout << "Tear down EngineTest class." << std::endl;
    }
};
/**
 * Helper Methods
 */
void testOutput() {
    ifstream testFile(filePath.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(filePath.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(filePath.c_str());
    EXPECT_TRUE(response == 0);
}

void testOutput(const std::string& path) {
    ifstream testFile(path.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    EXPECT_TRUE(response == 0);
}

void testOutput(const std::string& path, const std::string& expectedOutput) {
    ifstream testFile(path.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    EXPECT_TRUE(response == 0);
}

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(Runtime::QueryManagerPtr queryManager, const DataSinkPtr& sink)
        : PipelineExecutionContext(
            0,
            std::move(queryManager),
            [sink](TupleBuffer& buffer, Runtime::WorkerContextRef worker) {
                sink->writeData(buffer, worker);
            },
            [sink](TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>{}){
            // nop
        };
};

auto setupQEP(const NodeEnginePtr& engine, QueryId queryId) {
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);

    DataSinkPtr sink = createTextFileSink(sch, queryId, engine, filePath, false);
    auto context = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink);
    auto executable = std::make_shared<TextExecutablePipeline>();
    auto pipeline = ExecutablePipeline::create(0, queryId, context, executable, 1, {sink});
    auto source =
        createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12, {pipeline});
    auto executionPlan = ExecutableQueryPlan::create(queryId,
                                                     queryId,
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
TEST_F(EngineTest, testStartStopEngineEmpty) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto engine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);
    EXPECT_TRUE(engine->stop());
}

TEST_F(EngineTest, teststartDeployStop) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto engine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);

    auto [qep, pipeline] = setupQEP(engine, testQueryId);
    EXPECT_TRUE(engine->deployQueryInNodeEngine(qep));
    pipeline->completedPromise.get_future().get();
    EXPECT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    EXPECT_TRUE(engine->stop());

    testOutput();
}

TEST_F(EngineTest, testStartDeployUndeployStop) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto ptr = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);

    auto [qep, pipeline] = setupQEP(ptr, testQueryId);
    EXPECT_TRUE(ptr->deployQueryInNodeEngine(qep));
    EXPECT_TRUE(ptr->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    pipeline->completedPromise.get_future().get();
    EXPECT_TRUE(ptr->undeployQuery(testQueryId));
    EXPECT_TRUE(ptr->stop());

    testOutput();
}

TEST_F(EngineTest, testStartRegisterStartStopDeregisterStop) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto ptr = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);

    auto [qep, pipeline] = setupQEP(ptr, testQueryId);
    EXPECT_TRUE(ptr->registerQueryInNodeEngine(qep));
    EXPECT_TRUE(ptr->startQuery(testQueryId));
    EXPECT_TRUE(ptr->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    pipeline->completedPromise.get_future().get();
    EXPECT_TRUE(ptr->stopQuery(testQueryId));

    EXPECT_TRUE(ptr->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Stopped);

    EXPECT_TRUE(ptr->unregisterQuery(testQueryId));
    EXPECT_TRUE(ptr->stop());

    testOutput();
}
//
TEST_F(EngineTest, testParallelDifferentSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto engine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);

    //  GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);

    auto sink1 = createTextFileSink(sch1, 0, engine, "qep1.txt", false);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(0, 1, context1, executable1, 1, {sink1});
    auto source1 =
        createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12, {pipeline1});
    auto executionPlan =
        ExecutableQueryPlan::create(1, 1, {source1}, {sink1}, {pipeline1}, engine->getQueryManager(), engine->getBufferManager());

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sink2 = createTextFileSink(sch2, 0, engine, "qep2.txt", false);
    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink2);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(0, 2, context2, executable2, 1, {sink2});
    auto source2 =
        createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 2, 12, {pipeline2});
    auto executionPlan2 =
        ExecutableQueryPlan::create(2, 2, {source2}, {sink2}, {pipeline2}, engine->getQueryManager(), engine->getBufferManager());

    EXPECT_TRUE(engine->registerQueryInNodeEngine(executionPlan));
    EXPECT_TRUE(engine->registerQueryInNodeEngine(executionPlan2));

    EXPECT_TRUE(engine->startQuery(1));
    EXPECT_TRUE(engine->startQuery(2));

    EXPECT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Running);
    EXPECT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Running);

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    EXPECT_TRUE(engine->stopQuery(1, true));
    EXPECT_TRUE(engine->stopQuery(2, true));

    EXPECT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Stopped);
    EXPECT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Stopped);

    EXPECT_TRUE(engine->stop());

    EXPECT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Invalid);
    EXPECT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Invalid);

    testOutput("qep1.txt");
    testOutput("qep2.txt");
}
//
TEST_F(EngineTest, testParallelSameSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto engine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);

    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);

    auto sink1 = createTextFileSink(sch1, 1, engine, "qep1.txt", true);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(0, 1, context1, executable1, 1, {sink1});
    auto source1 =
        createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12, {pipeline1});
    auto executionPlan =
        ExecutableQueryPlan::create(1, 1, {source1}, {sink1}, {pipeline1}, engine->getQueryManager(), engine->getBufferManager());

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink2 = createTextFileSink(sch2, 2, engine, "qep2.txt", true);

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink2);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(1, 2, context2, executable2, 1, {sink2});
    DataSourcePtr source2 =
        createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 2, 12, {pipeline2});
    auto executionPlan2 =
        ExecutableQueryPlan::create(2, 2, {source2}, {sink2}, {pipeline2}, engine->getQueryManager(), engine->getBufferManager());

    EXPECT_TRUE(engine->registerQueryInNodeEngine(executionPlan));
    EXPECT_TRUE(engine->registerQueryInNodeEngine(executionPlan2));

    EXPECT_TRUE(engine->startQuery(1));
    EXPECT_TRUE(engine->startQuery(2));

    EXPECT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Running);
    EXPECT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Running);

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    EXPECT_TRUE(engine->undeployQuery(1));
    EXPECT_TRUE(engine->undeployQuery(2));
    EXPECT_TRUE(engine->stop());

    testOutput("qep1.txt");
    testOutput("qep2.txt");
}
//
TEST_F(EngineTest, testParallelSameSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto engine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);

    // create two executable query plans, which emit to the same sink
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sharedSink = createTextFileSink(sch1, 0, engine, "qep12.txt", false);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sharedSink);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(1, 1, context1, executable1, 1, {sharedSink});
    auto source1 =
        createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12, {pipeline1});
    auto executionPlan = ExecutableQueryPlan::create(1,
                                                     1,
                                                     {source1},
                                                     {sharedSink},
                                                     {pipeline1},
                                                     engine->getQueryManager(),
                                                     engine->getBufferManager());

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sharedSink);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(2, 2, context2, executable2, 1, {sharedSink});

    DataSourcePtr source2 =
        createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 2, 12, {pipeline2});

    auto executionPlan2 = ExecutableQueryPlan::create(2,
                                                      2,
                                                      {source2},
                                                      {sharedSink},
                                                      {pipeline2},
                                                      engine->getQueryManager(),
                                                      engine->getBufferManager());

    EXPECT_TRUE(engine->registerQueryInNodeEngine(executionPlan));
    EXPECT_TRUE(engine->registerQueryInNodeEngine(executionPlan2));

    EXPECT_TRUE(engine->startQuery(1));
    EXPECT_TRUE(engine->startQuery(2));

    EXPECT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Running);
    EXPECT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Running);

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    EXPECT_TRUE(engine->undeployQuery(1));
    EXPECT_TRUE(engine->undeployQuery(2));
    EXPECT_TRUE(engine->stop());
    testOutput("qep12.txt", joinedExpectedOutput);
}
//
TEST_F(EngineTest, DISABLED_testParallelSameSourceAndSinkRegstart) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto engine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sink1 = createTextFileSink(sch1, 0, engine, "qep3.txt", true);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(0, 1, context1, executable1, 1, {sink1});

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(1, 2, context2, executable2, 1, {sink1});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                1,
                                                                12,
                                                                {pipeline1, pipeline2});
    auto executionPlan =
        ExecutableQueryPlan::create(1, 1, {source1}, {sink1}, {pipeline1}, engine->getQueryManager(), engine->getBufferManager());

    auto executionPlan2 =
        ExecutableQueryPlan::create(2, 2, {source1}, {sink1}, {pipeline2}, engine->getQueryManager(), engine->getBufferManager());

    //GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source1 =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    /*
    DataSinkPtr sink1 = createTextFileSink(sch1, 0, engine, "qep3.txt", true);
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

    EXPECT_TRUE(engine->registerQueryInNodeEngine(executionPlan));
    EXPECT_TRUE(engine->registerQueryInNodeEngine(executionPlan2));

    EXPECT_TRUE(engine->startQuery(1));
    EXPECT_TRUE(engine->startQuery(2));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    EXPECT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Running);
    EXPECT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Running);

    EXPECT_TRUE(engine->undeployQuery(1));
    EXPECT_TRUE(engine->undeployQuery(2));
    EXPECT_TRUE(engine->stop());

    testOutput("qep3.txt", joinedExpectedOutput);
}
//
TEST_F(EngineTest, testStartStopStartStop) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto engine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);

    auto [qep, pipeline] = setupQEP(engine, testQueryId);
    EXPECT_TRUE(engine->deployQueryInNodeEngine(qep));
    pipeline->completedPromise.get_future().get();

    EXPECT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);

    EXPECT_TRUE(engine->undeployQuery(testQueryId));
    EXPECT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Invalid);

    EXPECT_TRUE(engine->stop());
    EXPECT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Invalid);
    testOutput();
}

namespace detail {
void segkiller() { raise(SIGSEGV); }

void assertKiller() {
    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        using Runtime::NodeEngine::NodeEngine;

        explicit MockedNodeEngine(PhysicalStreamConfigPtr config,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  BufferStoragePtr&& bufferStorage,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  uint64_t nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(config),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(bufferStorage),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(compiler),
                         std::make_shared<NES::Runtime::StateManager>(nodeEngineId),
                         std::make_shared<Experimental::MaterializedView::MaterializedViewManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string callstack) override {
            stop(false);
            EXPECT_TRUE(strcmp(exception->what(),
                               "Failed assertion on false error message: this will fail now with a NesRuntimeException")
                        == 0);
            NodeEngine::onFatalException(exception, std::move(callstack));
        }
    };
    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 31340);
    NES_ASSERT(false, "this will fail now with a NesRuntimeException");
}
}// namespace detail

TEST_F(EngineTest, DISABLED_testExceptionCrash) { EXPECT_EXIT(detail::assertKiller(), testing::ExitedWithCode(1), ""); }

TEST_F(EngineTest, DISABLED_testSemiUnhandledExceptionCrash) {
    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        std::promise<bool> completedPromise;
        explicit MockedNodeEngine(PhysicalStreamConfigPtr&& config,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  BufferStoragePtr&& bufferStorage,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  uint64_t nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(config),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(bufferStorage),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(compiler),
                         std::make_shared<NES::Runtime::StateManager>(nodeEngineId),
                         std::make_shared<Experimental::MaterializedView::MaterializedViewManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string) override {
            const auto* str = exception->what();
            NES_ERROR(str);
            EXPECT_TRUE(strcmp(str, "Got fatal error on thread 0: Catch me if you can!") == 0);
            completedPromise.set_value(true);
            stop(true);
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

    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 31337);

    //GeneratedQueryExecutionPlanBuilder builder = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createTextFileSink(sch, 0, engine, filePath, true);
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
    //EXPECT_TRUE(engine->deployQueryInNodeEngine(qep));
    EXPECT_TRUE(engine->completedPromise.get_future().get());
    EXPECT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::ErrorState);
    EXPECT_TRUE(engine->stop());
}

TEST_F(EngineTest, DISABLED_testFullyUnhandledExceptionCrash) {
    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        std::promise<bool> completedPromise;

        explicit MockedNodeEngine(PhysicalStreamConfigPtr&& config,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  BufferStoragePtr&& bufferStorage,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  uint64_t nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(config),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(bufferStorage),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(compiler),
                         std::make_shared<NES::Runtime::StateManager>(0),
                         std::make_shared<Experimental::MaterializedView::MaterializedViewManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string) override {
            const auto* str = exception->what();
            NES_ERROR(str);
            EXPECT_TRUE(strcmp(str, "Unknown exception caught") == 0);
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
    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 31337);

    //GeneratedQueryExecutionPlanBuilder builder = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createTextFileSink(sch, 0, engine, filePath, true);
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
    //EXPECT_TRUE(engine->deployQueryInNodeEngine(qep));
    engine->completedPromise.get_future().get();
    EXPECT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    EXPECT_TRUE(engine->stop());
}

TEST_F(EngineTest, DISABLED_testFatalCrash) {
    auto engine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31400, PhysicalStreamConfig::createEmpty());
    EXPECT_EXIT(detail::segkiller(), testing::ExitedWithCode(1), "Runtime failed fatally");
}

}// namespace NES
