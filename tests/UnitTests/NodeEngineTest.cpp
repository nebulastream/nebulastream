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

#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <QueryCompiler/ExecutablePipeline.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cassert>
#include <future>
#include <iostream>

using namespace std;
using namespace NES::Windowing;

#define DEBUG_OUTPUT
namespace NES {

uint64_t testQueryId = 123;

std::string expectedOutput =
    "+----------------------------------------------------+\n"
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

class HandCodedExecutablePipeline : public ExecutablePipeline {
  public:
    std::atomic<uint64_t> count = 0;
    std::atomic<uint64_t> sum = 0;
    std::promise<bool> completedPromise;

    uint32_t execute(TupleBuffer& inBuf, QueryExecutionContextPtr context, WorkerContext&) override {
        auto tuples = inBuf.getBufferAs<uint64_t>();

        NES_INFO("Test: Start execution");

        uint64_t psum = 0;
        for (size_t i = 0; i < inBuf.getNumberOfTuples(); ++i) {
            psum += tuples[i];
        }
        count += inBuf.getNumberOfTuples();
        sum += psum;

        NES_INFO(
            "Test: query result = Processed Block:" << inBuf.getNumberOfTuples() << " count: " << count << " psum: "
                                                    << psum << " sum: " << sum);

        if (sum == 10) {
            NES_DEBUG("TEST: result correct");

            TupleBuffer outputBuffer = context->allocateTupleBuffer();

            NES_DEBUG("TEST: got buffer");
            auto arr = outputBuffer.getBufferAs<uint32_t>();
            arr[0] = static_cast<uint32_t>(sum.load());
            outputBuffer.setNumberOfTuples(1);
            NES_DEBUG("TEST: written " << arr[0]);
            WorkerContext wctx{0};
            context->emitBuffer(outputBuffer, wctx);
            completedPromise.set_value(true);
        } else {
            NES_DEBUG("TEST: result wrong ");
            completedPromise.set_value(false);
        }

        NES_DEBUG("TEST: return");
        return 0;
    }
};
typedef std::shared_ptr<HandCodedExecutablePipeline> CompiledTestQueryExecutionPlanPtr;

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
    void SetUp() {
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
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(filePath.c_str());
    EXPECT_TRUE(response == 0);
}

void testOutput(std::string path) {
    ifstream testFile(path.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    EXPECT_TRUE(response == 0);
}

void testOutput(std::string path, std::string expectedOutput) {
    ifstream testFile(path.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    EXPECT_TRUE(response == 0);
}

auto setupQEP(NodeEnginePtr engine, QueryId queryId) {
    GeneratedQueryExecutionPlanBuilder builder = GeneratedQueryExecutionPlanBuilder::create();
    DataSourcePtr source =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(engine->getBufferManager(), engine->getQueryManager());
    SchemaPtr sch = Schema::create()
                        ->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createTextFileSink(sch, 0, engine, filePath, true);
    builder.addSource(source);
    builder.addSink(sink);
    builder.setQueryId(queryId);
    builder.setQuerySubPlanId(queryId);
    builder.setQueryManager(engine->getQueryManager());
    builder.setBufferManager(engine->getBufferManager());
    builder.setCompiler(engine->getCompiler());

    auto context = std::make_shared<PipelineExecutionContext>(queryId, engine->getBufferManager(), [sink](TupleBuffer& buffer, WorkerContext& wctx) {
        sink->writeData(buffer, wctx);
    }, nullptr, nullptr);
    auto executable = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline = PipelineStage::create(0, queryId, executable, context, nullptr);
    builder.addPipelineStage(pipeline);
    return std::make_tuple(builder.build(), executable);
}

//TODO: add test for register and start only

/**
 * Test methods
 *     cout << "Stats=" << ptr->getStatistics() << endl;
 */
TEST_F(EngineTest, testStartStopEngineEmpty) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    ASSERT_TRUE(engine->stop());
}

TEST_F(EngineTest, teststartDeployStop) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    auto [qep, pipeline] = setupQEP(engine, testQueryId);
    ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    pipeline->completedPromise.get_future().get();
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);
    ASSERT_TRUE(engine->stop());

    testOutput();
}

TEST_F(EngineTest, testStartDeployUndeployStop) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto ptr = NodeEngine::create("127.0.0.1", 31337, streamConf);

    auto [qep, pipeline] = setupQEP(ptr, testQueryId);
    ASSERT_TRUE(ptr->deployQueryInNodeEngine(qep));
    pipeline->completedPromise.get_future().get();
    ASSERT_TRUE(ptr->getQueryStatus(testQueryId) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);
    ASSERT_TRUE(ptr->undeployQuery(testQueryId));
    ASSERT_TRUE(ptr->stop());

    testOutput();
}

TEST_F(EngineTest, testStartRegisterStartStopDeregisterStop) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto ptr = NodeEngine::create("127.0.0.1", 31337,streamConf);

    auto [qep, pipeline] = setupQEP(ptr, testQueryId);
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep));
    ASSERT_TRUE(ptr->startQuery(testQueryId));
    pipeline->completedPromise.get_future().get();
    ASSERT_TRUE(ptr->getQueryStatus(testQueryId) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);
    ASSERT_TRUE(ptr->stopQuery(testQueryId));

    ASSERT_TRUE(ptr->getQueryStatus(testQueryId) == QueryExecutionPlan::QueryExecutionPlanStatus::Stopped);

    ASSERT_TRUE(ptr->unregisterQuery(testQueryId));
    ASSERT_TRUE(ptr->stop());

    testOutput();
}
//
TEST_F(EngineTest, testParallelDifferentSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(engine->getBufferManager(), engine->getQueryManager());
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createTextFileSink(sch1, 0, engine, "qep1.txt", true);
    builder1.addSource(source1);
    builder1.addSink(sink1);
    builder1.setQueryId(1);
    builder1.setQuerySubPlanId(1);
    builder1.setQueryManager(engine->getQueryManager());
    builder1.setBufferManager(engine->getBufferManager());
    builder1.setCompiler(engine->getCompiler());
    auto context1 = std::make_shared<PipelineExecutionContext>(0, engine->getBufferManager(), [sink1](TupleBuffer& buffer, WorkerContext& w) {
        sink1->writeData(buffer, w);
    }, nullptr, nullptr);
    auto executable1 = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline1 = PipelineStage::create(0, 1, executable1, context1, nullptr);
    builder1.addPipelineStage(pipeline1);

    GeneratedQueryExecutionPlanBuilder builder2 = GeneratedQueryExecutionPlanBuilder::create();
    DataSourcePtr source2 = createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(engine->getBufferManager(),
                                                                                     engine->getQueryManager());
    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink2 = createTextFileSink(sch2, 0, engine, "qep2.txt", true);
    builder2.addSource(source2);
    builder2.addSink(sink2);
    builder2.setQueryId(2);
    builder2.setQuerySubPlanId(2);
    builder2.setQueryManager(engine->getQueryManager());
    builder2.setBufferManager(engine->getBufferManager());
    builder2.setCompiler(engine->getCompiler());
    auto context2 = std::make_shared<PipelineExecutionContext>(0, engine->getBufferManager(), [sink2](TupleBuffer& buffer, WorkerContext& w) {
        sink2->writeData(buffer, w);
    }, nullptr, nullptr);
    auto executable2 = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline2 = PipelineStage::create(0, 2, executable2, context2, nullptr);
    builder2.addPipelineStage(pipeline2);

    ASSERT_TRUE(engine->registerQueryInNodeEngine(builder1.build()));
    ASSERT_TRUE(engine->registerQueryInNodeEngine(builder2.build()));

    ASSERT_TRUE(engine->startQuery(1));
    ASSERT_TRUE(engine->startQuery(2));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->getQueryStatus(1) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(2) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);

    ASSERT_TRUE(engine->stopQuery(1));
    ASSERT_TRUE(engine->stopQuery(2));

    ASSERT_TRUE(engine->getQueryStatus(1) == QueryExecutionPlan::QueryExecutionPlanStatus::Stopped);
    ASSERT_TRUE(engine->getQueryStatus(2) == QueryExecutionPlan::QueryExecutionPlanStatus::Stopped);

    ASSERT_TRUE(!engine->stop());

    ASSERT_TRUE(engine->getQueryStatus(1) == QueryExecutionPlan::QueryExecutionPlanStatus::Invalid);
    ASSERT_TRUE(engine->getQueryStatus(2) == QueryExecutionPlan::QueryExecutionPlanStatus::Invalid);

    testOutput("qep1.txt");
    testOutput("qep2.txt");
}
//
TEST_F(EngineTest, testParallelSameSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(engine->getBufferManager(), engine->getQueryManager());
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createTextFileSink(sch1, 0, engine, "qep1.txt", true);
    builder1.addSource(source1);
    builder1.addSink(sink1);
    builder1.setQueryId(1);
    builder1.setQuerySubPlanId(1);
    builder1.setQueryManager(engine->getQueryManager());
    builder1.setBufferManager(engine->getBufferManager());
    builder1.setCompiler(engine->getCompiler());
    auto context1 = std::make_shared<PipelineExecutionContext>(0, engine->getBufferManager(), [sink1](TupleBuffer& buffer, WorkerContext& w) {
        sink1->writeData(buffer, w);
    }, nullptr, nullptr);
    auto executable1 = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline1 = PipelineStage::create(0, 1, executable1, context1, nullptr);
    builder1.addPipelineStage(pipeline1);

    GeneratedQueryExecutionPlanBuilder builder2 = GeneratedQueryExecutionPlanBuilder::create();
    DataSourcePtr source2 = createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(engine->getBufferManager(),
                                                                                     engine->getQueryManager());
    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink2 = createTextFileSink(sch2, 0, engine, "qep2.txt", true);
    builder2.addSource(source2);
    builder2.addSink(sink2);
    builder2.setQueryId(2);
    builder2.setQuerySubPlanId(2);
    builder2.setQueryManager(engine->getQueryManager());
    builder2.setBufferManager(engine->getBufferManager());
    builder2.setCompiler(engine->getCompiler());
    auto context2 = std::make_shared<PipelineExecutionContext>(0, engine->getBufferManager(), [sink2](TupleBuffer& buffer, WorkerContext& w) {
        sink2->writeData(buffer, w);
    }, nullptr, nullptr);
    auto executable2 = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline2 = PipelineStage::create(0, 2, executable2, context2, nullptr);
    builder2.addPipelineStage(pipeline2);

    ASSERT_TRUE(engine->registerQueryInNodeEngine(builder1.build()));
    ASSERT_TRUE(engine->registerQueryInNodeEngine(builder2.build()));

    ASSERT_TRUE(engine->startQuery(1));
    ASSERT_TRUE(engine->startQuery(2));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->getQueryStatus(1) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(2) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);

    ASSERT_TRUE(engine->undeployQuery(1));
    ASSERT_TRUE(engine->undeployQuery(2));
    ASSERT_TRUE(engine->stop());

    testOutput("qep1.txt");
    testOutput("qep2.txt");
}
//
TEST_F(EngineTest, testParallelSameSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(engine->getBufferManager(), engine->getQueryManager());
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createTextFileSink(sch1, 0, engine, "qep12.txt", true);
    builder1.addSource(source1);
    builder1.addSink(sink1);
    builder1.setQueryId(1);
    builder1.setQuerySubPlanId(1);
    builder1.setQueryManager(engine->getQueryManager());
    builder1.setBufferManager(engine->getBufferManager());
    builder1.setCompiler(engine->getCompiler());
    auto context1 = std::make_shared<PipelineExecutionContext>(0, engine->getBufferManager(), [sink1](TupleBuffer& buffer, WorkerContext& w) {
        sink1->writeData(buffer, w);
    }, nullptr, nullptr);
    auto executable1 = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline1 = PipelineStage::create(0, 1, executable1, context1, nullptr);
    builder1.addPipelineStage(pipeline1);

    GeneratedQueryExecutionPlanBuilder builder2 = GeneratedQueryExecutionPlanBuilder::create();
    DataSourcePtr source2 = createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(engine->getBufferManager(),
                                                                                     engine->getQueryManager());
    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    builder2.addSource(source2);
    builder2.addSink(sink1);
    builder2.setQueryId(2);
    builder2.setQuerySubPlanId(2);
    builder2.setQueryManager(engine->getQueryManager());
    builder2.setBufferManager(engine->getBufferManager());
    builder2.setCompiler(engine->getCompiler());
    auto context2 = std::make_shared<PipelineExecutionContext>(0, engine->getBufferManager(), [sink1](TupleBuffer& buffer, WorkerContext& w) {
        sink1->writeData(buffer, w);
    }, nullptr, nullptr);
    auto executable2 = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline2 = PipelineStage::create(0, 2, executable2, context2, nullptr);
    builder2.addPipelineStage(pipeline2);

    ASSERT_TRUE(engine->registerQueryInNodeEngine(builder1.build()));
    ASSERT_TRUE(engine->registerQueryInNodeEngine(builder2.build()));

    ASSERT_TRUE(engine->startQuery(1));
    ASSERT_TRUE(engine->startQuery(2));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->getQueryStatus(1) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(2) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);

    ASSERT_TRUE(engine->undeployQuery(1));
    ASSERT_TRUE(engine->undeployQuery(2));
    ASSERT_TRUE(engine->stop());
    testOutput("qep12.txt", joinedExpectedOutput);
}
//
TEST_F(EngineTest, testParallelSameSourceAndSinkRegstart) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(engine->getBufferManager(), engine->getQueryManager());
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createTextFileSink(sch1, 0, engine, "qep3.txt", true);
    builder1.addSource(source1);
    builder1.addSink(sink1);
    builder1.setQueryId(1);
    builder1.setQuerySubPlanId(1);
    builder1.setQueryManager(engine->getQueryManager());
    builder1.setBufferManager(engine->getBufferManager());
    builder1.setCompiler(engine->getCompiler());
    auto context1 = std::make_shared<PipelineExecutionContext>(0, engine->getBufferManager(), [sink1](TupleBuffer& buffer, WorkerContext& w) {
        sink1->writeData(buffer, w);
    }, nullptr, nullptr);
    auto executable1 = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline1 = PipelineStage::create(0, 1, executable1, context1, nullptr);
    builder1.addPipelineStage(pipeline1);

    GeneratedQueryExecutionPlanBuilder builder2 = GeneratedQueryExecutionPlanBuilder::create();

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    builder2.addSource(source1);
    builder2.addSink(sink1);
    builder2.setQueryId(2);
    builder2.setQuerySubPlanId(2);
    builder2.setQueryManager(engine->getQueryManager());
    builder2.setBufferManager(engine->getBufferManager());
    builder2.setCompiler(engine->getCompiler());
    auto context2 = std::make_shared<PipelineExecutionContext>(0, engine->getBufferManager(), [sink1](TupleBuffer& buffer, WorkerContext& w) {
        sink1->writeData(buffer, w);
    },
        nullptr, nullptr);
    auto executable2 = std::make_shared<HandCodedExecutablePipeline>();
    auto pipeline2 = PipelineStage::create(0, 2, executable2, context2, nullptr);
    builder2.addPipelineStage(pipeline2);

    ASSERT_TRUE(engine->registerQueryInNodeEngine(builder1.build()));
    ASSERT_TRUE(engine->registerQueryInNodeEngine(builder2.build()));

    ASSERT_TRUE(engine->startQuery(1));
    ASSERT_TRUE(engine->startQuery(2));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->getQueryStatus(1) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(2) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);

    ASSERT_TRUE(engine->undeployQuery(1));
    ASSERT_TRUE(engine->undeployQuery(2));
    ASSERT_TRUE(engine->stop());

    testOutput("qep3.txt", joinedExpectedOutput);
}
//
TEST_F(EngineTest, testStartStopStartStop) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    auto [qep, pipeline] = setupQEP(engine, testQueryId);
    ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    pipeline->completedPromise.get_future().get();

    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == QueryExecutionPlan::QueryExecutionPlanStatus::Running);

    ASSERT_TRUE(engine->undeployQuery(testQueryId));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == QueryExecutionPlan::QueryExecutionPlanStatus::Invalid);

    ASSERT_TRUE(engine->stop());
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == QueryExecutionPlan::QueryExecutionPlanStatus::Invalid);
    testOutput();
}
}// namespace NES
