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

#include "gtest/gtest.h"

#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutField.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>
#include <iostream>
#include <utility>

#include "../../util/DummySink.hpp"
#include "../../util/SchemaSourceDescriptor.hpp"
#include "../../util/TestQuery.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Topology/TopologyNode.hpp>

#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

#include "../../util/TestQueryCompiler.hpp"
#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/LocalBufferPool.hpp>

using namespace NES;
using NodeEngine::TupleBuffer;

class ProjectionTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("ProjectionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("ProjectionTest: Setup QueryCatalogTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() {
        // create test input buffer
        testSchema = Schema::create()
                         ->addField("test$id", BasicType::INT64)
                         ->addField("test$one", BasicType::INT64)
                         ->addField("test$value", BasicType::INT64);
        auto streamConf = PhysicalStreamConfig::createEmpty();
        nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, {streamConf}, 1, 4096, 1024, 12, 12);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_DEBUG("ProjectionTest: Tear down ProjectionTest test case.");
        nodeEngine->stop();
        nodeEngine = nullptr;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("ProjectionTest: Tear down ProjectionTest test class."); }

    SchemaPtr testSchema;
    NodeEngine::NodeEnginePtr nodeEngine{nullptr};
};

/**
 * @brief A window source, which generates data consisting of (key, value, ts).
 * Key = 1||2
 * Value = 1
 * Ts = #Iteration
 */
class WindowSource : public NES::DefaultSource {
  public:
    uint64_t runCnt = 0;
    int64_t timestamp;
    bool varyWatermark;
    bool decreaseTime;
    WindowSource(SchemaPtr schema,
                 NodeEngine::BufferManagerPtr bufferManager,
                 NodeEngine::QueryManagerPtr queryManager,
                 const uint64_t numbersOfBufferToProduce,
                 uint64_t frequency,
                 bool varyWatermark,
                 bool decreaseTime,
                 int64_t timestamp,
                 std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors)
        : DefaultSource(std::move(schema),
                        std::move(bufferManager),
                        std::move(queryManager),
                        numbersOfBufferToProduce,
                        frequency,
                        1,
                        12,
                        successors),
          varyWatermark(varyWatermark), decreaseTime(decreaseTime), timestamp(timestamp) {}

    std::optional<TupleBuffer> receiveData() override {
        auto buffer = bufferManager->getBufferBlocking();
        auto rowLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(schema, true);
        auto bindedRowLayout = rowLayout->bind(buffer);

        for (int i = 0; i < 10; i++) {
            NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int16_t, true>::create(0, bindedRowLayout)[i] = 1;
            NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int16_t, true>::create(1, bindedRowLayout)[i] = 1;

            if (varyWatermark) {
                if (!decreaseTime) {
                    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayout)[i] =
                        timestamp++;
                } else {
                    if (runCnt == 0) {
                        /**
                         *in this run, we create normal tuples and one tuples that triggers a very large watermark
                         * first buffer
                         * |key:INT64|value:INT64|ts:UINT64|
                            +----------------------------------------------------+
                            |1|1|30|
                            |1|1|31|
                            |1|1|32|
                            |1|1|33|
                            |1|1|34|
                            |1|1|35|
                            |1|1|36|
                            |1|1|37|
                            |1|1|38|
                            |1|1|59|
                            +----------------------------------------------------+
                         */
                        if (i < 9) {
                            NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2,
                                                                                                           bindedRowLayout)[i] =
                                timestamp++;
                        } else {
                            NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2,
                                                                                                           bindedRowLayout)[i] =
                                timestamp + 20;
                        }
                    } else {
                        /**
                         * in this run we add ts below the current watermark to see if they are part of the result
                         * |key:INT64|value:INT64|ts:UINT64|
                            +----------------------------------------------------+
                            |1|1|48|
                            |1|1|47|
                            |1|1|46|
                            |1|1|45|
                            |1|1|44|
                            |1|1|43|
                            |1|1|42|
                            |1|1|41|
                            |1|1|40|
                            |1|1|39|
                            +----------------------------------------------------+
                         */
                        timestamp = timestamp - 1 <= 0 ? 0 : timestamp - 1;
                        NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayout)[i] =
                            timestamp;
                    }
                }
            } else {
                NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayout)[i] = timestamp;
            }
        }
        buffer.setNumberOfTuples(10);
        timestamp = timestamp + 10;
        runCnt++;

        NES_DEBUG("ProjectionTest: source buffer=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, schema));
        return buffer;
    };

    static DataSourcePtr create(NodeEngine::BufferManagerPtr bufferManager,
                                NodeEngine::QueryManagerPtr queryManager,
                                const uint64_t numbersOfBufferToProduce,
                                uint64_t frequency,
                                const bool varyWatermark = false,
                                bool decreaseTime = false,
                                int64_t timestamp = 5) {
        auto windowSchema = Schema::create()
                                ->addField("test$key", BasicType::INT64)
                                ->addField("test$value", BasicType::INT64)
                                ->addField("test$ts", BasicType::UINT64)
                                ->addField("test$empty", BasicType::UINT64);
        return std::make_shared<WindowSource>(windowSchema,
                                              bufferManager,
                                              queryManager,
                                              numbersOfBufferToProduce,
                                              frequency,
                                              varyWatermark,
                                              decreaseTime,
                                              timestamp,
                                              std::vector<NodeEngine::Execution::SuccessorExecutablePipeline>());
    }
};

typedef std::shared_ptr<DefaultSource> DefaultSourcePtr;

class TestSink : public SinkMedium {
  public:
    TestSink(uint64_t expectedBuffer, SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), 0), expectedBuffer(expectedBuffer){};

    static std::shared_ptr<TestSink>
    create(uint64_t expectedBuffer, SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager) {
        return std::make_shared<TestSink>(expectedBuffer, schema, bufferManager);
    }

    bool writeData(TupleBuffer& input_buffer, NodeEngine::WorkerContext&) override {
        std::unique_lock lock(m);
        NES_DEBUG("ProjectionTest: TestSink: got buffer " << input_buffer);
        NES_DEBUG("ProjectionTest: PrettyPrintTupleBuffer"
                  << UtilityFunctions::prettyPrintTupleBuffer(input_buffer, getSchemaPtr()));

        resultBuffers.emplace_back(std::move(input_buffer));
        if (resultBuffers.size() == expectedBuffer) {
            completed.set_value(true);
        } else if (resultBuffers.size() > expectedBuffer) {
            EXPECT_TRUE(false);
        }
        return true;
    }

    /**
     * @brief Factory to create a new TestSink.
     * @param expectedBuffer number of buffers expected this sink should receive.
     * @return
     */

    TupleBuffer get(uint64_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    const std::string toString() const override { return ""; }

    void setup() override{};

    std::string toString() { return "Test_Sink"; }

    void shutdown() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    ~TestSink() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

    SinkMediumTypes getSinkMediumType() { return SinkMediumTypes::PRINT_SINK; }

  private:
    void cleanupBuffers() { resultBuffers.clear(); }

    std::mutex m;
    uint64_t expectedBuffer;

  public:
    std::promise<bool> completed;
    std::vector<TupleBuffer> resultBuffers;
};

void fillBuffer(TupleBuffer& buf, NodeEngine::DynamicMemoryLayout::DynamicRowLayoutPtr memoryLayout) {
    auto bindedRowLayout = memoryLayout->bind(buf);

    auto recordIndexFields = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayout);
    auto fields01 = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(1, bindedRowLayout);
    auto fields02 = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(2, bindedRowLayout);

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 3;
        fields02[recordIndex] = 8;
    }

    buf.setNumberOfTuples(10);
}

TEST_F(ProjectionTest, projectionQueryCorrectField) {
    auto streamConf = PhysicalStreamConfig::createEmpty();

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            SourceDescriptorPtr,
            NodeEngine::NodeEnginePtr,
            size_t numSourceLocalBuffers,
            std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 successors);
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id")).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << "plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start(nodeEngine->getStateManager());
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Running);
    NodeEngine::WorkerContext workerContext{1};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(outputSchema, true);
    auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
    auto resultRecordIndexFields =
        NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayoutResult);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        // id
        EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
    }

    testSink->shutdown();
    buffer.release();
    plan->stop();
}

TEST_F(ProjectionTest, projectionQueryWrongField) {
    auto streamConf = PhysicalStreamConfig::createEmpty();

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            SourceDescriptorPtr,
            NodeEngine::NodeEnginePtr,
            size_t numSourceLocalBuffers,
            std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 successors);
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("value")).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << "plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start(nodeEngine->getStateManager());
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Running);
    NodeEngine::WorkerContext workerContext{1};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(outputSchema, true);
    auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
    auto resultRecordIndexFields =
        NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayoutResult);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        // id
        EXPECT_EQ(resultRecordIndexFields[recordIndex], 8);
    }

    testSink->shutdown();
    plan->stop();
}

TEST_F(ProjectionTest, projectionQueryTwoCorrectField) {
    auto streamConf = PhysicalStreamConfig::createEmpty();

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            SourceDescriptorPtr,
            NodeEngine::NodeEnginePtr,
            size_t numSourceLocalBuffers,
            std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 successors);
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64)->addField("value", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id"), Attribute("value")).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << "plan=" << queryPlan->toString() << std::endl;
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Deployed);
    plan->start(nodeEngine->getStateManager());
    ASSERT_EQ(plan->getStatus(), NodeEngine::Execution::ExecutableQueryPlanStatus::Running);
    NodeEngine::WorkerContext workerContext{1};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(outputSchema, true);
    auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
    auto resultRecordIndexFields =
        NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayoutResult);
    auto resultFields01 = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(1, bindedRowLayoutResult);

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        // id
        EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
        EXPECT_EQ(resultFields01[recordIndex], 8);
    }

    testSink->shutdown();
    plan->stop();
}

TEST_F(ProjectionTest, projectOneExistingOneNotExistingField) {
    auto streamConf = PhysicalStreamConfig::createEmpty();

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            SourceDescriptorPtr,
            NodeEngine::NodeEnginePtr,
            size_t numSourceLocalBuffers,
            std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 numSourceLocalBuffers,
                                                                 successors);
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id"), Attribute("asd")).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    try {
        auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
        FAIL();
    } catch (...) {
        SUCCEED();
    }
}

TEST_F(ProjectionTest, projectNotExistingField) {
    auto streamConf = PhysicalStreamConfig::createEmpty();

    // creating query plan
    auto query = TestQuery::from(testSchema).project(Attribute("asd")).sink(DummySink::create());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);

    try {
        auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
        FAIL();
    } catch (...) {
        SUCCEED();
    }
}

TEST_F(ProjectionTest, DISABLED_tumblingWindowQueryTestWithProjection) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource =
        WindowSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), /*bufferCnt*/ 2, /*frequency*/ 1);

    auto query = TestQuery::from(windowSource->getSchema()).project(Attribute("ts"), Attribute("value"), Attribute("key"));
    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10));

    query = query.window(windowType).byKey(Attribute("key", INT64)).apply(Sum(Attribute("value", INT64)));

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto distributeWindowRule = Optimizer::DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);
    std::cout << " plan=" << queryPlan->toString() << std::endl;

    //auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    //auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    /*  std::vector<std::shared_ptr<WindowLogicalOperatorNode>> winOps =
        generatableOperators->getNodesByType<WindowLogicalOperatorNode>();
    winOps[0]->setOutputSchema(windowResultSchema);
    std::vector<std::shared_ptr<SourceLogicalOperatorNode>> leafOps =
        queryPlan->getRootOperators()[0]->getNodesByType<SourceLogicalOperatorNode>();

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       //      .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       .addSource(windowSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    auto plan = builder.build();
    //  nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery(1);
*/
    // wait till all buffers have been produced
    testSink->completed.get_future().get();

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);
    auto resultBuffer = testSink->get(0);

    NES_DEBUG("ProjectionTest: buffer=" << UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, windowResultSchema));
    //TODO 1 Tuple im result buffer in 312 2 results?
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(windowResultSchema, true);
    auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
    auto startFields = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(0, bindedRowLayoutResult);
    auto endFields = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(1, bindedRowLayoutResult);
    auto keyFields = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(2, bindedRowLayoutResult);
    auto valueFields = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(3, bindedRowLayoutResult);

    for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
        // start
        EXPECT_EQ(startFields[recordIndex], 0);
        // end
        EXPECT_EQ(endFields[recordIndex], 10);
        // key
        EXPECT_EQ(keyFields[recordIndex], 1);
        // value
        EXPECT_EQ(valueFields[recordIndex], 10);
    }
}

TEST_F(ProjectionTest, tumblingWindowQueryTestWithWrongProjection) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource =
        WindowSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), /*bufferCnt*/ 2, /*frequency*/ 1000);

    auto query = TestQuery::from(windowSource->getSchema()).project(Attribute("ts"), Attribute("empty"), Attribute("key"));
    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10));

    query = query.window(windowType).byKey(Attribute("key", INT64)).apply(Sum(Attribute("value", INT64)));

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    bool success = false;
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    try {
        auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    } catch (...) {
        SUCCEED();
        success = true;
    }
    EXPECT_TRUE(success);
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
// So, merge is a blocking window_scan with two children.
TEST_F(ProjectionTest, DISABLED_mergeQueryWithWrongProjection) {

    EXPECT_THROW(
        {// created buffer per source * number of sources
            uint64_t expectedBuf = 20;

            PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();

            // auto testSource1 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
            //                                                                 nodeEngine->getQueryManager(), 1, 12);

            auto query1 = TestQuery::from(testSchema);

            query1 = query1.filter(Attribute("id") < 5);

            // creating P2
            // auto testSource2 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
            //                                                                  nodeEngine->getQueryManager(), 1, 12);
            auto query2 = TestQuery::from(testSchema).filter(Attribute("id") <= 5).project(Attribute("id"));

            // creating P3
            // merge does not change schema
            SchemaPtr ptr = std::make_shared<Schema>(testSchema);
            auto mergedQuery = query2.unionWith(&query1).sink(DummySink::create());

            auto testSink = std::make_shared<TestSink>(expectedBuf, testSchema, nodeEngine->getBufferManager());

            auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);

            auto queryPlan = typeInferencePhase->execute(mergedQuery.getQueryPlan());
        },
        TypeInferenceException);
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
// So, merge is a blocking window_scan with two children.
TEST_F(ProjectionTest, DISABLED_mergeQuery) {
    // created buffer per source * number of sources
    uint64_t expectedBuf = 20;

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();

    //  auto testSource1 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
    //                                                                nodeEngine->getQueryManager(), 1, 12);

    auto query1 = TestQuery::from(testSchema);

    query1 = query1.filter(Attribute("id") < 5);

    // creating P2
    // auto testSource2 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
    //                                                                nodeEngine->getQueryManager(), 1, 12);
    auto query2 = TestQuery::from(testSchema).filter(Attribute("id") <= 5);

    // creating P3
    // merge does not change schema
    SchemaPtr ptr = std::make_shared<Schema>(testSchema);
    auto mergedQuery = query2.unionWith(&query1).project(Attribute("id")).sink(DummySink::create());

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(expectedBuf, outputSchema, nodeEngine->getBufferManager());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(mergedQuery.getQueryPlan());
    //auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    // auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);
    /*
    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       //.setCompiler(nodeEngine->getCompiler())
                       .addOperatorQueryPlan(generatableOperators)
                       .setQueryId(1)
                       .setQuerySubPlanId(1)
                       // .addSource(testSource1)
                       // .addSource(testSource2)
                       .addSink(testSink);

    auto plan = builder.build();
  */  //nodeEngine->getQueryManager()->registerQuery(plan);

    // The plan should have three pipeline
    //EXPECT_EQ(plan->getNumberOfPipelines(), 3);

    // TODO switch to event time if that is ready to remove sleep
    auto memoryLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(testSchema, true);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    fillBuffer(buffer, memoryLayout);
    // TODO do not rely on sleeps
    // ingest test data
    //plan->setup();
    //plan->start(nodeEngine->getStateManager());
    NodeEngine::WorkerContext workerContext{1};
    //auto stage_0 = plan->getPipeline(0);
    //auto stage_1 = plan->getPipeline(1);
    for (int i = 0; i < 10; i++) {

        //   stage_0->execute(buffer, workerContext);// P1
        //   stage_1->execute(buffer, workerContext);// P2
        // Contfext -> Context 1 and Context 2;
        //
        // P1 -> P2 -> P3
        // P1 -> 10 tuples -> sum=10;
        // P2 -> 10 tuples -> sum=10;
        // P1 -> 10 tuples -> P2 -> sum =10;
        // P2 -> 20 tuples -> sum=20;
        // TODO why sleep here?
        sleep(1);
    }
    testSink->completed.get_future().get();
    //plan->stop();

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5);// how to interpret this?

    auto resultLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(outputSchema, true);
    auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
    auto recordIndexFields =
        NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayoutResult);

    for (int recordIndex = 0; recordIndex < 5; recordIndex++) {
        EXPECT_EQ(recordIndexFields[recordIndex], recordIndex);
    }

    testSink->shutdown();
}