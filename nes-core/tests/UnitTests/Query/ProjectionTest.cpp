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

#include "gtest/gtest.h"

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Network/NetworkChannel.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Services/QueryParsingService.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/DummySink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestSink.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <utility>

#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Util/TestQueryCompiler.hpp>

#include <NesBaseTest.hpp>
#include <Util/TestUtils.hpp>

using namespace NES;
using Runtime::TupleBuffer;

class ProjectionTest : public Testing::NESBaseTest {
  public:
    Optimizer::TypeInferencePhasePtr typeInferencePhase;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("ProjectionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("ProjectionTest: Setup QueryCatalogServiceTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        // create test input buffer
        testSchema = Schema::create()
                         ->addField("test$id", BasicType::INT64)
                         ->addField("test$one", BasicType::INT64)
                         ->addField("test$value", BasicType::INT64);

        windowSchema = Schema::create()
                           ->addField("test$key", BasicType::INT64)
                           ->addField("test$value", BasicType::INT64)
                           ->addField("test$ts", BasicType::UINT64);

        auto sourceConf = PhysicalSource::create("x", "x1");
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->physicalSources.add(sourceConf);
        workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
        workerConfiguration->numberOfBuffersPerWorker.setValue(12);

        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        // Initialize the typeInferencePhase with a dummy SourceCatalog & UdfCatalog
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        Catalogs::UDF::UdfCatalogPtr udfCatalog = Catalogs::UDF::UdfCatalog::create();
        auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("ProjectionTest: Tear down ProjectionTest test case.");
        ASSERT_TRUE(nodeEngine->stop());
        nodeEngine = nullptr;
        Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("ProjectionTest: Tear down ProjectionTest test class."); }

    SchemaPtr testSchema;
    SchemaPtr windowSchema;
    Runtime::NodeEnginePtr nodeEngine{nullptr};
};

class NonRunnableDataSource : public NES::DefaultSource {
  public:
    explicit NonRunnableDataSource(const SchemaPtr& schema,
                                   const Runtime::BufferManagerPtr& bufferManager,
                                   const Runtime::QueryManagerPtr& queryManager,
                                   uint64_t numbersOfBufferToProduce,
                                   uint64_t gatheringInterval,
                                   OperatorId operatorId,
                                   OriginId originId,
                                   size_t numSourceLocalBuffers,
                                   const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
        : DefaultSource(schema,
                        bufferManager,
                        queryManager,
                        numbersOfBufferToProduce,
                        gatheringInterval,
                        operatorId,
                        originId,
                        numSourceLocalBuffers,
                        successors) {
        wasGracefullyStopped = NES::Runtime::QueryTerminationType::HardStop;
    }

    void runningRoutine() override {
        open();
        completedPromise.set_value(canTerminate.get_future().get());
        close();
    }

    bool stop(Runtime::QueryTerminationType termination) override {
        canTerminate.set_value(true);
        return NES::DefaultSource::stop(termination);
    }

  private:
    std::promise<bool> canTerminate;
};

DataSourcePtr createNonRunnableSource(const SchemaPtr& schema,
                                      const Runtime::BufferManagerPtr& bufferManager,
                                      const Runtime::QueryManagerPtr& queryManager,
                                      OperatorId operatorId,
                                      OriginId originId,
                                      size_t numSourceLocalBuffers,
                                      const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<NonRunnableDataSource>(schema,
                                                   bufferManager,
                                                   queryManager,
                                                   /*bufferCnt*/ 1,
                                                   /*frequency*/ 1000,
                                                   operatorId,
                                                   originId,
                                                   numSourceLocalBuffers,
                                                   successors);
}

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
    std::promise<bool> canTerminate;

    explicit WindowSource(SchemaPtr schema,
                          Runtime::BufferManagerPtr bufferManager,
                          Runtime::QueryManagerPtr queryManager,
                          const uint64_t numbersOfBufferToProduce,
                          uint64_t frequency,
                          bool varyWatermark,
                          bool decreaseTime,
                          int64_t timestamp,
                          std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : DefaultSource(std::move(schema),
                        std::move(bufferManager),
                        std::move(queryManager),
                        numbersOfBufferToProduce,
                        frequency,
                        1,
                        0,
                        12,
                        std::move(successors)),
          timestamp(timestamp), varyWatermark(varyWatermark), decreaseTime(decreaseTime) {}

    void close() override {
        canTerminate.get_future().get();
        NES::DefaultSource::close();
    }

    bool stop(Runtime::QueryTerminationType termination) override {
        canTerminate.set_value(true);
        return NES::DefaultSource::stop(termination);
    }

    std::optional<TupleBuffer> receiveData() override {
        auto buffer = bufferManager->getBufferBlocking();
        auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
        auto bindedRowLayout = rowLayout->bind(buffer);

        for (int i = 0; i < 10; i++) {
            Runtime::MemoryLayouts::RowLayoutField<int16_t, true>::create(0, rowLayout, buffer)[i] = 1;
            Runtime::MemoryLayouts::RowLayoutField<int16_t, true>::create(1, rowLayout, buffer)[i] = 1;

            if (varyWatermark) {
                if (!decreaseTime) {
                    Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, rowLayout, buffer)[i] = timestamp++;
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
                            Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, rowLayout, buffer)[i] = timestamp++;
                        } else {
                            Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, rowLayout, buffer)[i] =
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
                        Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, rowLayout, buffer)[i] = timestamp;
                    }
                }
            } else {
                Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, rowLayout, buffer)[i] = timestamp;
            }
        }
        buffer.setNumberOfTuples(10);
        timestamp = timestamp + 10;
        runCnt++;

        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, buffer);
        NES_DEBUG("ProjectionTest: source buffer=" << dynamicTupleBuffer);
        return buffer;
    };

    static DataSourcePtr create(const Runtime::BufferManagerPtr& bufferManager,
                                const Runtime::QueryManagerPtr& queryManager,
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
                                              std::vector<Runtime::Execution::SuccessorExecutablePipeline>());
    }

    static DataSourcePtr create(const SchemaPtr& schema,
                                const Runtime::BufferManagerPtr& bufferManager,
                                const Runtime::QueryManagerPtr& queryManager,
                                const uint64_t numbersOfBufferToProduce,
                                uint64_t frequency,
                                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                const bool varyWatermark = false,
                                bool decreaseTime = false,
                                int64_t timestamp = 5) {

        return std::make_shared<WindowSource>(schema,
                                              bufferManager,
                                              queryManager,
                                              numbersOfBufferToProduce,
                                              frequency,
                                              varyWatermark,
                                              decreaseTime,
                                              timestamp,
                                              successors);
    }
};

void fillBuffer(TupleBuffer& buf, const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout) {

    auto recordIndexFields = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, memoryLayout, buf);
    auto fields01 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(1, memoryLayout, buf);
    auto fields02 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(2, memoryLayout, buf);

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 3;
        fields02[recordIndex] = 8;
    }

    buf.setNumberOfTuples(10);
}

TEST_F(ProjectionTest, projectionQueryCorrectField) {
    auto sourceConf = PhysicalSource::create("x", "x1");

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(testSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           0,
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = TestSink::create(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id")).sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 64};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(buffer, memoryLayout);

        plan->getPipelines()[0]->execute(buffer, workerContext);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10ULL);

        auto resultLayout =
            Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());
        auto resultRecordIndexFields =
            Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, resultLayout, resultBuffer);
        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            // id
            EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
        }
    }
    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));
}

TEST_F(ProjectionTest, projectionQueryWrongField) {
    auto sourceConf = PhysicalSource::create("x", "x1");

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(testSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           0,
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("value")).sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 64};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(buffer, memoryLayout);

        plan->getPipelines()[0]->execute(buffer, workerContext);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10ULL);

        auto resultLayout =
            Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());
        auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
        auto resultRecordIndexFields =
            Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, resultLayout, resultBuffer);
        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            // id
            EXPECT_EQ(resultRecordIndexFields[recordIndex], 8);
        }
    }
    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));
}

TEST_F(ProjectionTest, projectionQueryTwoCorrectField) {
    auto sourceConf = PhysicalSource::create("x", "x1");

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(testSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           0,
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64)->addField("value", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id"), Attribute("value")).sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 64};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(buffer, memoryLayout);

        plan->getPipelines()[0]->execute(buffer, workerContext);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10ULL);

        auto resultLayout =
            Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());
        auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
        auto resultRecordIndexFields =
            Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, resultLayout, resultBuffer);
        auto resultFields01 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(1, resultLayout, resultBuffer);

        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            // id
            EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
            EXPECT_EQ(resultFields01[recordIndex], 8);
        }
    }
    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));
}

TEST_F(ProjectionTest, projectOneExistingOneNotExistingField) {
    auto sourceConf = PhysicalSource::create("x", "x1");

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(testSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           0,
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id"), Attribute("asd")).sink(testSinkDescriptor);

    try {
        auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
        FAIL();
    } catch (...) {
        SUCCEED();
    }
}

TEST_F(ProjectionTest, projectNotExistingField) {
    auto sourceConf = PhysicalSource::create("x", "x1");

    // creating query plan
    auto query = TestQuery::from(testSchema).project(Attribute("asd")).sink(DummySink::create());

    try {
        auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
        FAIL();
    } catch (...) {
        SUCCEED();
    }
}

TEST_F(ProjectionTest, tumblingWindowQueryTestWithProjection) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 0,
                                        successors);
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10));

    auto aggregation = Sum(Attribute("value"));
    query = query.window(windowType).byKey(Attribute("key")).apply(aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField(createField("key", INT64))
                                  ->addField("value", INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    Optimizer::DistributeWindowRulePtr distributeWindowRule =
        Optimizer::DistributedWindowRule::create(Configurations::OptimizerConfiguration());
    queryPlan = distributeWindowRule->apply(queryPlan);
    queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
    queryPlan = originIdInferencePhase->execute(queryPlan);
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    ASSERT_TRUE(nodeEngine->registerQueryInNodeEngine(plan));
    ASSERT_TRUE(nodeEngine->startQuery(0));

    // wait till all buffers have been produced
    ASSERT_EQ(testSink->completed.get_future().get(), 1UL);
    NES_INFO("QueryExecutionTest: The test sink contains " << testSink->getNumberOfResultBuffers() << " result buffers.");
    // get result buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);

    if (auto resultBuffer = testSink->get(0); !!resultBuffer) {
        auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, resultBuffer);
        NES_DEBUG("ProjectionTest: buffer=" << dynamicTupleBuffer);

        //TODO 1 Tuple im result buffer in 312 2 results?
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1ULL);

        auto resultLayout =
            Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, nodeEngine->getBufferManager()->getBufferSize());
        auto startFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(0, resultLayout, resultBuffer);
        auto endFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(1, resultLayout, resultBuffer);
        auto keyFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, resultLayout, resultBuffer);
        auto valueFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(3, resultLayout, resultBuffer);

        for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
            // start
            EXPECT_EQ(startFields[recordIndex], 0ULL);
            // end
            EXPECT_EQ(endFields[recordIndex], 10ULL);
            // key
            EXPECT_EQ(keyFields[recordIndex], 1ULL);
            // value
            EXPECT_EQ(valueFields[recordIndex], 10ULL);
        }
    }
    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));
}

TEST_F(ProjectionTest, tumblingWindowQueryTestWithWrongProjection) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource = WindowSource::create(nodeEngine->getBufferManager(),
                                             nodeEngine->getQueryManager(),
                                             /*bufferCnt*/ 2,
                                             /*frequency*/ 1000);

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

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine);
    query.sink(DummySink::create());

    bool success = false;

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
TEST_F(ProjectionTest, mergeQueryWithWrongProjection) {

    EXPECT_THROW(
        {// created buffer per source * number of sources
            uint64_t expectedBuf = 20;

            PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");

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
            SchemaPtr ptr = testSchema->copy();
            auto mergedQuery = query2.unionWith(query1).sink(DummySink::create());

            auto testSink = std::make_shared<TestSink>(expectedBuf, testSchema, nodeEngine);

            auto queryPlan = typeInferencePhase->execute(mergedQuery.getQueryPlan());
        },
        TypeInferenceException);
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
// So, merge is a blocking window_scan with two children.
TEST_F(ProjectionTest, mergeQuery) {
    // created buffer per source * number of sources
    auto sourceConf = PhysicalSource::create("x", "x1");

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(testSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           0,
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64)->addField("value", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    //    auto query1 = TestQuery::from(testSchema);
    auto query1 = TestQuery::from(testSourceDescriptor);
    query1 = query1.filter(Attribute("id") < 5);

    // creating P2
    auto query2 = TestQuery::from(testSourceDescriptor).filter(Attribute("id") <= 5);

    // creating P3
    // merge does not change schema
    SchemaPtr ptr = testSchema->copy();
    auto mergedQuery = query2.unionWith(query1).project(Attribute("id")).sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(mergedQuery.getQueryPlan());
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    ASSERT_FALSE(result->hasError());
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 3U);
    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 64};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(buffer, memoryLayout);
        // TODO do not rely on sleeps

        plan->getPipelines()[0]->execute(buffer, workerContext);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10ULL);

        auto resultLayout =
            Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());
        auto bindedRowLayoutResult = resultLayout->bind(resultBuffer);
        auto resultRecordIndexFields =
            Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, resultLayout, resultBuffer);
        auto resultFields01 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(1, resultLayout, resultBuffer);

        for (int recordIndex = 0; recordIndex < 5; recordIndex++) {
            EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex * 2);
        }
    }
    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));
}