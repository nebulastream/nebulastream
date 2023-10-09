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

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Network/NetworkChannel.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Services/QueryParsingService.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Core.hpp>
#include <Util/DummySink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestSink.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/TestUtils.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

class QueryExecutionTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup QueryCatalogServiceTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        // create test input buffer
        windowSchema = Schema::create()
                           ->addField("test$key", BasicType::INT64)
                           ->addField("test$value", BasicType::INT64)
                           ->addField("test$ts", BasicType::UINT64);
        testSchema = Schema::create()
                         ->addField("test$id", BasicType::INT64)
                         ->addField("test$one", BasicType::INT64)
                         ->addField("test$value", BasicType::INT64);
        auto defaultSourceType = DefaultSourceType::create();
        PhysicalSourcePtr sourceConf = PhysicalSource::create("default", "default1", defaultSourceType);
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->physicalSources.add(sourceConf);

        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        // enable distributed window optimization
        auto optimizerConfiguration = Configurations::OptimizerConfiguration();
        optimizerConfiguration.performDistributedWindowOptimization = true;
        optimizerConfiguration.distributedWindowChildThreshold = 2;
        optimizerConfiguration.distributedWindowCombinerThreshold = 4;
        distributeWindowRule = Optimizer::DistributedWindowRule::create(optimizerConfiguration);
        originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();

        // Initialize the typeInferencePhase with a dummy SourceCatalog & UDFCatalog
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        Catalogs::UDF::UDFCatalogPtr udfCatalog = Catalogs::UDF::UDFCatalog::create();
        auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down QueryExecutionTest test case.");
        ASSERT_TRUE(nodeEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryExecutionTest: Tear down QueryExecutionTest test class."); }

    Runtime::Execution::ExecutableQueryPlanPtr prepareExecutableQueryPlan(
        QueryPlanPtr queryPlan,
        QueryCompilation::QueryCompilerOptionsPtr options = QueryCompilation::QueryCompilerOptions::createDefaultOptions()) {
        queryPlan = typeInferencePhase->execute(queryPlan);
        queryPlan = distributeWindowRule->apply(queryPlan);
        queryPlan = originIdInferencePhase->execute(queryPlan);
        queryPlan = typeInferencePhase->execute(queryPlan);
        auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
        auto queryCompiler = TestUtils::createTestQueryCompiler(options);
        auto result = queryCompiler->compileQuery(request);
        return result->getExecutableQueryPlan();
    }

    SchemaPtr testSchema;
    SchemaPtr windowSchema;
    Runtime::NodeEnginePtr nodeEngine;
    Optimizer::DistributeWindowRulePtr distributeWindowRule;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::OriginIdInferencePhasePtr originIdInferencePhase;
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

    WindowSource(SchemaPtr schema,
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
        auto buffer = allocateBuffer();

        for (int i = 0; i < 10; i++) {
            buffer[i][0].write<int64_t>(1);
            buffer[i][1].write<int64_t>(1);

            if (varyWatermark) {
                if (!decreaseTime) {
                    buffer[i][2].write<uint64_t>(timestamp++);
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
                            buffer[i][2].write<uint64_t>(timestamp++);
                        } else {
                            buffer[i][2].write<uint64_t>(timestamp + 20);
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
                        buffer[i][2].write<uint64_t>(timestamp);
                    }
                }
            } else {
                buffer[i][2].write<uint64_t>(timestamp);
            }
        }
        buffer.setNumberOfTuples(10);
        timestamp = timestamp + 10;
        runCnt++;
        std::stringstream bufferAsString;
        bufferAsString << buffer;
        NES_DEBUG("QueryExecutionTest: source buffer={}", bufferAsString.str());
        return buffer.getBuffer();
    };

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
        fields01[recordIndex] = 1;
        fields02[recordIndex] = recordIndex % 2;
    }
    buf.setNumberOfTuples(10);
}

void fillBufferWithData(TupleBuffer& buf,
                        const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout,
                        const std::vector<std::vector<int64_t>>& data) {
    int tupleCount = data.size();
    uint64_t fieldCount = memoryLayout->getSchema()->fields.size();

    std::vector<Runtime::MemoryLayouts::RowLayoutField<int64_t>> fields;
    DataTypePtr expectedType = DataTypeFactory::createInt64();
    for (uint64_t fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
        auto field = memoryLayout->getSchema()->fields[fieldIndex];
        NES_ASSERT(expectedType->isEquals(field->getDataType()),
                   "fillBufferWithData: Only Int64 data types in schema supported.");
        fields.push_back(Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(fieldIndex, memoryLayout, buf));
    }

    for (int recordIndex = 0; recordIndex < tupleCount; ++recordIndex) {
        auto tuple = data[recordIndex];
        ASSERT_EQ(fieldCount, tuple.size());// Tuple has the wrong number of field.
        for (uint64_t fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
            fields[fieldIndex][recordIndex] = tuple[fieldIndex];
        }
    }

    buf.setNumberOfTuples(tupleCount);
}

TEST_F(QueryExecutionTest, filterQuery) {

    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](SchemaPtr testSchema,
            OperatorId id,
            OriginId,
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

    auto outputSchema = Schema::create()
                            ->addField("test$id", BasicType::INT64)
                            ->addField("test$one", BasicType::INT64)
                            ->addField("test$value", BasicType::INT64);

    auto outputBufferOptimizationLevels = {
        NES::QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel::ALL,
        NES::QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel::NO,
        NES::QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel::ONLY_INPLACE_OPERATIONS_NO_FALLBACK,
        NES::QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel::
            REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK,
        NES::QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel::REUSE_INPUT_BUFFER_NO_FALLBACK,
        NES::QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel::OMIT_OVERFLOW_CHECK_NO_FALLBACK};

    auto filterProcessingStrategies = {NES::QueryCompilation::QueryCompilerOptions::FilterProcessingStrategy::BRANCHED,
                                       NES::QueryCompilation::QueryCompilerOptions::FilterProcessingStrategy::PREDICATION};
    for (auto outputBufferOptimizationLevel :
         outputBufferOptimizationLevels) {// try different OutputBufferOptimizationLevel's: enum with six states
        for (auto filterProcessingStrategy : filterProcessingStrategies) {// try Predication on/off: bool
            auto options = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
            options->setOutputBufferOptimizationLevel(outputBufferOptimizationLevel);
            options->setFilterProcessingStrategy(filterProcessingStrategy);

            // now, test the query for all possible combinations
            auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
            auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

            // two filter operators to validate correct behaviour of (multiple) branchless predicated filters
            auto query = TestQuery::from(testSourceDescriptor)
                             .filter(Attribute("id") < 6)
                             .map(Attribute("idx2") = Attribute("id") * 2)
                             .filter(Attribute("idx2") < 10)
                             .project(Attribute("id"), Attribute("one"), Attribute("value"))
                             .sink(testSinkDescriptor);

            auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

            auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
            auto queryCompiler = TestUtils::createTestQueryCompiler(options);
            auto result = queryCompiler->compileQuery(request);
            auto plan = result->getExecutableQueryPlan();
            // The plan should have one pipeline
            ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
            EXPECT_EQ(plan->getPipelines().size(), 1u);
            ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
            ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
            ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
            //            ASSERT_EQ(Runtime::WorkerContext::getBufferProvider(), nullptr);
            Runtime::WorkerContext workerContext(1, nodeEngine->getBufferManager(), 4);
            ASSERT_NE(Runtime::WorkerContext::getBufferProviderTLS(), nullptr);
            if (auto inputBuffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!inputBuffer) {
                auto memoryLayout =
                    Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
                fillBuffer(inputBuffer, memoryLayout);

                ASSERT_EQ(plan->getPipelines()[0]->execute(inputBuffer, workerContext), ExecutionResult::Ok);
                // This plan should produce one output buffer
                EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
                auto resultBuffer = testSink->get(0);
                // The output buffer should contain 5 tuple;
                EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);

                auto resultRecordIndexField =
                    Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, memoryLayout, resultBuffer);
                auto resultRecordOneField =
                    Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(1, memoryLayout, resultBuffer);
                auto resultRecordValueField =
                    Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(2, memoryLayout, resultBuffer);

                for (uint32_t recordIndex = 0u; recordIndex < 5u; ++recordIndex) {
                    // id
                    EXPECT_EQ(resultRecordIndexField[recordIndex], recordIndex);
                    // one
                    EXPECT_EQ(resultRecordOneField[recordIndex], 1LL);
                    // id
                    EXPECT_EQ(resultRecordValueField[recordIndex], recordIndex % 2);
                }

            } else {
                FAIL();
            }

            ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));

            // wont be called by runtime as no runtime support in this test
            ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
        }
    }
}

TEST_F(QueryExecutionTest, projectionQuery) {
    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](SchemaPtr testSchema,
            OperatorId id,
            OriginId originId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(testSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           originId,
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).project(Attribute("id")).sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 4};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(buffer, memoryLayout);
        plan->getPipelines()[0]->execute(buffer, workerContext);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1U);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10UL);

        auto resultLayout =
            Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());
        auto resultRecordIndexFields =
            Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, resultLayout, resultBuffer);

        for (uint32_t recordIndex = 0UL; recordIndex < 10UL; ++recordIndex) {
            // id
            EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
        }
    }
    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));
    // need to be called manually here
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

struct __attribute__((packed)) ResultTuple {
    int64_t build$id;
    int64_t build$value;
    int64_t probe$id;
    int64_t probe$one;
    int64_t probe$value;

    bool operator==(const ResultTuple& other) const {
        return (this->probe$id == other.probe$id && this->probe$one == other.probe$one && this->probe$value == other.probe$value
                && this->build$id == other.build$id && this->build$value == other.build$value);
    };
};
std::ostream& operator<<(std::ostream& os, const ResultTuple t) {
    os << "ResultTuple: {" << t.build$id << ", " << t.build$value << ", " << t.probe$id << ", " << t.probe$one << ", "
       << t.probe$value << "}" << std::endl;
    return os;
}

TEST_F(QueryExecutionTest, streamingJoinQuery) {
    // creating sources
    SchemaPtr schemaProbeSide = Schema::create()
                                    ->addField("probe$id1", BasicType::INT64)
                                    ->addField("probe$one", BasicType::INT64)
                                    ->addField("probe$value", BasicType::INT64);
    auto sourceDescriptorProbeSide = std::make_shared<TestUtils::TestSourceDescriptor>(
        schemaProbeSide,
        [&](SchemaPtr testSchema,
            OperatorId id,
            OriginId originId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(testSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           originId,
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    SchemaPtr schemaBuildSide =
        Schema::create()->addField("build$id2", BasicType::INT64)->addField("build$value", BasicType::INT64);
    auto sourceDescriptorBuildSide = std::make_shared<TestUtils::TestSourceDescriptor>(
        schemaBuildSide,
        [&](SchemaPtr schemaBuildSide,
            OperatorId id,
            OriginId originId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(schemaBuildSide,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           originId,
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    auto outputSchema = Schema::create()
                            ->addField("probe$id1", BasicType::INT64)
                            ->addField("probe$one", BasicType::INT64)
                            ->addField("probe$value", BasicType::INT64)
                            ->addField("build$id2", BasicType::INT64)
                            ->addField("build$value", BasicType::INT64);
    auto testSink = TestSink::create(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(sourceDescriptorBuildSide)
                     .joinWith(TestQuery::from(sourceDescriptorProbeSide))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(4)))
                     .sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    // The plan should have two pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 3U);
};

// @Reviewers: This is the Pipeline code generated for the build and probe pipelines, for direct review:
// (todo we can delete this after the review)

//    // BUILD PIPELINE:
//    struct __attribute__((packed)) InputTuple {
//        int64_t build$id2;
//        int64_t build$value;
//    };
//    struct __attribute__((packed)) InputTupleBuild {
//        int64_t build$id2;
//        int64_t build$value;
//    };
//
//    using namespace NES;
//    class ExecutablePipelineStage : public NES::Runtime::Execution::ExecutablePipelineStage {
//
//        ExecutionResult execute(NES::Runtime::TupleBuffer &inputTupleBuffer,
//                                Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext,
//                                Runtime::WorkerContext &workerContext) {
//            /* variable declarations */
//            ExecutionResult ret = ExecutionResult::Ok;
//            int64_t numberOfResultTuples = 0;
//            /* statements section */
//            InputTuple *inputTuples = (InputTuple *) inputTupleBuffer.getBuffer();
//            uint64_t numberOfTuples = inputTupleBuffer.getNumberOfTuples();
//            auto batchJoinOperatorHandler = pipelineExecutionContext.getOperatorHandler<Join::Experimental::BatchJoinOperatorHandler>(1);
//            auto batchJoinHandler = batchJoinOperatorHandler->getBatchJoinHandler<NES::Join::Experimental::BatchJoinHandler, int64_t, InputTuple>();
//            auto hashTable = batchJoinHandler->getHashTable();
//
//            for (uint64_t recordIndex = 0; recordIndex < numberOfTuples; ++recordIndex) {
//                int64_t build$id2_buildKey = inputTuples[recordIndex].build$id2;
//                hashTable->insert(build$id2_buildKey, inputTuples[recordIndex]);
//            };
//            return ret;
//        }
//
//        uint32_t setup(Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext) {
//            auto batchJoinOperatorHandler = pipelineExecutionContext.getOperatorHandler<Join::Experimental::BatchJoinOperatorHandler>(0);
//            auto batchJoinDefinition = batchJoinOperatorHandler->getBatchJoinDefinition();
//            auto resultSchema = batchJoinOperatorHandler->getResultSchema();
//            auto batchJoinHandler = Join::Experimental::BatchJoinHandler<int64_t, InputTupleBuild>::create(batchJoinDefinition, 17);
//            batchJoinOperatorHandler->setBatchJoinHandler(batchJoinHandler);
//
//            return 0;
//        }
//    };
//
//
//    // PROBE PIPELINE:
//    struct __attribute__((packed)) InputTupleProbe {
//        int64_t probe$id1;
//        int64_t probe$one;
//        int64_t probe$value;
//    };
//    struct __attribute__((packed)) InputTupleBuild {
//        int64_t build$id2;
//        int64_t build$value;
//    };
//    struct __attribute__((packed)) ResultTuple {
//        int64_t build$id2;
//        int64_t build$value;
//        int64_t probe$id1;
//        int64_t probe$one;
//        int64_t probe$value;
//    };
//
//    class ExecutablePipelineStage : public NES::Runtime::Execution::ExecutablePipelineStage {
//
//        ExecutionResult execute(NES::Runtime::TupleBuffer &inputTupleBuffer,
//                                Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext,
//                                Runtime::WorkerContext &workerContext) {
//            /* variable declarations */
//            ExecutionResult ret = ExecutionResult::Ok;
//            int64_t numberOfResultTuples = 0;
//
//            /* statements section */
//            InputTuple *inputTuples = (InputTuple *) inputTupleBuffer.getBuffer();
//            uint64_t numberOfTuples = inputTupleBuffer.getNumberOfTuples();
//
//            auto batchJoinOperatorHandler = pipelineExecutionContext.getOperatorHandler<Join::Experimental::BatchJoinOperatorHandler>(0);
//            auto batchJoinHandler = batchJoinOperatorHandler->getBatchJoinHandler<NES::Join::Experimental::BatchJoinHandler, int64_t, InputTupleBuild>();
//            auto hashTable = batchJoinHandler->getHashTable();
//            NES::Runtime::TupleBuffer resultTupleBuffer = workerContext.allocateTupleBuffer();
//            ResultTuple *resultTuples = (ResultTuple *) resultTupleBuffer.getBuffer();
//            int64_t maxTuple = resultTupleBuffer.getBufferSize() / 40;
//
//            for (uint64_t recordIndex = 0; recordIndex < numberOfTuples; ++recordIndex) {
//                if (hashTable->contains(inputTuples[recordIndex].probe$id1)) {
//                    InputBuildTuple joinPartner = hashTable->find(inputTuples[recordIndex].probe$id1);
//                    resultTuples[numberOfResultTuples].build$id2 = joinPartner.build$id2;
//                    resultTuples[numberOfResultTuples].build$value = joinPartner.build$value;
//                    resultTuples[numberOfResultTuples].probe$id1 = inputTuples[recordIndex].probe$id1;
//                    resultTuples[numberOfResultTuples].probe$one = inputTuples[recordIndex].probe$one;
//                    resultTuples[numberOfResultTuples].probe$value = inputTuples[recordIndex].probe$value;
//                    ++numberOfResultTuples;
//                    if (numberOfResultTuples >= maxTuple) {
//                        resultTupleBuffer.setNumberOfTuples(numberOfResultTuples);
//                        resultTupleBuffer.setOriginId(inputTupleBuffer.getOriginId());
//                        resultTupleBuffer.setWatermark(inputTupleBuffer.getWatermark());
//                        pipelineExecutionContext.emitBuffer(resultTupleBuffer, workerContext);
//                        numberOfResultTuples = 0;
//                        resultTupleBuffer = workerContext.allocateTupleBuffer();
//                        resultTuples = (ResultTuple *) resultTupleBuffer.getBuffer();
//
//                    };
//
//                };
//
//            };
//            resultTupleBuffer.setNumberOfTuples(numberOfResultTuples);
//            resultTupleBuffer.setWatermark(inputTupleBuffer.getWatermark());
//            resultTupleBuffer.setOriginId(inputTupleBuffer.getOriginId());
//            resultTupleBuffer.setSequenceNumber(inputTupleBuffer.getSequenceNumber());
//            pipelineExecutionContext.emitBuffer(resultTupleBuffer, workerContext);
//            return ret;;
//        }
//
//        uint32_t setup(Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext) {
//            return 0;;
//        }
//    };

TEST_F(QueryExecutionTest, batchJoinQuery) {
    // creating sources
    SchemaPtr schemaProbeSide = Schema::create()
                                    ->addField("probe$id1", BasicType::INT64)
                                    ->addField("probe$one", BasicType::INT64)
                                    ->addField("probe$value", BasicType::INT64);
    auto sourceDescriptorProbeSide = std::make_shared<TestUtils::TestSourceDescriptor>(
        schemaProbeSide,
        [&](SchemaPtr testSchema,
            OperatorId id,
            OriginId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(testSchema,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           0,// dummy origin id
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    SchemaPtr schemaBuildSide =
        Schema::create()->addField("build$id2", BasicType::INT64)->addField("build$value", BasicType::INT64);
    auto sourceDescriptorBuildSide = std::make_shared<TestUtils::TestSourceDescriptor>(
        schemaBuildSide,
        [&](SchemaPtr schemaBuildSide,
            OperatorId id,
            OriginId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createNonRunnableSource(schemaBuildSide,
                                           nodeEngine->getBufferManager(),
                                           nodeEngine->getQueryManager(),
                                           id,
                                           0,// dummy origin id
                                           numSourceLocalBuffers,
                                           std::move(successors));
        });

    auto outputSchema = Schema::create()
                            ->addField("probe$id1", BasicType::INT64)
                            ->addField("probe$one", BasicType::INT64)
                            ->addField("probe$value", BasicType::INT64)
                            ->addField("build$id2", BasicType::INT64)
                            ->addField("build$value", BasicType::INT64);
    auto testSink = TestSink::create(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(sourceDescriptorProbeSide)
                     .batchJoinWith(TestQuery::from(sourceDescriptorBuildSide))
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    // The plan should have two pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 2U);

    auto memoryLayoutBuildSide =
        Runtime::MemoryLayouts::RowLayout::create(schemaBuildSide, nodeEngine->getBufferManager()->getBufferSize());
    auto memoryLayoutProbeSide =
        Runtime::MemoryLayouts::RowLayout::create(schemaProbeSide, nodeEngine->getBufferManager()->getBufferSize());

    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 4};
    auto bufferBuildSide = nodeEngine->getBufferManager()->getBufferBlocking();
    auto bufferProbeSide = nodeEngine->getBufferManager()->getBufferBlocking();
    auto bufferProbeSide2 = nodeEngine->getBufferManager()->getBufferBlocking();
    ASSERT_TRUE(!!bufferBuildSide && !!bufferProbeSide && !!bufferProbeSide2);

    std::vector<std::vector<int64_t>> dataBuildSide = {{5, 50}, {6, 60}};
    fillBufferWithData(bufferBuildSide, memoryLayoutBuildSide, dataBuildSide);

    std::vector<std::vector<int64_t>> dataProbeSide = {{5, 1, 5000}, {6, 1, 6000}, {6, 1, 6001}};
    fillBufferWithData(bufferProbeSide, memoryLayoutProbeSide, dataProbeSide);

    // 110 tuples in probe side -> we fill result buffer more than once, 2 are emitted
    std::vector<std::vector<int64_t>> dataProbeSide2(110, {6, 1, 6002});
    fillBufferWithData(bufferProbeSide2, memoryLayoutProbeSide, dataProbeSide2);

    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);

    plan->getPipelines()[1]->execute(bufferBuildSide, workerContext);
    plan->getPipelines()[0]->execute(bufferProbeSide, workerContext);
    plan->getPipelines()[0]->execute(bufferProbeSide2, workerContext);

    // This plan should produce two output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 3U);

    auto resultBuffer1 = testSink->get(0);
    auto resultBuffer2 = testSink->get(1);
    auto resultBuffer3 = testSink->get(2);
    // The output buffers should contain 3, 102 and 8 tuple;
    EXPECT_EQ(resultBuffer1.getNumberOfTuples(), 3UL);
    EXPECT_EQ(resultBuffer2.getNumberOfTuples(), 102UL);// 102 is max a buffer can hold
    EXPECT_EQ(resultBuffer3.getNumberOfTuples(), 8UL);  // 8 tuples that couldn't fit in buffer 2

    auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());
    auto resultRecordIndexFields1 =
        Runtime::MemoryLayouts::RowLayoutField<ResultTuple, true>::create(0, resultLayout, resultBuffer1);
    auto resultRecordIndexFields2 =
        Runtime::MemoryLayouts::RowLayoutField<ResultTuple, true>::create(0, resultLayout, resultBuffer2);
    auto resultRecordIndexFields3 =
        Runtime::MemoryLayouts::RowLayoutField<ResultTuple, true>::create(0, resultLayout, resultBuffer3);

    // content of res buffer 1
    std::vector<ResultTuple> expectedTuples1 = {{5, 50, 5, 1, 5000}, {6, 60, 6, 1, 6000}, {6, 60, 6, 1, 6001}};

    // res buffers 2 and 3 will always contain the same tuple:
    ResultTuple expectedTuple2 = {6, 60, 6, 1, 6002};
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(resultRecordIndexFields1[i], expectedTuples1[i]);
    }
    for (int i = 0; i < 102; ++i) {
        EXPECT_EQ(resultRecordIndexFields2[i], expectedTuple2);
    }
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(resultRecordIndexFields3[i], expectedTuple2);
    }

    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));
    // need to be called manually here
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(QueryExecutionTest, arithmeticOperatorsQuery) {
    // creating query plan

    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](SchemaPtr testSchema,
            OperatorId id,
            OriginId,
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

    auto outputSchema = Schema::create()
                            ->addField("id", BasicType::INT64)
                            ->addField("one", BasicType::INT64)
                            ->addField("value", BasicType::INT64)
                            ->addField("result_pow_int", BasicType::FLOAT64)
                            ->addField("result_pow_float", BasicType::FLOAT64)
                            ->addField("result_mod_int", BasicType::INT64)
                            ->addField("result_mod_float", BasicType::FLOAT64)
                            ->addField("result_ceil", BasicType::FLOAT64)
                            ->addField("result_exp", BasicType::FLOAT64)
                            ->addField("result_batch_test", BasicType::BOOLEAN);

    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor)
                     .filter(Attribute("id") < 6)
                     .map(Attribute("result_pow_int") = POWER(2, Attribute("one") + Attribute("id")))    // int
                     .map(Attribute("result_pow_float") = POWER(2.0, Attribute("one") + Attribute("id")))// float
                     .map(Attribute("result_mod_int") = 20 % (Attribute("id") + 1))                      // int
                     .map(Attribute("result_mod_float") = MOD(-20.0, Attribute("id") + Attribute("one")))// float
                     .map(Attribute("result_ceil") = ABS(ROUND(FLOOR(CEIL((Attribute("id")) / 2.0)))))// more detailed tests below
                     .map(Attribute("result_exp") = EXP(Attribute("result_ceil")))
                     .map(Attribute("result_batch_test") =// batch test of many arithmetic operators, should always be 1/TRUE:
                          // test functionality of many functions at once through combination with their inverse function:
                          Attribute("result_ceil") == LN(EXP(Attribute("result_ceil")))
                              && Attribute("result_ceil") == SQRT(POWER(Attribute("result_ceil"), 2))
                              && Attribute("result_ceil") == LOG10(POWER(10, Attribute("result_ceil")))
                              // test FLOOR, ROUND, CEIL, ABS
                              && FLOOR(Attribute("id") / 2.0) <= Attribute("id") / 2.0
                              && Attribute("id") / 2.0 <= CEIL(Attribute("id") / 2.0)
                              && FLOOR(Attribute("id") / 2.0) <= ROUND(Attribute("id") / 2.0)
                              && ROUND(Attribute("id") / 2.0) <= CEIL(Attribute("id") / 2.0)
                              && ABS(1 - Attribute("id")) == ABS(Attribute("id") - 1))
                     .sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 4};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(buffer, memoryLayout);
        plan->getPipelines()[0]->execute(buffer, workerContext);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1U);

        std::string expectedContent = "+----------------------------------------------------+\n"
                                      "|id:INT64|one:INT64|value:INT64|result_pow_int:FLOAT64|result_pow_float:FLOAT64|result_"
                                      "mod_int:INT64|result_mod_float:"
                                      "FLOAT64|result_ceil:FLOAT64|result_exp:FLOAT64|result_batch_test:BOOLEAN|\n"
                                      "+----------------------------------------------------+\n"
                                      "|0|1|0|2.000000|2.000000|0|-0.000000|0.000000|1.000000|1|\n"
                                      "|1|1|1|4.000000|4.000000|0|-0.000000|1.000000|2.718282|1|\n"
                                      "|2|1|0|8.000000|8.000000|2|-2.000000|1.000000|2.718282|1|\n"
                                      "|3|1|1|16.000000|16.000000|0|-0.000000|2.000000|7.389056|1|\n"
                                      "|4|1|0|32.000000|32.000000|0|-0.000000|2.000000|7.389056|1|\n"
                                      "|5|1|1|64.000000|64.000000|2|-2.000000|3.000000|20.085537|1|\n"
                                      "+----------------------------------------------------+";

        auto resultBuffer = testSink->get(0);

        auto rowLayoutActual = Runtime::MemoryLayouts::RowLayout::create(outputSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBufferActual = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayoutActual, resultBuffer);
        std::stringstream bufferAsString;
        bufferAsString << buffer;
        NES_DEBUG("QueryExecutionTest: buffer={}", bufferAsString.str());
        EXPECT_EQ(expectedContent, dynamicTupleBufferActual.toString(outputSchema));
    }
    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));

    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

/**
 * @brief This test verify that the watermark assigned correctly.
 * WindowSource -> WatermarkAssignerOperator -> TestSink
 *
 */
TEST_F(QueryExecutionTest, watermarkAssignerTest) {
    uint64_t millisecondOfallowedLateness = 2U; /*milliseconds of allowedLateness*/

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](SchemaPtr windowSchema,
            OperatorId,
            OriginId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 0,
                                        successors,
                                        /*varyWatermark*/ true);
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. add window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value. Use window of 5ms to ensure that it is closed.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(5));

    // add a watermark assigner operator with the specified allowedLateness
    query = query.assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("ts"),
                                                                               Milliseconds(millisecondOfallowedLateness),
                                                                               Milliseconds()));

    query = query.window(windowType).byKey(Attribute("key", BasicType::INT64)).apply(Sum(Attribute("value", BasicType::INT64)));
    // add a watermark assigner operator with allowedLateness of 1 millisecond

    // 3. add sink. We expect that this sink will receive one buffer
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("_$start", BasicType::UINT64))
                                  ->addField(createField("_$end", BasicType::UINT64))
                                  ->addField(createField("test$key", BasicType::INT64))
                                  ->addField("test$value", BasicType::INT64);

    // each source buffer produce 1 result buffer, totalling 2 buffers
    auto testSink = TestSink::create(/*expected result buffer*/ 2, windowResultSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto plan = prepareExecutableQueryPlan(query.getQueryPlan());

    ASSERT_TRUE(nodeEngine->registerQueryInNodeEngine(plan));
    ASSERT_TRUE(nodeEngine->startQuery(0));

    // wait till all buffers have been produced
    ASSERT_EQ(testSink->completed.get_future().get(), 2UL);

    auto resultBuffer = testSink->get(0);

    // 10 records, starting at ts=5 with 1ms difference each record, hence ts of the last record=14
    EXPECT_EQ(resultBuffer.getWatermark(), 14 - millisecondOfallowedLateness);

    ASSERT_TRUE(nodeEngine->stopQuery(0));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

/**
 * @brief This tests creates a windowed query.
 * WindowSource -> windowOperator -> windowScan -> TestSink
 * The source generates 2. buffers.
 */
TEST_F(QueryExecutionTest, tumblingWindowQueryTest) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](SchemaPtr windowSchema,
            OperatorId,
            OriginId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 0,
                                        std::move(successors));
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("test$ts")), Milliseconds(10));

    query = query.window(windowType).byKey(Attribute("key", BasicType::INT64)).apply(Sum(Attribute("value", BasicType::INT64)));

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("_$start", BasicType::UINT64))
                                  ->addField(createField("_$end", BasicType::UINT64))
                                  ->addField(createField("test$key", BasicType::INT64))
                                  ->addField("test$value", BasicType::INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto plan = prepareExecutableQueryPlan(query.getQueryPlan());

    ASSERT_TRUE(nodeEngine->registerQueryInNodeEngine(plan));
    ASSERT_TRUE(nodeEngine->startQuery(0));

    // wait till all buffers have been produced
    ASSERT_EQ(testSink->completed.get_future().get(), 1UL);

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);
    if (auto resultBuffer = testSink->get(0); !!resultBuffer) {
        auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, resultBuffer);
        std::stringstream bufferAsString;
        bufferAsString << dynamicTupleBuffer;
        NES_DEBUG("QueryExecutionTest: buffer={}", bufferAsString.str());

        //TODO 1 Tuple im result buffer in 312 2 results?
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1UL);

        auto resultLayout =
            Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, nodeEngine->getBufferManager()->getBufferSize());
        auto startFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(0, resultLayout, resultBuffer);
        auto endFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(1, resultLayout, resultBuffer);
        auto keyFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, resultLayout, resultBuffer);
        auto valueFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(3, resultLayout, resultBuffer);

        for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
            // start
            EXPECT_EQ(startFields[recordIndex], 0UL);
            // end
            EXPECT_EQ(endFields[recordIndex], 10UL);
            // key
            EXPECT_EQ(keyFields[recordIndex], 1UL);
            // value
            EXPECT_EQ(valueFields[recordIndex], 10UL);
        }
    }

    ASSERT_TRUE(nodeEngine->stopQuery(0));
    ASSERT_EQ(0U, testSink->getNumberOfResultBuffers());
}

/**
 * @brief This tests creates a windowed query.
 * WindowSource -> windowOperator -> windowScan -> TestSink
 * The source generates 2. buffers.
 */
TEST_F(QueryExecutionTest, tumblingWindowQueryTestWithOutOfOrderBuffer) {

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](SchemaPtr windowSchema,
            OperatorId,
            OriginId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 0,
                                        successors,
                                        /*varyWatermark*/ true,
                                        true,
                                        30);
        });
    auto query = TestQuery::from(windowSourceDescriptor);
    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10));

    query = query.window(windowType).byKey(Attribute("key", BasicType::INT64)).apply(Sum(Attribute("value", BasicType::INT64)));

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", BasicType::UINT64))
                                  ->addField(createField("end", BasicType::UINT64))
                                  ->addField(createField("key", BasicType::INT64))
                                  ->addField("value", BasicType::INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto plan = prepareExecutableQueryPlan(query.getQueryPlan());

    ASSERT_TRUE(nodeEngine->registerQueryInNodeEngine(plan));
    ASSERT_TRUE(nodeEngine->startQuery(plan->getQueryId()));

    // wait till all buffers have been produced
    ASSERT_EQ(testSink->completed.get_future().get(), 1UL);

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);
    {
        auto resultBuffer = testSink->get(0);

        auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, resultBuffer);
        std::stringstream bufferAsString;
        bufferAsString << dynamicTupleBuffer;
        NES_DEBUG("QueryExecutionTest: buffer={}", bufferAsString.str());

        //TODO 1 Tuple im result buffer in 312 2 results?
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1UL);
        auto resultLayout =
            Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, nodeEngine->getBufferManager()->getBufferSize());

        auto startFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(0, resultLayout, resultBuffer);
        auto endFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(1, resultLayout, resultBuffer);
        auto keyFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, resultLayout, resultBuffer);
        auto valueFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(3, resultLayout, resultBuffer);

        for (int recordIndex = 0; recordIndex < 1; recordIndex++) {
            // start
            EXPECT_EQ(startFields[recordIndex], 30UL);
            // end
            EXPECT_EQ(endFields[recordIndex], 40UL);
            // key
            EXPECT_EQ(keyFields[recordIndex], 1UL);
            // value
            EXPECT_EQ(valueFields[recordIndex], 9UL);
        }
    }

    ASSERT_TRUE(nodeEngine->stopQuery(plan->getQueryId()));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourcesize10slide5) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](SchemaPtr windowSchema,
            OperatorId,
            OriginId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 2,
                                        /*frequency*/ 0,
                                        std::move(successors));
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. dd window operator:
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(10), Milliseconds(5));

    auto aggregation = Sum(Attribute("value"));
    query = query.window(windowType).byKey(Attribute("key")).apply(aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", BasicType::UINT64))
                                  ->addField(createField("end", BasicType::UINT64))
                                  ->addField(createField("key", BasicType::INT64))
                                  ->addField("value", BasicType::INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto plan = prepareExecutableQueryPlan(query.getQueryPlan());
    ASSERT_TRUE(nodeEngine->registerQueryInNodeEngine(plan));
    ASSERT_TRUE(nodeEngine->startQuery(0));

    // wait till all buffers have been produced
    ASSERT_EQ(testSink->completed.get_future().get(), 1UL);
    NES_INFO("QueryExecutionTest: The test sink contains {} result buffers.", testSink->getNumberOfResultBuffers());

    // get result buffer
    if (auto resultBuffer = testSink->get(0); !!resultBuffer) {

        NES_INFO("QueryExecutionTest: The result buffer contains {} tuples.", resultBuffer.getNumberOfTuples());
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2UL);
        auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, resultBuffer);
        std::stringstream dynamicTupleBufferAsString;
        dynamicTupleBufferAsString << dynamicTupleBuffer;
        NES_INFO("QueryExecutionTest: buffer={}", dynamicTupleBufferAsString.str());
        std::string expectedContent = "+----------------------------------------------------+\n"
                                      "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                      "+----------------------------------------------------+\n"
                                      "|0|10|1|10|\n"
                                      "|5|15|1|10|\n"
                                      "+----------------------------------------------------+";
        EXPECT_EQ(expectedContent, dynamicTupleBuffer.toString(windowResultSchema));
    }

    ASSERT_TRUE(nodeEngine->stopQuery(0));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourceSize15Slide5) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](SchemaPtr windowSchema,
            OperatorId,
            OriginId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return WindowSource::create(windowSchema,
                                        nodeEngine->getBufferManager(),
                                        nodeEngine->getQueryManager(),
                                        /*bufferCnt*/ 3,
                                        /*frequency*/ 0,
                                        successors);
        });
    auto query = TestQuery::from(windowSourceDescriptor);

    // 2. dd window operator:
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(15), Milliseconds(5));

    auto aggregation = Sum(Attribute("value"));
    query = query.window(windowType).byKey(Attribute("key")).apply(aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", BasicType::UINT64))
                                  ->addField(createField("end", BasicType::UINT64))
                                  ->addField(createField("key", BasicType::INT64))
                                  ->addField("value", BasicType::INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 2, windowResultSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto plan = prepareExecutableQueryPlan(query.getQueryPlan());
    ASSERT_TRUE(nodeEngine->registerQueryInNodeEngine(plan));
    ASSERT_TRUE(nodeEngine->startQuery(0));

    // wait till all buffers have been produced
    ASSERT_EQ(testSink->completed.get_future().get(), 2UL);
    NES_INFO("QueryExecutionTest: The test sink contains {} result buffers.", testSink->getNumberOfResultBuffers());
    // get result buffer

    {
        auto resultBuffer = testSink->get(0);
        auto rowLayoutActual = Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBufferActual = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayoutActual, resultBuffer);
        NES_INFO("QueryExecutionTest: The result buffer contains {} tuples.", resultBuffer.getNumberOfTuples());
        std::stringstream dynamicTupleBufferAsString;
        dynamicTupleBufferAsString << dynamicTupleBufferActual;
        NES_INFO("QueryExecutionTest: buffer={}", dynamicTupleBufferAsString.str());
        std::string expectedContent = "+----------------------------------------------------+\n"
                                      "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                      "+----------------------------------------------------+\n"
                                      "|0|15|1|10|\n"
                                      "+----------------------------------------------------+";
        EXPECT_EQ(expectedContent, dynamicTupleBufferActual.toString(windowResultSchema));

        auto resultBuffer2 = testSink->get(1);
        auto rowLayoutActual2 = Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBufferActual2 = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayoutActual2, resultBuffer2);
        NES_INFO("QueryExecutionTest: The result buffer contains {} tuples.", resultBuffer2.getNumberOfTuples());
        std::stringstream dynamicTupleBufferString;
        dynamicTupleBufferString << dynamicTupleBufferActual2;
        NES_INFO("QueryExecutionTest: buffer={}", dynamicTupleBufferString.str());
        std::string expectedContent2 = "+----------------------------------------------------+\n"
                                       "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                       "+----------------------------------------------------+\n"
                                       "|5|20|1|20|\n"
                                       "|10|25|1|10|\n"
                                       "+----------------------------------------------------+";
        EXPECT_EQ(expectedContent2, dynamicTupleBufferActual2.toString(windowResultSchema));
    }

    ASSERT_TRUE(nodeEngine->stopQuery(0));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

/*
 * In this test we use a slide size that is equivalent to the slice size, to test if slicing works proper for small slides as well
 */
TEST_F(QueryExecutionTest, SlidingWindowQueryWindowSourcesize4slide2) {
    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        windowSchema,
        [&](SchemaPtr windowSchema,
            OperatorId,
            OriginId,
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
    // 2.1 add Sliding window of size 10ms and with Slide 2ms and a sum aggregation on the value.
    auto windowType = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    auto aggregation = Sum(Attribute("value"));
    query = query.window(windowType).byKey(Attribute("key")).apply(aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    //    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto windowResultSchema = Schema::create()
                                  ->addField(createField("start", BasicType::UINT64))
                                  ->addField(createField("end", BasicType::UINT64))
                                  ->addField(createField("key", BasicType::INT64))
                                  ->addField("value", BasicType::INT64);

    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    query.sink(testSinkDescriptor);

    auto plan = prepareExecutableQueryPlan(query.getQueryPlan());
    ASSERT_TRUE(nodeEngine->registerQueryInNodeEngine(plan));
    ASSERT_TRUE(nodeEngine->startQuery(0));

    // wait till all buffers have been produced
    ASSERT_EQ(testSink->completed.get_future().get(), 1UL);
    NES_INFO("QueryExecutionTest: The test sink contains {} result buffers.", testSink->getNumberOfResultBuffers());
    // get result buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1UL);
    {
        auto resultBuffer = testSink->get(0);

        NES_INFO("QueryExecutionTest: The result buffer contains {} tuples.", resultBuffer.getNumberOfTuples());
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2UL);
        auto rowLayoutActual = Runtime::MemoryLayouts::RowLayout::create(windowResultSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBufferActual = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayoutActual, resultBuffer);
        std::stringstream dynamicTupleBufferAsString;
        dynamicTupleBufferAsString << dynamicTupleBufferActual;
        NES_INFO("QueryExecutionTest: buffer={}", dynamicTupleBufferAsString.str());
        std::string expectedContent = "+----------------------------------------------------+\n"
                                      "|start:UINT64|end:UINT64|key:INT64|value:INT64|\n"
                                      "+----------------------------------------------------+\n"
                                      "|2|6|1|10|\n"
                                      "|4|8|1|10|\n"
                                      "+----------------------------------------------------+";
        EXPECT_EQ(expectedContent, dynamicTupleBufferActual.toString(windowResultSchema));
    }

    ASSERT_TRUE(nodeEngine->stopQuery(0));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
// So, merge is a blocking window_scan with two children.
TEST_F(QueryExecutionTest, DISABLED_mergeQuery) {
    // this test is disabled but cannot be enabled safely now
    // I ll make it fail just as a reminder
    ASSERT_TRUE(false);
    // created buffer per source * number of sources
    uint64_t expectedBuf = 20;

    //auto testSource1 = createNonRunnableSource(testSchema, nodeEngine->getBufferManager(),
    //                                                                 nodeEngine->getQueryManager(), 1, 12);

    auto query1 = TestQuery::from(testSchema);

    query1 = query1.filter(Attribute("id") < 5);

    // creating P2
    // auto testSource2 = createNonRunnableSource(testSchema, nodeEngine->getBufferManager(),
    //                                                                 nodeEngine->getQueryManager(), 1, 12);
    auto query2 = TestQuery::from(testSchema).filter(Attribute("id") <= 5);

    // creating P3
    // merge does not change schema
    SchemaPtr ptr = testSchema->copy();
    auto mergedQuery = query2.unionWith(query1).sink(DummySink::create());

    auto testSink = std::make_shared<TestSink>(expectedBuf, testSchema, nodeEngine);

    auto plan = prepareExecutableQueryPlan(mergedQuery.getQueryPlan());
    // auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    // auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    // nodeEngine->getQueryManager()->registerQuery(plan);

    // The plan should have three pipeline
    // EXPECT_EQ(plan->getNumberOfPipelines(), 3);

    // TODO switch to event time if that is ready to remove sleep
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    fillBuffer(buffer, memoryLayout);
    // TODO do not rely on sleeps
    // ingest test data
    //plan->setup();
    // plan->start(nodeEngine->getStateManager());
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 64};
    //auto stage_0 = plan->getPipeline(0);
    //auto stage_1 = plan->getPipeline(1);
    for (int i = 0; i < 10; i++) {

        //  stage_0->execute(buffer, workerContext);// P1
        //  stage_1->execute(buffer, workerContext);// P2
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
    ASSERT_EQ(testSink->completed.get_future().get(), 1UL);

    auto resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5UL);// how to interpret this?

    auto recordIndexFields = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, memoryLayout, resultBuffer);

    for (auto recordIndex = 0L; recordIndex < 5L; ++recordIndex) {
        EXPECT_EQ(recordIndexFields[recordIndex], recordIndex);
    }

    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

/**
 * @brief This test creates a CASE-WHEN query with three CASE-expressions
 * which return the default value and the second and first when value respectively.
 */
TEST_F(QueryExecutionTest, caseWhenExpressionQuery) {
    // creating query plan

    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchema,
        [&](SchemaPtr testSchema,
            OperatorId id,
            OriginId,
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

    auto outputSchema = Schema::create()
                            ->addField("id", BasicType::INT64)
                            ->addField("one", BasicType::INT64)
                            ->addField("value", BasicType::INT64)
                            ->addField("defaultValue", BasicType::FLOAT64)
                            ->addField("compareValue", BasicType::FLOAT64)
                            ->addField("firstWhenResultValue", BasicType::FLOAT64)
                            ->addField("secondWhenResultValue", BasicType::FLOAT64)
                            ->addField("result_leq_default_float", BasicType::FLOAT64)
                            ->addField("result_eq_second_when_float", BasicType::FLOAT64)
                            ->addField("result_combined_first_when_float", BasicType::FLOAT64);

    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto compareValue = ExpressionItem(5.0);
    auto firstWhenResultValue = ExpressionItem(-1.0);
    auto secondWhenResultValue = ExpressionItem(-2.0);
    auto defaultValue = ExpressionItem(10.0);

    auto query =
        TestQuery::from(testSourceDescriptor)
            .filter(Attribute("id") < 2)
            .map(Attribute("defaultValue") = defaultValue)
            .map(Attribute("compareValue") = compareValue)
            .map(Attribute("firstWhenResultValue") = firstWhenResultValue)
            .map(Attribute("secondWhenResultValue") = secondWhenResultValue)
            //no WHEN condition is true, so default value will be used
            .map(Attribute("result_leq_default_float") =
                     CASE({WHEN(compareValue <= 2, 2.0), WHEN(compareValue <= 4, 4.0)}, defaultValue))
            //second WHEN condition is true, so its value will be used
            .map(Attribute("result_eq_second_when_float") =
                     CASE({WHEN(compareValue == 3, 3.0), WHEN(compareValue == 5.0, secondWhenResultValue)}, defaultValue))
            //first WHEN condition is true, so its value will be used
            .map(Attribute("result_combined_first_when_float") =
                     CASE({WHEN(compareValue == 5.0 && compareValue < 6.0, firstWhenResultValue),
                           WHEN(compareValue == 5.0 && compareValue < 3.0, 3.0)},
                          defaultValue))
            .sink(testSinkDescriptor);

    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1U);
    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 4};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(buffer, memoryLayout);
        plan->getPipelines()[0]->execute(buffer, workerContext);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1U);

        std::string expectedContent =
            "+----------------------------------------------------+\n"
            "|id:INT64|one:INT64|value:INT64|defaultValue:FLOAT64|compareValue:FLOAT64|firstWhenResultValue:FLOAT64"
            "|secondWhenResultValue:FLOAT64|result_leq_default_float:FLOAT64|result_eq_second_when_float:FLOAT64|result_combined_"
            "first_when_float:FLOAT64|\n"
            "+----------------------------------------------------+\n"
            "|0|1|0|10.000000|5.000000|-1.000000|-2.000000|10.000000|-2.000000|-1.000000|\n"
            "|1|1|1|10.000000|5.000000|-1.000000|-2.000000|10.000000|-2.000000|-1.000000|\n"
            "+----------------------------------------------------+";

        auto resultBuffer = testSink->get(0);

        auto rowLayoutActual = Runtime::MemoryLayouts::RowLayout::create(outputSchema, resultBuffer.getBufferSize());
        auto dynamicTupleBufferActual = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayoutActual, resultBuffer);
        std::stringstream bufferAsString;
        bufferAsString << buffer;
        NES_DEBUG("QueryExecutionTest: buffer={}", bufferAsString.str());
        EXPECT_EQ(expectedContent, dynamicTupleBufferActual.toString(outputSchema));
    }
    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));

    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}