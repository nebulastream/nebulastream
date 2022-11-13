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
// clang-format: off
#include "gtest/gtest.h"
// clang-format: on
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Network/NetworkChannel.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
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
#include <Runtime/WorkerContext.hpp>
#include <Services/QueryParsingService.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/DummySink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestSink.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <iostream>
#include <utility>
#ifdef PYTHON_UDF_ENABLED
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalPythonUdfOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PythonUdfExecutablePipelineStage.hpp>
#endif

using namespace NES;
using Runtime::TupleBuffer;

class FilterQueryExecutionTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup QueryCatalogServiceTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
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
        NES_DEBUG("QueryExecutionTest: Tear down QueryExecutionTest test case.");
        ASSERT_TRUE(nodeEngine->stop());
        Testing::TestWithErrorHandling<testing::Test>::TearDown();
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

TEST_F(FilterQueryExecutionTest, filterQuery) {

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

    auto outputSchema = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);

    // now, test the query for all possible combinations
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    // two filter operators to validate correct behaviour of (multiple) branchless predicated filters
    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 6).sink(testSinkDescriptor);
    auto plan = prepareExecutableQueryPlan(query.getQueryPlan());
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 1u);
    ASSERT_TRUE(nodeEngine->getQueryManager()->registerQuery(plan));
    ASSERT_TRUE(nodeEngine->getQueryManager()->startQuery(plan));
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    //ASSERT_EQ(Runtime::WorkerContext::getBufferProvider(), nullptr);
    Runtime::WorkerContext workerContext(1, nodeEngine->getBufferManager(), 4);
    ASSERT_NE(Runtime::WorkerContext::getBufferProviderTLS(), nullptr);
    if (auto inputBuffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!inputBuffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillBuffer(inputBuffer, memoryLayout);
        nodeEngine->getQueryManager()->addWorkForNextPipeline(inputBuffer, plan->getPipelines()[0]);
        auto res = nodeEngine->getQueryManager()->stopQuery(plan, Runtime::QueryTerminationType::Graceful);

        //ASSERT_EQ(plan->getPipelines()[0]->execute(inputBuffer, workerContext), ExecutionResult::Ok);
        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 6u);

        auto resultRecordIndexField =
            Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, memoryLayout, resultBuffer);
        auto resultRecordOneField = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(1, memoryLayout, resultBuffer);

        for (uint32_t recordIndex = 0u; recordIndex < 4u; ++recordIndex) {
            // id
            EXPECT_EQ(resultRecordIndexField[recordIndex], recordIndex);
            // one
            EXPECT_EQ(resultRecordOneField[recordIndex], 1LL);
        }

    } else {
        FAIL();
    }

    ASSERT_TRUE(nodeEngine->getQueryManager()->stopQuery(plan));

    // wont be called by runtime as no runtime support in this test
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}
