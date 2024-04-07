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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregationHandler.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class BatchAggregationPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BatchAggregationPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BatchAggregationPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup BatchAggregationPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down BatchAggregationPipelineTest test class."); }
};

/**
 * @brief Emit operator that emits a row oriented tuple buffer.
 */
TEST_P(BatchAggregationPipelineTest, aggregationPipeline) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto aggregationResultFieldName = "f1";
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readF1, aggregationResultFieldName)};
    auto aggregationOp = std::make_shared<Operators::BatchAggregation>(0 /*handler index*/, aggregationFunctions);
    scanOperator->setChild(aggregationOp);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);

    // Fill buffer
    testBuffer[0]["f1"].write(+10_s64);
    testBuffer[1]["f1"].write(+20_s64);
    testBuffer[2]["f1"].write(+30_s64);
    testBuffer[3]["f1"].write(+10_s64);
    testBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto preAggExecutablePipeline = provider->create(pipeline, options);
    auto preAggregationHandler = std::make_shared<Operators::BatchAggregationHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});

    auto aggScan = std::make_shared<Operators::BatchAggregationScan>(0 /*handler index*/, aggregationFunctions);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    aggScan->setChild(emitOperator);
    auto pipeline2 = std::make_shared<PhysicalOperatorPipeline>();
    pipeline2->setRootOperator(aggScan);
    auto pipeline2Context = MockedPipelineExecutionContext({preAggregationHandler});
    auto aggExecutablePipeline = provider->create(pipeline2, options);

    preAggExecutablePipeline->setup(pipeline1Context);
    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    aggExecutablePipeline->setup(pipeline2Context);
    aggExecutablePipeline->execute(buffer, pipeline2Context, *wc);
    preAggExecutablePipeline->stop(pipeline1Context);
    aggExecutablePipeline->stop(pipeline2Context);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema = emitSchema->addField("f1", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(emitMemoryLayout, pipeline2Context.buffers[0]);
    EXPECT_EQ(resulttestBuffer[0][aggregationResultFieldName].read<int64_t>(), 70);
}

TEST_P(BatchAggregationPipelineTest, keyedAggregationPipeline) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema = schema->addField("f1", BasicType::INT64)->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    resultSchema->addField("f1", BasicType::INT64);
    auto resultMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto aggregationResultFieldName = "f2";
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();

    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<Expressions::ExpressionPtr> keyFields = {readF1};
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName)};
    std::vector<PhysicalTypePtr> types = {integerType};
    auto aggregationOp =
        std::make_shared<Operators::BatchKeyedAggregation>(0 /*handler index*/,
                                                           keyFields,
                                                           types,
                                                           aggregationFunctions,
                                                           std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    scanOperator->setChild(aggregationOp);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);

    // Fill buffer
    testBuffer[0]["f1"].write(+1_s64);
    testBuffer[0]["f2"].write(+10_s64);
    testBuffer[1]["f1"].write(+1_s64);
    testBuffer[1]["f2"].write(+1_s64);
    testBuffer[2]["f1"].write(+2_s64);
    testBuffer[2]["f2"].write(+2_s64);
    testBuffer[3]["f1"].write(+3_s64);
    testBuffer[3]["f2"].write(+10_s64);
    testBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto preAggExecutablePipeline = provider->create(pipeline, options);
    auto preAggregationHandler = std::make_shared<Operators::BatchKeyedAggregationHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});
    preAggExecutablePipeline->setup(pipeline1Context);
    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    preAggExecutablePipeline->stop(pipeline1Context);
    auto hmSize = preAggregationHandler->getThreadLocalStore(wc->getId())->getCurrentSize();
    ASSERT_EQ(hmSize, 3);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        BatchAggregationPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<BatchAggregationPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution