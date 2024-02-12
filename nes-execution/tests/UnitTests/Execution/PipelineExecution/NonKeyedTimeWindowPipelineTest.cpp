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
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
class NonKeyedTimeWindowPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NonKeyedTimeWindowPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Setup NonKeyedTimeWindowPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }
};

/**
 * @brief Test running a pipeline containing a threshold window with a sum aggregation
 */
TEST_P(NonKeyedTimeWindowPipelineTest, windowWithSum) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    scanSchema->addField("ts", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>("ts");
    auto aggregationResultFieldName = "test$sum";
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName)};
    auto slicePreAggregation =
        std::make_shared<Operators::NonKeyedSlicePreAggregation>(0 /*handler index*/,
                                                                 std::make_unique<Operators::EventTimeFunction>(readTsField),
                                                                 aggregationFunctions);
    scanOperator->setChild(slicePreAggregation);
    auto preAggPipeline = std::make_shared<PhysicalOperatorPipeline>();
    preAggPipeline->setRootOperator(scanOperator);
    auto sliceMergingAction = std::make_unique<Operators::NonKeyedWindowEmitAction>(aggregationFunctions,
                                                                                    "start",
                                                                                    "end",
                                                                                    /*origin id*/ 0);
    auto sliceMerging = std::make_shared<Operators::NonKeyedSliceMerging>(0 /*handler index*/,
                                                                          aggregationFunctions,
                                                                          std::move(sliceMergingAction));
    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$sum", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    sliceMerging->setChild(emitOperator);
    auto sliceMergingPipeline = std::make_shared<PhysicalOperatorPipeline>();
    sliceMergingPipeline->setRootOperator(sliceMerging);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write(+1_s64);
    dynamicBuffer[0]["f2"].write(+10_s64);
    dynamicBuffer[0]["ts"].write(+1_s64);
    dynamicBuffer[1]["f1"].write(+2_s64);
    dynamicBuffer[1]["f2"].write(+20_s64);
    dynamicBuffer[1]["ts"].write(+1_s64);
    dynamicBuffer[2]["f1"].write(+3_s64);
    dynamicBuffer[2]["f2"].write(+30_s64);
    dynamicBuffer[2]["ts"].write(+2_s64);
    dynamicBuffer[3]["f1"].write(+1_s64);
    dynamicBuffer[3]["f2"].write(+40_s64);
    dynamicBuffer[3]["ts"].write(+3_s64);
    dynamicBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto preAggExecutablePipeline = provider->create(preAggPipeline, options);
    std::vector<OriginId> origins = {0};
    auto preAggregationHandler = std::make_shared<Operators::NonKeyedSlicePreAggregationHandler>(10, 10, origins);

    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});
    preAggExecutablePipeline->setup(pipeline1Context);
    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    auto sliceMergingExecutablePipeline = provider->create(sliceMergingPipeline, options);
    auto sliceMergingHandler = std::make_shared<Operators::NonKeyedSliceMergingHandler>();

    auto pipeline2Context = MockedPipelineExecutionContext({sliceMergingHandler});
    sliceMergingExecutablePipeline->setup(pipeline2Context);
    EXPECT_EQ(pipeline1Context.buffers.size(), 1);
    sliceMergingExecutablePipeline->execute(pipeline1Context.buffers[0], pipeline2Context, *wc);

    EXPECT_EQ(pipeline2Context.buffers.size(), 1);
    preAggExecutablePipeline->stop(pipeline1Context);
    sliceMergingExecutablePipeline->stop(pipeline2Context);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, pipeline2Context.buffers[0]);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 100);

}// namespace NES::Runtime::Execution

/**
 * @brief Test running a pipeline containing a threshold window with a sum aggregation
 */
TEST_P(NonKeyedTimeWindowPipelineTest, windowWithMultiAggregates) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    scanSchema->addField("ts", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>("ts");
    auto aggregationResultFieldName1 = "test$sum";
    auto aggregationResultFieldName2 = "test$avg";
    auto aggregationResultFieldName3 = "test$max";
    auto aggregationResultFieldName4 = "test$min";
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName1),
        std::make_shared<Aggregation::AvgAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName2),
        std::make_shared<Aggregation::MinAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName3),
        std::make_shared<Aggregation::MaxAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName4)};
    auto slicePreAggregation =
        std::make_shared<Operators::NonKeyedSlicePreAggregation>(0 /*handler index*/,
                                                                 std::make_unique<Operators::EventTimeFunction>(readTsField),
                                                                 aggregationFunctions);
    scanOperator->setChild(slicePreAggregation);
    auto preAggPipeline = std::make_shared<PhysicalOperatorPipeline>();
    preAggPipeline->setRootOperator(scanOperator);
    auto sliceMergingAction = std::make_unique<Operators::NonKeyedWindowEmitAction>(aggregationFunctions,
                                                                                    "start",
                                                                                    "end",
                                                                                    /*origin id*/ 0);
    auto sliceMerging = std::make_shared<Operators::NonKeyedSliceMerging>(0 /*handler index*/,
                                                                          aggregationFunctions,
                                                                          std::move(sliceMergingAction));
    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema = emitSchema->addField("test$sum", BasicType::INT64)
                     ->addField("test$avg", BasicType::INT64)
                     ->addField("test$max", BasicType::INT64)
                     ->addField("test$min", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    sliceMerging->setChild(emitOperator);
    auto sliceMergingPipeline = std::make_shared<PhysicalOperatorPipeline>();
    sliceMergingPipeline->setRootOperator(sliceMerging);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write(+1_s64);
    dynamicBuffer[0]["f2"].write(+10_s64);
    dynamicBuffer[0]["ts"].write(+1_s64);
    dynamicBuffer[1]["f1"].write(+2_s64);
    dynamicBuffer[1]["f2"].write(+20_s64);
    dynamicBuffer[1]["ts"].write(+1_s64);
    dynamicBuffer[2]["f1"].write(+3_s64);
    dynamicBuffer[2]["f2"].write(+30_s64);
    dynamicBuffer[2]["ts"].write(+2_s64);
    dynamicBuffer[3]["f1"].write(+1_s64);
    dynamicBuffer[3]["f2"].write(+40_s64);
    dynamicBuffer[3]["ts"].write(+3_s64);
    dynamicBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto preAggExecutablePipeline = provider->create(preAggPipeline, options);
    std::vector<OriginId> origins = {0};
    auto preAggregationHandler = std::make_shared<Operators::NonKeyedSlicePreAggregationHandler>(10, 10, origins);

    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});
    preAggExecutablePipeline->setup(pipeline1Context);

    auto sliceMergingExecutablePipeline = provider->create(sliceMergingPipeline, options);
    auto sliceMergingHandler = std::make_shared<Operators::NonKeyedSliceMergingHandler>();

    auto pipeline2Context = MockedPipelineExecutionContext({sliceMergingHandler});
    sliceMergingExecutablePipeline->setup(pipeline2Context);

    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    EXPECT_EQ(pipeline1Context.buffers.size(), 1);
    sliceMergingExecutablePipeline->execute(pipeline1Context.buffers[0], pipeline2Context, *wc);

    EXPECT_EQ(pipeline2Context.buffers.size(), 1);
    preAggExecutablePipeline->stop(pipeline1Context);
    sliceMergingExecutablePipeline->stop(pipeline2Context);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, pipeline2Context.buffers[0]);
    EXPECT_EQ(resultDynamicBuffer[0][0].read<int64_t>(), 100);
    EXPECT_EQ(resultDynamicBuffer[0][1].read<int64_t>(), 25);
    EXPECT_EQ(resultDynamicBuffer[0][2].read<int64_t>(), 10);
    EXPECT_EQ(resultDynamicBuffer[0][3].read<int64_t>(), 40);

}// namespace NES::Runtime::Execution

/**
 * @brief Test running a pipeline containing a threshold window with two min aggregations with different data types
 */
TEST_P(NonKeyedTimeWindowPipelineTest, windowWithMultiAggregatesOnDifferentDataType) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    scanSchema->addField("f3", BasicType::FLOAT32);
    scanSchema->addField("ts", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readF3 = std::make_shared<Expressions::ReadFieldExpression>("f3");
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>("ts");
    auto aggregationResultFieldName1 = "test$min_i64";
    auto aggregationResultFieldName2 = "test$min_f32";
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::MinAggregationFunction>(integerType, integerType, readF2, aggregationResultFieldName1),
        std::make_shared<Aggregation::MinAggregationFunction>(floatType, floatType, readF3, aggregationResultFieldName2)
            };
    auto slicePreAggregation =
        std::make_shared<Operators::NonKeyedSlicePreAggregation>(0 /*handler index*/,
                                                                 std::make_unique<Operators::EventTimeFunction>(readTsField),
                                                                 aggregationFunctions);
    scanOperator->setChild(slicePreAggregation);
    auto preAggPipeline = std::make_shared<PhysicalOperatorPipeline>();
    preAggPipeline->setRootOperator(scanOperator);
    auto sliceMergingAction = std::make_unique<Operators::NonKeyedWindowEmitAction>(aggregationFunctions,
                                                                                    "start",
                                                                                    "end",
                                                                                    /*origin id*/ 0);
    auto sliceMerging = std::make_shared<Operators::NonKeyedSliceMerging>(0 /*handler index*/,
                                                                          aggregationFunctions,
                                                                          std::move(sliceMergingAction));
    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema = emitSchema->addField("test$min_i64", BasicType::INT64)
                     ->addField("test$min_f32", BasicType::FLOAT32);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    sliceMerging->setChild(emitOperator);
    auto sliceMergingPipeline = std::make_shared<PhysicalOperatorPipeline>();
    sliceMergingPipeline->setRootOperator(sliceMerging);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write(+1_s64);
    dynamicBuffer[0]["f2"].write(+10_s64);
    dynamicBuffer[0]["f3"].write((float) 0.5);
    dynamicBuffer[0]["ts"].write(+1_s64);
    dynamicBuffer[1]["f1"].write(+2_s64);
    dynamicBuffer[1]["f2"].write(+20_s64);
    dynamicBuffer[1]["f3"].write((float) 0.5);
    dynamicBuffer[1]["ts"].write(+1_s64);
    dynamicBuffer[2]["f1"].write(+3_s64);
    dynamicBuffer[2]["f2"].write(+30_s64);
    dynamicBuffer[2]["f3"].write((float) 0.5);
    dynamicBuffer[2]["ts"].write(+2_s64);
    dynamicBuffer[3]["f1"].write(+1_s64);
    dynamicBuffer[3]["f2"].write(+40_s64);
    dynamicBuffer[3]["f3"].write((float) 0.5);
    dynamicBuffer[3]["ts"].write(+3_s64);
    dynamicBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto preAggExecutablePipeline = provider->create(preAggPipeline, options);
    std::vector<OriginId> origins = {0};
    auto preAggregationHandler = std::make_shared<Operators::NonKeyedSlicePreAggregationHandler>(10, 10, origins);

    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});
    preAggExecutablePipeline->setup(pipeline1Context);

    auto sliceMergingExecutablePipeline = provider->create(sliceMergingPipeline, options);
    auto sliceMergingHandler = std::make_shared<Operators::NonKeyedSliceMergingHandler>();

    auto pipeline2Context = MockedPipelineExecutionContext({sliceMergingHandler});
    sliceMergingExecutablePipeline->setup(pipeline2Context);

    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    EXPECT_EQ(pipeline1Context.buffers.size(), 1);
    sliceMergingExecutablePipeline->execute(pipeline1Context.buffers[0], pipeline2Context, *wc);

    EXPECT_EQ(pipeline2Context.buffers.size(), 1);
    preAggExecutablePipeline->stop(pipeline1Context);
    sliceMergingExecutablePipeline->stop(pipeline2Context);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, pipeline2Context.buffers[0]);
    EXPECT_EQ(resultDynamicBuffer[0][0].read<int64_t>(), 10);
    EXPECT_EQ(resultDynamicBuffer[0][1].read<float>(), 0.5);
}// namespace NES::Runtime::Execution

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        NonKeyedTimeWindowPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<NonKeyedTimeWindowPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution
