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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
class KeyedTimeWindowPipelineTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KeyedTimeWindowPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup KeyedTimeWindowPipelineTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup GlobalTimeWindowPipelineTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down GlobalTimeWindowPipelineTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down GlobalTimeWindowPipelineTest test class." << std::endl; }
};

/**
 * @brief Test running a pipeline containing a threshold window with a sum aggregation
 */
TEST_P(KeyedTimeWindowPipelineTest, windowWithSum) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("k", BasicType::INT64);
    scanSchema->addField("v", BasicType::INT64);
    scanSchema->addField("ts", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("k");
    auto readValue = std::make_shared<Expressions::ReadFieldExpression>("v");
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>("ts");
    auto aggregationResultFieldName = "test$sum";
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<Expressions::ExpressionPtr> keyFields = {readKey};
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readValue, aggregationResultFieldName)};
    std::vector<PhysicalTypePtr> types = {integerType};
    auto slicePreAggregation =
        std::make_shared<Operators::KeyedSlicePreAggregation>(0 /*handler index*/,
                                                              std::make_unique<Operators::EventTimeFunction>(readTsField),
                                                              keyFields,
                                                              types,
                                                              aggregationFunctions,
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    scanOperator->setChild(slicePreAggregation);
    auto preAggPipeline = std::make_shared<PhysicalOperatorPipeline>();
    preAggPipeline->setRootOperator(scanOperator);
    std::vector<std::string> resultKeyFields = {"k1"};
    auto sliceMergingAction = std::make_unique<Operators::KeyedWindowEmitAction>(aggregationFunctions,
                                                                                 "start",
                                                                                 "end",
                                                                                 8,
                                                                                 8,
                                                                                 resultKeyFields,
                                                                                 types,
                                                                                 /*origin id*/ 0);
    auto sliceMerging = std::make_shared<Operators::KeyedSliceMerging>(0 /*handler index*/,
                                                                       aggregationFunctions,
                                                                       std::move(sliceMergingAction),
                                                                       types,
                                                                       8,
                                                                       8);
    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$sum", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    sliceMerging->setChild(emitOperator);
    auto sliceMergingPipeline = std::make_shared<PhysicalOperatorPipeline>();
    sliceMergingPipeline->setRootOperator(sliceMerging);

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    testBuffer[0]["k"].write(+1_s64);
    testBuffer[0]["v"].write(+10_s64);
    testBuffer[0]["ts"].write(+1_s64);
    testBuffer[1]["k"].write(+2_s64);
    testBuffer[1]["v"].write(+20_s64);
    testBuffer[1]["ts"].write(+1_s64);
    testBuffer[2]["k"].write(+3_s64);
    testBuffer[2]["v"].write(+30_s64);
    testBuffer[2]["ts"].write(+2_s64);
    testBuffer[3]["k"].write(+1_s64);
    testBuffer[3]["v"].write(+40_s64);
    testBuffer[3]["ts"].write(+3_s64);
    testBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceData({1, 1, true});
    buffer.setOriginId(0);

    std::vector<OriginId> origins = {0};

    auto preAggExecutablePipeline = provider->create(preAggPipeline, options);
    auto preAggregationHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(10, 10, origins);

    auto sliceMergingExecutablePipeline = provider->create(sliceMergingPipeline, options);
    auto sliceMergingHandler = std::make_shared<Operators::KeyedSliceMergingHandler>();

    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});
    preAggExecutablePipeline->setup(pipeline1Context);

    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    EXPECT_EQ(pipeline1Context.buffers.size(), 1);

    auto pipeline2Context = MockedPipelineExecutionContext({sliceMergingHandler});
    sliceMergingExecutablePipeline->setup(pipeline2Context);
    sliceMergingExecutablePipeline->execute(pipeline1Context.buffers[0], pipeline2Context, *wc);
    EXPECT_EQ(pipeline2Context.buffers.size(), 1);

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(emitMemoryLayout, pipeline2Context.buffers[0]);
    EXPECT_EQ(resulttestBuffer.getNumberOfTuples(), 3);
    EXPECT_EQ(resulttestBuffer[0][aggregationResultFieldName].read<int64_t>(), 50);
    EXPECT_EQ(resulttestBuffer[1][aggregationResultFieldName].read<int64_t>(), 20);
    EXPECT_EQ(resulttestBuffer[2][aggregationResultFieldName].read<int64_t>(), 30);

    preAggExecutablePipeline->stop(pipeline1Context);
    sliceMergingExecutablePipeline->stop(pipeline2Context);

}// namespace NES::Runtime::Execution

TEST_P(KeyedTimeWindowPipelineTest, multiKeyWindowWithSum) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("k1", BasicType::INT64);
    scanSchema->addField("k2", BasicType::INT64);
    scanSchema->addField("v", BasicType::INT64);
    scanSchema->addField("ts", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readKey1 = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto readKey2 = std::make_shared<Expressions::ReadFieldExpression>("k2");
    auto readValue = std::make_shared<Expressions::ReadFieldExpression>("v");
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>("ts");
    auto aggregationResultFieldName = "sum";
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    std::vector<Expressions::ExpressionPtr> keyFields = {readKey1, readKey2};
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readValue, aggregationResultFieldName)};
    PhysicalTypePtr physicalType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<PhysicalTypePtr> keyTypes = {integerType, integerType};
    auto slicePreAggregation =
        std::make_shared<Operators::KeyedSlicePreAggregation>(0 /*handler index*/,
                                                              std::make_unique<Operators::EventTimeFunction>(readTsField),
                                                              keyFields,
                                                              keyTypes,
                                                              aggregationFunctions,
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    scanOperator->setChild(slicePreAggregation);
    auto preAggPipeline = std::make_shared<PhysicalOperatorPipeline>();
    preAggPipeline->setRootOperator(scanOperator);
    std::vector<std::string> resultKeyFields = {"k1", "k2"};
    auto sliceMergingAction = std::make_unique<Operators::KeyedWindowEmitAction>(aggregationFunctions,
                                                                                 "start",
                                                                                 "end",
                                                                                 16,
                                                                                 8,
                                                                                 resultKeyFields,
                                                                                 keyTypes,
                                                                                 /*origin id*/ 0);
    auto sliceMerging = std::make_shared<Operators::KeyedSliceMerging>(0 /*handler index*/,
                                                                       aggregationFunctions,
                                                                       std::move(sliceMergingAction),
                                                                       keyTypes,
                                                                       16,
                                                                       8);
    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("k1", BasicType::INT64);
    emitSchema->addField("k2", BasicType::INT64);
    emitSchema->addField("sum", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    sliceMerging->setChild(emitOperator);
    auto sliceMergingPipeline = std::make_shared<PhysicalOperatorPipeline>();
    sliceMergingPipeline->setRootOperator(sliceMerging);

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    testBuffer[0]["k1"].write(+1_s64);
    testBuffer[0]["k2"].write(+1_s64);
    testBuffer[0]["v"].write(+10_s64);
    testBuffer[0]["ts"].write(+1_s64);
    testBuffer[1]["k1"].write(+1_s64);
    testBuffer[1]["k2"].write(+2_s64);
    testBuffer[1]["v"].write(+20_s64);
    testBuffer[1]["ts"].write(+1_s64);
    testBuffer[2]["k1"].write(+2_s64);
    testBuffer[2]["k2"].write(+2_s64);
    testBuffer[2]["v"].write(+30_s64);
    testBuffer[2]["ts"].write(+2_s64);
    testBuffer[3]["k1"].write(+1_s64);
    testBuffer[3]["k2"].write(+2_s64);
    testBuffer[3]["v"].write(+40_s64);
    testBuffer[3]["ts"].write(+3_s64);
    testBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceData({1, 1, true});
    buffer.setOriginId(0);

    std::vector<OriginId> origins = {0};

    auto preAggExecutablePipeline = provider->create(preAggPipeline, options);
    auto preAggregationHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(10, 10, origins);

    auto sliceMergingExecutablePipeline = provider->create(sliceMergingPipeline, options);
    auto sliceMergingHandler = std::make_shared<Operators::KeyedSliceMergingHandler>();

    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});
    preAggExecutablePipeline->setup(pipeline1Context);

    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    EXPECT_EQ(pipeline1Context.buffers.size(), 1);

    auto pipeline2Context = MockedPipelineExecutionContext({sliceMergingHandler});
    sliceMergingExecutablePipeline->setup(pipeline2Context);
    sliceMergingExecutablePipeline->execute(pipeline1Context.buffers[0], pipeline2Context, *wc);
    EXPECT_EQ(pipeline2Context.buffers.size(), 1);

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(emitMemoryLayout, pipeline2Context.buffers[0]);
    EXPECT_EQ(resulttestBuffer.getNumberOfTuples(), 3);
    EXPECT_EQ(resulttestBuffer[0]["k1"].read<int64_t>(), 1);
    EXPECT_EQ(resulttestBuffer[0]["k2"].read<int64_t>(), 1);
    EXPECT_EQ(resulttestBuffer[0][aggregationResultFieldName].read<int64_t>(), 10);
    EXPECT_EQ(resulttestBuffer[1]["k1"].read<int64_t>(), 1);
    EXPECT_EQ(resulttestBuffer[1]["k2"].read<int64_t>(), 2);
    EXPECT_EQ(resulttestBuffer[1][aggregationResultFieldName].read<int64_t>(), 60);
    EXPECT_EQ(resulttestBuffer[2]["k1"].read<int64_t>(), 2);
    EXPECT_EQ(resulttestBuffer[2]["k2"].read<int64_t>(), 2);
    EXPECT_EQ(resulttestBuffer[2][aggregationResultFieldName].read<int64_t>(), 30);

    preAggExecutablePipeline->stop(pipeline1Context);
    sliceMergingExecutablePipeline->stop(pipeline2Context);

}// namespace NES::Runtime::Execution

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        KeyedTimeWindowPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<KeyedTimeWindowPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution
