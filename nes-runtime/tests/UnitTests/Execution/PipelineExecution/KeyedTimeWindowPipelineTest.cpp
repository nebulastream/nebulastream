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
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
class KeyedTimeWindowPipelineTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

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
    DataTypePtr integerType = DataTypeFactory::createInt64();
    std::vector<Expressions::ExpressionPtr> keyFields = {readKey};
    std::vector<Expressions::ExpressionPtr> aggregationFields = {readValue};
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType)};
    PhysicalTypePtr physicalType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<PhysicalTypePtr> types = {physicalType};
    auto slicePreAggregation =
        std::make_shared<Operators::KeyedSlicePreAggregation>(0 /*handler index*/,
                                                              readTsField,
                                                              keyFields,
                                                              types,
                                                              aggregationFields,
                                                              aggregationFunctions,
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    scanOperator->setChild(slicePreAggregation);
    auto preAggPipeline = std::make_shared<PhysicalOperatorPipeline>();
    preAggPipeline->setRootOperator(scanOperator);
    std::vector<std::string> aggregationResultExpressions = {aggregationResultFieldName};
    std::vector<std::string> resultKeyFields = {"k1"};
    auto sliceMerging = std::make_shared<Operators::KeyedSliceMerging>(0 /*handler index*/,
                                                                       aggregationFunctions,
                                                                       aggregationResultExpressions,
                                                                       types,
                                                                       resultKeyFields,
                                                                       "start",
                                                                       "end");
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
    dynamicBuffer[0]["k"].write((int64_t) 1);
    dynamicBuffer[0]["v"].write((int64_t) 10);
    dynamicBuffer[0]["ts"].write((int64_t) 1);
    dynamicBuffer[1]["k"].write((int64_t) 2);
    dynamicBuffer[1]["v"].write((int64_t) 20);
    dynamicBuffer[1]["ts"].write((int64_t) 1);
    dynamicBuffer[2]["k"].write((int64_t) 3);
    dynamicBuffer[2]["v"].write((int64_t) 30);
    dynamicBuffer[2]["ts"].write((int64_t) 2);
    dynamicBuffer[3]["k"].write((int64_t) 1);
    dynamicBuffer[3]["v"].write((int64_t) 40);
    dynamicBuffer[3]["ts"].write((int64_t) 3);
    dynamicBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    std::vector<OriginId> origins = {0};

    auto preAggExecutablePipeline = provider->create(preAggPipeline);
    auto sliceStaging = std::make_shared<Operators::KeyedSliceStaging>();
    auto preAggregationHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(10, 10, origins, sliceStaging);

    auto sliceMergingExecutablePipeline = provider->create(sliceMergingPipeline);
    auto sliceMergingHandler = std::make_shared<Operators::KeyedSliceMergingHandler>(sliceStaging);

    auto pipeline1Context = MockedPipelineExecutionContext({preAggregationHandler});
    preAggExecutablePipeline->setup(pipeline1Context);

    preAggExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    EXPECT_EQ(pipeline1Context.buffers.size(), 1);

    auto pipeline2Context = MockedPipelineExecutionContext({sliceMergingHandler});
    sliceMergingExecutablePipeline->setup(pipeline2Context);
    sliceMergingExecutablePipeline->execute(pipeline1Context.buffers[0], pipeline2Context, *wc);
    EXPECT_EQ(pipeline2Context.buffers.size(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, pipeline2Context.buffers[0]);
    EXPECT_EQ(resultDynamicBuffer.getNumberOfTuples(), 3);
    EXPECT_EQ(resultDynamicBuffer[0][aggregationResultFieldName].read<int64_t>(), 50);
    EXPECT_EQ(resultDynamicBuffer[1][aggregationResultFieldName].read<int64_t>(), 20);
    EXPECT_EQ(resultDynamicBuffer[2][aggregationResultFieldName].read<int64_t>(), 30);

    preAggExecutablePipeline->stop(pipeline1Context);
    sliceMergingExecutablePipeline->stop(pipeline2Context);

}// namespace NES::Runtime::Execution

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        KeyedTimeWindowPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<KeyedTimeWindowPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution
