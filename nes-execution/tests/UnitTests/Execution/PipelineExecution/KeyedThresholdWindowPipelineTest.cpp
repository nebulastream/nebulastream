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
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/ThresholdWindow/KeyedThresholdWindow/KeyedThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/KeyedThresholdWindow/KeyedThresholdWindowOperatorHandler.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
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
class KeyedThresholdWindowPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    std::vector<Aggregation::AggregationFunctionPtr> aggVector;
    std::vector<std::unique_ptr<Aggregation::AggregationValue>> aggValues;
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KeyedThresholdWindowPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup KeyedThresholdWindowPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup KeyedThresholdWindowPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down KeyedThresholdWindowPipelineTest test class."); }
};

/**
* @brief Test running a pipeline containing a threshold window with a Sum aggregation
*/
TEST_P(KeyedThresholdWindowPipelineTest, thresholdWindowWithSum) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    scanSchema->addField("key", BasicType::UINT32);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("key");

    auto one = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto aggregationResultFieldName = "test$Sum";
    auto keyFieldName = "key";

    DataTypePtr integerType = DataTypeFactory::createInt64();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerPhysicalType,
                                                                        integerPhysicalType,
                                                                        readF2,
                                                                        aggregationResultFieldName);

    aggVector.emplace_back(sumAgg);

    auto thresholdWindowOperator =
        std::make_shared<Operators::KeyedThresholdWindow>(greaterThanExpression,
                                                          0,
                                                          std::vector<Expressions::ExpressionPtr>{readF2},
                                                          readKey,
                                                          keyFieldName,
                                                          std::vector<Record::RecordFieldIdentifier>{aggregationResultFieldName},
                                                          aggVector,
                                                          0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Sum", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    testBuffer[0]["f1"].write(+1_s64);// does not qualify
    testBuffer[0]["key"].write(0_u32);
    testBuffer[0]["f2"].write(+10_s64);
    testBuffer[1]["f1"].write(+2_s64);// qualifies
    testBuffer[1]["key"].write(0_u32);
    testBuffer[1]["f2"].write(+20_s64);
    testBuffer[2]["f1"].write(+3_s64);// qualifies
    testBuffer[2]["key"].write(0_u32);
    testBuffer[2]["f2"].write(+30_s64);

    // the last tuple closes the window
    testBuffer[3]["f1"].write(+1_s64);// does not qualify
    testBuffer[3]["key"].write(0_u32);
    testBuffer[3]["f2"].write(+40_s64);
    testBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline, options);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    auto handler = std::make_shared<Operators::KeyedThresholdWindowOperatorHandler>();

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resulttestBuffer[0][aggregationResultFieldName].read<int64_t>(), 50);
}

/**
* @brief Test running a pipeline containing a threshold window with a sum and a max aggregation on rows having different keys.
*/
TEST_P(KeyedThresholdWindowPipelineTest, thresholdWindowWithSumAndMaxDifferentKeys) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    scanSchema->addField("key", BasicType::UINT32);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("key");

    auto one = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto sumAggregationResultFieldName = "test$Sum";
    auto maxAggregationResultFieldName = "test$Max";
    auto keyFieldName = "key";

    DataTypePtr integerType = DataTypeFactory::createInt64();
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto integerPhysicalType = physicalTypeFactory.getPhysicalType(integerType);

    auto sumAgg = std::make_shared<Aggregation::SumAggregationFunction>(integerPhysicalType,
                                                                        integerPhysicalType,
                                                                        readF2,
                                                                        sumAggregationResultFieldName);

    auto maxAgg = std::make_shared<Aggregation::MaxAggregationFunction>(integerPhysicalType,
                                                                        integerPhysicalType,
                                                                        readF2,
                                                                        maxAggregationResultFieldName);

    aggVector.emplace_back(sumAgg);
    aggVector.emplace_back(maxAgg);

    auto thresholdWindowOperator = std::make_shared<Operators::KeyedThresholdWindow>(
        greaterThanExpression,
        0,
        std::vector<Expressions::ExpressionPtr>{readF2},
        readKey,
        keyFieldName,
        std::vector<Record::RecordFieldIdentifier>{sumAggregationResultFieldName, maxAggregationResultFieldName},
        aggVector,
        0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("test$Sum", BasicType::INT64);
    emitSchema->addField("test$Max", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    testBuffer[0]["f1"].write(+1_s64);// does not qualify
    testBuffer[0]["key"].write(0_u32);
    testBuffer[0]["f2"].write(+10_s64);
    testBuffer[1]["f1"].write(+2_s64);// qualifies
    testBuffer[1]["key"].write(0_u32);
    testBuffer[1]["f2"].write(+20_s64);
    testBuffer[2]["f1"].write(+3_s64);// qualifies
    testBuffer[2]["key"].write(0_u32);
    testBuffer[2]["f2"].write(+30_s64);

    // the last tuple closes the window
    testBuffer[3]["f1"].write(+1_s64);// does not qualify, closes the threshold window for key 0
    testBuffer[3]["key"].write(0_u32);
    testBuffer[3]["f2"].write(+40_s64);

    testBuffer[4]["f1"].write(+1_s64);// does not qualify
    testBuffer[4]["key"].write(1_u32);
    testBuffer[4]["f2"].write(+100_s64);
    testBuffer[5]["f1"].write(+2_s64);// qualifies
    testBuffer[5]["key"].write(1_u32);
    testBuffer[5]["f2"].write(+200_s64);
    testBuffer[6]["f1"].write(+3_s64);// qualifies
    testBuffer[6]["key"].write(1_u32);
    testBuffer[6]["f2"].write(+300_s64);

    // the last tuple closes the window
    testBuffer[7]["f1"].write(+1_s64);// does not qualify, closes the threshold window for key 1
    testBuffer[7]["key"].write(1_u32);
    testBuffer[7]["f2"].write(+400_s64);
    testBuffer.setNumberOfTuples(8);

    auto executablePipeline = provider->create(pipeline, options);

    auto sumAggregationValue = std::make_unique<Aggregation::SumAggregationValue<int64_t>>();
    aggValues.emplace_back(std::move(sumAggregationValue));
    auto handler = std::make_shared<Operators::KeyedThresholdWindowOperatorHandler>();

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2);

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resulttestBuffer[0][sumAggregationResultFieldName].read<int64_t>(), 50);
    EXPECT_EQ(resulttestBuffer[0][maxAggregationResultFieldName].read<int64_t>(), 30);
    EXPECT_EQ(resulttestBuffer[1][sumAggregationResultFieldName].read<int64_t>(), 500);
    EXPECT_EQ(resulttestBuffer[1][maxAggregationResultFieldName].read<int64_t>(), 300);
}

// TODO #3468: parameterize the aggregation function instead of repeating the similar test
INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        KeyedThresholdWindowPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<KeyedThresholdWindowPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution
