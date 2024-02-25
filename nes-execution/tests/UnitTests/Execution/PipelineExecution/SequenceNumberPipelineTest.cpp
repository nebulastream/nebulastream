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
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Core.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <utility>

namespace NES::Runtime::Execution {

class SequenceNumberPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SequenceNumberPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SequenceNumberPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup SequenceNumberPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SequenceNumberPipelineTest test class."); }
};

/**
 * @brief This method creates four buffers and sets the f1 = 10 for all tuples in the second and fourth buffer
 */
std::vector<TupleBuffer> createDataAllSeqNumbersEmitted(BufferManagerPtr bm, SchemaPtr schema) {
    std::vector<TupleBuffer> retBuffers;
    constexpr uint64_t NUM_BUF = 4;

    for (uint64_t bufCnt = 0; bufCnt < NUM_BUF; ++bufCnt) {
        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);
        for (int64_t i = 0; i < 100; ++i) {
            dynamicBuffer[i]["f1"].write(i % 10_s64);
            dynamicBuffer[i]["f2"].write(+1_s64);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        if (bufCnt == 1 || bufCnt == 3) {
            for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); ++i) {
                dynamicBuffer[i]["f1"].write(+10_s64);
            }
        }
        buffer.setSequenceNumber(bufCnt + 1);
        retBuffers.emplace_back(buffer);
    }

    return retBuffers;
}

/**
 * @brief Selection pipeline that execute a select operator and filters all tuples that are not satisfying (f1 == 5)
 * We send four buffers and create the data so that no tuple satisfies the condition for the second, and fourth buffer.
 * Then we check if the PipelineExecutionContext has seen all sequence numbers, as we have to pass on all sequence numbers
 */
TEST_P(SequenceNumberPipelineTest, testAllSequenceNumbersGetEmitted) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = std::make_shared<Operators::Selection>(equalsExpression);
    scanOperator->setChild(selectionOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    for (auto& buf : createDataAllSeqNumbersEmitted(bm, schema)) {
        executablePipeline->execute(buf, pipelineContext, *wc);
    }
    executablePipeline->stop(pipelineContext);

    // Checking the output
    ASSERT_EQ(pipelineContext.buffers.size(), 4);
    for (const auto& buf : pipelineContext.buffers) {
        auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buf);
        for (uint64_t i = 0; i < resultDynamicBuffer.getNumberOfTuples(); i++) {
            ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int64_t>(), 5);
            ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
        }
    }

    // Checking, if we have seen all sequence numbers
    std::vector<SequenceNumber> seenSeqNumbers;
    std::transform(pipelineContext.seenSeqChunkLastChunk.begin(), pipelineContext.seenSeqChunkLastChunk.end(),
                   std::back_inserter(seenSeqNumbers), [] (const SequenceData& item) { return item.sequenceNumber; });
    ASSERT_THAT(seenSeqNumbers, ::testing::UnorderedElementsAreArray({1, 2, 3, 4}));
}

/**
 * @brief This method creates four buffers and sets the f1 = 10 for all tuples in the second and fourth buffer
 */
std::vector<TupleBuffer> createDataFullWithConstantFieldValues(BufferManagerPtr bm, SchemaPtr schema) {
    std::vector<TupleBuffer> retBuffers;
    constexpr uint64_t NUM_BUF = 4;

    for (uint64_t bufCnt = 0; bufCnt < NUM_BUF; ++bufCnt) {
        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);
        for (auto i = 0_u64; i < dynamicBuffer.getCapacity(); ++i) {
            dynamicBuffer[i]["f1"].write(+10_s64);
            dynamicBuffer[i]["f2"].write(+1_s64);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }
        buffer.setSequenceNumber(bufCnt + 1);
        buffer.setChunkNumber(1);
        buffer.setLastChunk(true);
        retBuffers.emplace_back(buffer);
    }

    return retBuffers;
}

/**
 * @brief Map pipeline that has creates an additional field to have a larger output schema than input. With this, we check
 * our implementation of the chunks and see, if they have been assigned correctly.
 */
TEST_P(SequenceNumberPipelineTest, testMultipleSequenceNumbers) {
    auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                           ->addField("f1", BasicType::INT64)
                           ->addField("f2", BasicType::INT64);
    auto outputSchema = inputSchema->copy()->addField("f3", BasicType::INT64);
    auto memoryLayoutInput = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
    auto memoryLayoutOutput = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutInput);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto addExpression = std::make_shared<Expressions::AddExpression>(readF1, readF2);
    auto writeF3 = std::make_shared<Expressions::WriteFieldExpression>("f3", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(writeF3);
    scanOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutOutput);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    for (auto& buf : createDataFullWithConstantFieldValues(bm, inputSchema)) {
        executablePipeline->execute(buf, pipelineContext, *wc);
    }
    executablePipeline->stop(pipelineContext);

    // Checking the output and the seq number, chunk number and last chunk
    ASSERT_EQ(pipelineContext.buffers.size(), 8);
    std::vector<SequenceData> expectedSeqChunkLastChunk = {
        {1, 1, false}, {1, 2, true}, {2, 1, false}, {2, 2, true},
        {3, 1, false}, {3, 2, true}, {4, 1, false}, {4, 2, true},
    };
    auto expectedSeqChunkLastChunkIt = expectedSeqChunkLastChunk.begin();
    for (const auto& buf : pipelineContext.buffers) {
        auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayoutOutput, buf);
        for (uint64_t i = 0; i < resultDynamicBuffer.getNumberOfTuples(); i++) {
            ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int64_t>(), 10);
            ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
            ASSERT_EQ(resultDynamicBuffer[i]["f3"].read<int64_t>(), 10 + 1);
        }
        SequenceData resultTriplet = {buf.getSequenceNumber(), buf.getChunkNumber(), buf.isLastChunk()};
        ASSERT_EQ(resultTriplet, *expectedSeqChunkLastChunkIt);
        ++expectedSeqChunkLastChunkIt;
    }

    // Checking, if we have seen all sequence numbers
    ASSERT_THAT(pipelineContext.seenSeqChunkLastChunk, ::testing::UnorderedElementsAreArray(expectedSeqChunkLastChunk));
}


std::shared_ptr<PhysicalOperatorPipeline> createFirstPipeline(const MemoryLayouts::RowLayoutPtr& memoryLayoutInput,
                                                              const MemoryLayouts::RowLayoutPtr& memoryLayoutOutput) {
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutInput);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto addExpression = std::make_shared<Expressions::AddExpression>(readF1, readF2);
    auto writeF3 = std::make_shared<Expressions::WriteFieldExpression>("f3", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(writeF3);
    scanOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutOutput);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    return pipeline;
}

std::shared_ptr<PhysicalOperatorPipeline> createSecondPipeline(const MemoryLayouts::RowLayoutPtr& memoryLayoutInput,
                                                               Aggregation::AggregationFunctionPtr aggregationFunction) {
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutInput);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    const auto readTsField = std::make_shared<Expressions::ReadFieldExpression>("ts");
    std::vector<Aggregation::AggregationFunctionPtr> aggregationFunctions = {std::move(aggregationFunction)};
    auto slicePreAggregation =
        std::make_shared<Operators::NonKeyedSlicePreAggregation>(0 /*handler index*/,
                                                                 std::make_unique<Operators::EventTimeFunction>(readTsField),
                                                                 aggregationFunctions);
    scanOperator->setChild(slicePreAggregation);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    return pipeline;
}

std::shared_ptr<PhysicalOperatorPipeline> createThirdPipeline(const MemoryLayouts::RowLayoutPtr& memoryLayoutOutput,
                                                              Aggregation::AggregationFunctionPtr aggregationFunction) {
    std::vector<Aggregation::AggregationFunctionPtr> aggregationFunctions = {std::move(aggregationFunction)};
    auto sliceMergingAction = std::make_unique<Operators::NonKeyedWindowEmitAction>(aggregationFunctions,
                                                                                    "start",
                                                                                    "end",
                                                                                    /*origin id*/ 0);
    auto sliceMerging = std::make_shared<Operators::NonKeyedSliceMerging>(0 /*handler index*/,
                                                                          aggregationFunctions,
                                                                          std::move(sliceMergingAction));

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutOutput);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    sliceMerging->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(sliceMerging);
    return pipeline;
}

/**
 * @brief We have a total of three pipelines here with two operators, map and a window aggregation.
 * 1) Map that has a larger output than input schema.
 * 2) PreAggPipeline that calculates pre-Aggregates for a window
 * 3) SliceMergingPipeline that merges the pre-Aggregates
 *
 * Additionally, we will send the output buffers of the first pipeline in a random fashion.
 * This way, we can test if a window operator waits until it has seen all chunks of a sequence number.
 */
TEST_P(SequenceNumberPipelineTest, testMultipleSequenceNumbersWithAggregation) {
    auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                           ->addField("f1", BasicType::INT64)
                           ->addField("f2", BasicType::INT64)
                           ->addField("ts", BasicType::UINT64);
    auto outputSchema = inputSchema->copy()->addField("f3", BasicType::INT64);
    auto outputSchemaWindow = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                  ->addField("start", BasicType::INT64)
                                  ->addField("end", BasicType::INT64)
                                  ->addField("test$count", BasicType::INT64);

    auto memoryLayoutInput = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
    auto memoryLayoutOutput = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());
    auto memoryLayoutOutputWindow = Runtime::MemoryLayouts::RowLayout::create(outputSchemaWindow, bm->getBufferSize());

    // Creating aggregation function
    const auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    const auto aggregationResultFieldName1 = "test$count";
    PhysicalTypePtr integerType = DefaultPhysicalTypeFactory().getPhysicalType(DataTypeFactory::createInt64());
    Aggregation::AggregationFunctionPtr aggregationFunction =
        std::make_shared<Aggregation::CountAggregationFunction>(integerType, integerType, readF1, aggregationResultFieldName1);

    // Creating executable pipelines
    auto pipeline1 = provider->create(createFirstPipeline(memoryLayoutInput, memoryLayoutOutput), options);
    auto pipeline2 = provider->create(createSecondPipeline(memoryLayoutOutput, aggregationFunction), options);
    auto pipeline3 = provider->create(createThirdPipeline(memoryLayoutOutputWindow, aggregationFunction), options);

    // Creating operator handlers
    constexpr auto windowSize = 10;
    constexpr auto windowSlide = 10;
    std::vector<OriginId> origins = {0};
    auto preAggregationHandler = std::make_shared<Operators::NonKeyedSlicePreAggregationHandler>(windowSize,
                                                                                                 windowSlide,
                                                                                                 origins);
    auto sliceMergingHandler = std::make_shared<Operators::NonKeyedSliceMergingHandler>();

    // Creating pipeline execution contexts
    auto pipeline1Context = MockedPipelineExecutionContext();
    auto pipeline2Context = MockedPipelineExecutionContext({preAggregationHandler}, false);
    auto pipeline3Context = MockedPipelineExecutionContext({sliceMergingHandler});

    // Setting up all pipelines
    pipeline1->setup(pipeline1Context);
    pipeline2->setup(pipeline2Context);
    pipeline3->setup(pipeline3Context);

    // Creating input data and executing the first pipeline
    constexpr auto NUM_BUFFERS = 2;
    auto ts = 0_u64;
    for (auto bufCnt = 0_u64; bufCnt < NUM_BUFFERS; ++bufCnt) {
        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, inputSchema);
        for (auto i = 0_u64; i < dynamicBuffer.getCapacity(); ++i) {
            dynamicBuffer[i]["f1"].write(+10_s64);
            dynamicBuffer[i]["f2"].write(+10_s64);
            dynamicBuffer[i]["ts"].write(ts++);
        }
        buffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        buffer.setSequenceData({bufCnt + 1, 1, true});
        buffer.setOriginId(0);
        buffer.setWatermark(ts - 1);

        pipeline1->execute(buffer, pipeline1Context, *wc);
    }

    // Comparing the expected number of output buffers and shuffling the buffers to mix up the chunk numbers and last chunks
    EXPECT_EQ(pipeline1Context.buffers.size(), NUM_BUFFERS * 2);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(pipeline1Context.buffers.begin(), pipeline1Context.buffers.end(), gen);

    // Executing the second and third pipeline
    for (auto buf : pipeline1Context.buffers) {
        pipeline2->execute(buf, pipeline2Context, *wc);
    }
    for (auto buf : pipeline2Context.buffers) {
        pipeline3->execute(buf, pipeline3Context, *wc);
    }

    // We use ts as we increase the timestamp for each tuple.
    auto expectedNumberOfTuples = (ts) / windowSize;
    auto numberOfTuples = std::accumulate(pipeline3Context.buffers.begin(), pipeline3Context.buffers.end(),
                                          0_u64, [] (const auto sum, const TupleBuffer& buf) {
                                              return sum + buf.getNumberOfTuples();
                                          });
    EXPECT_EQ(numberOfTuples, expectedNumberOfTuples);

    // Comparing expected output
    for (const auto& buf : pipeline3Context.buffers) {
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buf, outputSchemaWindow);
        for (auto i = 0_u64; i < dynamicBuffer.getNumberOfTuples(); ++i) {
            // As we count the number of tuple per window, the count should be the window size
            EXPECT_EQ(windowSize, dynamicBuffer[i][aggregationResultFieldName1].read<int64_t>());
        }
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SequenceNumberPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<SequenceNumberPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution