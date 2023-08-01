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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

namespace NES::Runtime::Execution::Operators {

class NonKeyedSlicePreAggregationTest : public testing::Test {
  public:
    std::shared_ptr<BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NonKeyedSlicePreAggregationTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup NonKeyedSlicePreAggregationTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup NonKeyedSlicePreAggregationTest test case." << std::endl;
        bm = std::make_shared<BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down NonKeyedSlicePreAggregationTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down NonKeyedSlicePreAggregationTest test class." << std::endl; }

    void emitWatermark(NonKeyedSlicePreAggregation& slicePreAggregation,
                       ExecutionContext& ctx,
                       uint64_t wts,
                       uint64_t originId,
                       uint64_t sequenceNumber) {
        auto buffer = bm->getBufferBlocking();
        buffer.setWatermark(wts);
        buffer.setOriginId(originId);
        buffer.setSequenceNumber(sequenceNumber);
        auto rb = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));
        ctx.setWatermarkTs(wts);
        ctx.setOrigin(originId);
        slicePreAggregation.close(ctx, rb);
    }

    void emitRecord(NonKeyedSlicePreAggregation& slicePreAggregation, ExecutionContext& ctx, Record record) {
        slicePreAggregation.execute(ctx, record);
    }
};

TEST_F(NonKeyedSlicePreAggregationTest, performAggregation) {
    auto readTs = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto unsignedIntegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
    auto slicePreAggregation = NonKeyedSlicePreAggregation(
        0 /*handler index*/,
        std::make_unique<EventTimeFunction>(readTs),
        {std::make_shared<Aggregation::CountAggregationFunction>(integerType, unsignedIntegerType, readF2, "count")});

    std::vector<OriginId> origins = {0};
    auto handler = std::make_shared<NonKeyedSlicePreAggregationHandler>(10, 10, origins);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));
    auto buffer = bm->getBufferBlocking();

    auto rb = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));
    slicePreAggregation.setup(ctx);
    auto stateStore = handler->getThreadLocalSliceStore(wc->getId());

    slicePreAggregation.open(ctx, rb);

    emitRecord(slicePreAggregation, ctx, Record({{"f1", 12_u64}, {"f2", +42_s64}}));
    emitRecord(slicePreAggregation, ctx, Record({{"f1", 12_u64}, {"f2", +42_s64}}));
    ASSERT_EQ(stateStore->getNumberOfSlices(), 1);
    ASSERT_EQ(stateStore->getFirstSlice()->getStart(), 10);
    ASSERT_EQ(stateStore->getFirstSlice()->getEnd(), 20);
    auto value = ((uint64_t*) stateStore->getFirstSlice()->getState()->ptr);
    ASSERT_EQ(*value, 2);

    emitRecord(slicePreAggregation, ctx, Record({{"f1", 24_u64}, {"f2", +42_s64}}));
    ASSERT_EQ(stateStore->getNumberOfSlices(), 2);
    ASSERT_EQ(stateStore->getLastSlice()->getStart(), 20);
    ASSERT_EQ(stateStore->getLastSlice()->getEnd(), 30);
    value = ((uint64_t*) stateStore->getLastSlice()->getState()->ptr);
    ASSERT_EQ(*value, 1);
    emitWatermark(slicePreAggregation, ctx, 22, 0, 1);
    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto smt = (SliceMergeTask<NonKeyedSlice>*) pipelineContext.buffers[0].getBuffer();
    ASSERT_EQ(smt->startSlice, 10);
    ASSERT_EQ(smt->endSlice, 20);
    ASSERT_EQ(smt->sequenceNumber, 1);
    ASSERT_EQ(stateStore->getNumberOfSlices(), 1);
}

TEST_F(NonKeyedSlicePreAggregationTest, performMultipleAggregation) {
    auto readTs = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");

    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr i64 = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr ui64 = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());

    auto slicePreAggregation =
        NonKeyedSlicePreAggregation(0 /*handler index*/,
                                    std::make_unique<EventTimeFunction>(readTs),
                                    {std::make_shared<Aggregation::SumAggregationFunction>(i64, i64, readF2, "sum"),
                                     std::make_shared<Aggregation::CountAggregationFunction>(ui64, ui64, readF2, "count")});

    std::vector<OriginId> origins = {0};
    auto handler = std::make_shared<NonKeyedSlicePreAggregationHandler>(10, 10, origins);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));
    auto buffer = bm->getBufferBlocking();

    auto rb = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));
    slicePreAggregation.setup(ctx);
    auto stateStore = handler->getThreadLocalSliceStore(wc->getId());

    slicePreAggregation.open(ctx, rb);

    emitRecord(slicePreAggregation, ctx, Record({{"f1", 12_u64}, {"f2", +42_s64}}));
    emitRecord(slicePreAggregation, ctx, Record({{"f1", 12_u64}, {"f2", +42_s64}}));
    ASSERT_EQ(stateStore->getNumberOfSlices(), 1);
    ASSERT_EQ(stateStore->getFirstSlice()->getStart(), 10);
    ASSERT_EQ(stateStore->getFirstSlice()->getEnd(), 20);
    struct State {
        uint64_t sum;
        uint64_t count;
    };
    auto value = ((State*) stateStore->getFirstSlice()->getState()->ptr);
    ASSERT_EQ(value[0].sum, 84);
    ASSERT_EQ(value[0].count, 2);

    emitRecord(slicePreAggregation, ctx, Record({{"f1", 24_u64}, {"f2", +42_s64}}));
    ASSERT_EQ(stateStore->getNumberOfSlices(), 2);
    ASSERT_EQ(stateStore->getLastSlice()->getStart(), 20);
    ASSERT_EQ(stateStore->getLastSlice()->getEnd(), 30);
    value = ((State*) stateStore->getLastSlice()->getState()->ptr);
    ASSERT_EQ(value[0].sum, 42);
    ASSERT_EQ(value[0].count, 1);
    emitWatermark(slicePreAggregation, ctx, 22, 0, 1);
    auto smt = (SliceMergeTask<NonKeyedSlice>*) pipelineContext.buffers[0].getBuffer();
    ASSERT_EQ(smt->startSlice, 10);
    ASSERT_EQ(smt->endSlice, 20);
    ASSERT_EQ(smt->sequenceNumber, 1);
    ASSERT_EQ(stateStore->getNumberOfSlices(), 1);
}

}// namespace NES::Runtime::Execution::Operators