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
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

namespace NES::Runtime::Execution::Operators {

class KeyedSlicePreAggregationTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("KeyedSlicePreAggregationTest.log", NES::LogLevel::LOG_DEBUG); }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bm = std::make_shared<BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    void emitWatermark(KeyedSlicePreAggregation& slicePreAggregation,
                       ExecutionContext& ctx,
                       uint64_t wts,
                       uint64_t originId,
                       uint64_t sequenceNumber) {
        auto buffer = bm->getBufferBlocking();
        buffer.setWatermark(wts);
        buffer.setOriginId(originId);
        buffer.setSequenceNumber(sequenceNumber);
        auto rb = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));
        slicePreAggregation.close(ctx, rb);
    }

    void emitRecord(KeyedSlicePreAggregation& slicePreAggregation, ExecutionContext& ctx, Record record) {
        slicePreAggregation.execute(ctx, record);
    }
};

TEST_F(KeyedSlicePreAggregationTest, aggregate) {
    auto readTs = std::make_shared<Expressions::ReadFieldExpression>("ts");
    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto readV1 = std::make_shared<Expressions::ReadFieldExpression>("v1");
    auto integer = DataTypeFactory::createInt64();
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    auto slicePreAggregation =
        KeyedSlicePreAggregation(0 /*handler index*/,
                                 readTs,
                                 {readKey},
                                 {integerType},
                                 {readV1},
                                 {std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType)},
                                 std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    auto sliceStaging = std::make_shared<KeyedSliceStaging>();
    std::vector<OriginId> origins = {0};
    auto handler = std::make_shared<KeyedSlicePreAggregationHandler>(10, 10, origins, sliceStaging);
    auto pipelineContext = MockedPipelineExecutionContext(handler);

    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));
    auto buffer = bm->getBufferBlocking();

    auto rb = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));
    slicePreAggregation.setup(ctx);
    auto stateStore = handler->getThreadLocalSliceStore(wc->getId());

    slicePreAggregation.open(ctx, rb);

    emitRecord(slicePreAggregation, ctx, Record({{"ts", Value<>((uint64_t) 11)}, {"k1", Value<>(11)}, {"v1", Value<>(2)}}));
    emitRecord(slicePreAggregation, ctx, Record({{"ts", Value<>((uint64_t) 12)}, {"k1", Value<>(12)}, {"v1", Value<>(42)}}));
    emitRecord(slicePreAggregation, ctx, Record({{"ts", Value<>((uint64_t) 12)}, {"k1", Value<>(11)}, {"v1", Value<>(3)}}));
    ASSERT_EQ(stateStore->getNumberOfSlices(), 1);
    ASSERT_EQ(stateStore->getFirstSlice()->getStart(), 10);
    ASSERT_EQ(stateStore->getFirstSlice()->getEnd(), 20);
    auto& hashMap = stateStore->getFirstSlice()->getState();
    ASSERT_EQ(hashMap->getCurrentSize(), 2);
    // heck entries in hash table.
    struct KVPair : public Interface::ChainedHashMap::Entry {
        uint64_t key;
        uint64_t value;
    };

    auto entries = (KVPair*) hashMap->getPage(0);
    ASSERT_EQ(entries[0].key, 11);
    ASSERT_EQ(entries[0].value, 5);
    ASSERT_EQ(entries[1].key, 12);
    ASSERT_EQ(entries[1].value, 42);
}

}// namespace NES::Runtime::Execution::Operators