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

#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

namespace NES::Runtime::Execution::Operators {

class KeyedSlicePreAggregationTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<BufferManager> bufferManager;
    std::shared_ptr<WorkerContext> workerContext;
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("KeyedSlicePreAggregationTest.log", NES::LogLevel::LOG_DEBUG); }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bufferManager = std::make_shared<BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
    }

    void emitWatermark(const KeyedSlicePreAggregation& slicePreAggregation,
                       ExecutionContext& context,
                       uint64_t wts,
                       uint64_t originId,
                       uint64_t sequenceNumber) {
        auto buffer = bufferManager->getBufferBlocking();
        buffer.setWatermark(wts);
        buffer.setOriginId(OriginId(originId));
        buffer.setSequenceNumber(sequenceNumber);
        auto recordBuffer = RecordBuffer(Value<MemRef>(reinterpret_cast<int8_t*>(std::addressof(buffer))));
        context.setWatermarkTs(wts);
        context.setOrigin(originId);
        slicePreAggregation.close(context, recordBuffer);
    }

    void emitRecord(const KeyedSlicePreAggregation& slicePreAggregation, ExecutionContext& ctx, Record record) {
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
                                 std::make_unique<EventTimeFunction>(readTs),
                                 {readKey},
                                 {integerType},
                                 {std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readV1, "sum")},
                                 std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    std::vector<OriginId> origins = {INVALID_ORIGIN_ID};
    auto handler = std::make_shared<KeyedSlicePreAggregationHandler>(10, 10, origins);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(reinterpret_cast<int8_t*>(workerContext.get())), Value<MemRef>((int8_t*) &pipelineContext));
    auto buffer = bufferManager->getBufferBlocking();

    auto rb = RecordBuffer(Value<MemRef>(reinterpret_cast<int8_t*>(std::addressof(buffer))));
    slicePreAggregation.setup(ctx);
    auto stateStore = handler->getThreadLocalSliceStore(workerContext->getId());

    slicePreAggregation.open(ctx, rb);

    emitRecord(slicePreAggregation, ctx, Record({{"ts", 11_u64}, {"k1", +11_s64}, {"v1", +2_s64}}));
    emitRecord(slicePreAggregation, ctx, Record({{"ts", 12_u64}, {"k1", +12_s64}, {"v1", +42_s64}}));
    emitRecord(slicePreAggregation, ctx, Record({{"ts", 12_u64}, {"k1", +11_s64}, {"v1", +3_s64}}));
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

    auto entries = reinterpret_cast<KVPair*>(hashMap->getPage(0));
    ASSERT_EQ(entries[0].key, 11);
    ASSERT_EQ(entries[0].value, 5);
    ASSERT_EQ(entries[1].key, 12);
    ASSERT_EQ(entries[1].value, 42);

    emitWatermark(slicePreAggregation, ctx, 22, 0, 1);
    auto smt = reinterpret_cast<SliceMergeTask<KeyedSlice>*>(pipelineContext.buffers[0].getBuffer());
    ASSERT_EQ(smt->startSlice, 10);
    ASSERT_EQ(smt->endSlice, 20);
    ASSERT_EQ(smt->sequenceNumber, TupleBuffer::INITIAL_SEQUENCE_NUMBER);
    ASSERT_EQ(smt->chunkNumber, TupleBuffer::INITIAL_CHUNK_NUMBER);
    ASSERT_EQ(smt->lastChunk, true);
}

}// namespace NES::Runtime::Execution::Operators