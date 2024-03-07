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
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

namespace NES::Runtime::Execution::Operators {

class KeyedSliceMergingTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<BufferManager> bufferManager;
    std::shared_ptr<WorkerContext> workerContext;
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("KeyedSliceMergingTest.log", NES::LogLevel::LOG_DEBUG); }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bufferManager = std::make_shared<BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
    }

    std::shared_ptr<KeyedSlice> createSlice(size_t start, size_t end, const std::vector<std::pair<uint64_t, uint64_t>>& values) {
        auto integer = DataTypeFactory::createInt64();
        PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

        auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
        auto map = std::make_unique<Interface::ChainedHashMap>(8, 8, 1000, std::move(allocator));
        Interface::ChainedHashMapRef ref(Value<MemRef>(reinterpret_cast<int8_t*>(map.get())),
                                         {integerType},
                                         integerType->size(),
                                         8);

        for (uint64_t hash = 0; const auto& [k, v] : values) {
            std::memcpy(ref.insert(Value<UInt64>(hash), {k}).getValuePtr().getValue().value, std::addressof(v), sizeof(v));
            hash++;
        }
        return std::make_shared<KeyedSlice>(std::move(map), start, end);
    }
};

class ChainedHashmapVerifier {
  public:
    void verify(ExecutionContext&,
                ExecuteOperatorPtr&,
                Value<UInt64>&,
                Value<UInt64>&,
                Value<UInt64>&,
                Value<UInt64>&,
                Value<Boolean>&,
                Value<MemRef>& globalSlice) {

        auto integer = DataTypeFactory::createInt64();
        PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
        auto* slice = reinterpret_cast<KeyedSlice*>(globalSlice.getValue().value);

        Interface::ChainedHashMapRef ref(Value<MemRef>(reinterpret_cast<int8_t*>(slice->getState().get())),
                                         {integerType},
                                         integerType->size(),
                                         8);

        for (const auto& entryRef : ref) {
            uint64_t key = 0;
            uint64_t value = 0;
            std::memcpy(&key, entryRef.getKeyPtr().getValue().value, sizeof(key));
            std::memcpy(&value, entryRef.getValuePtr().getValue().value, sizeof(value));

            ASSERT_TRUE(kvs.contains(key));
            ASSERT_EQ(kvs[key], value);
        }
    }

    explicit ChainedHashmapVerifier(std::map<uint64_t, uint64_t> kvs) : kvs(std::move(kvs)) {}

  private:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    std::map<uint64_t, uint64_t> kvs;
};

class MockedSliceMergingAction final : public SliceMergingAction {
  public:
    MOCK_METHOD(void,
                emitSlice,
                (ExecutionContext & ctx,
                 ExecuteOperatorPtr& child,
                 Value<UInt64>& windowStart,
                 Value<UInt64>& windowEnd,
                 Value<UInt64>& sequenceNumber,
                 Value<UInt64>& chunkNumber,
                 Value<Boolean>& lastChunk,
                 Value<MemRef>& globalSlice),
                (const, override));
};

TEST_F(KeyedSliceMergingTest, aggregate) {
    auto readV1 = std::make_shared<Expressions::ReadFieldExpression>("v1");
    auto integer = DataTypeFactory::createInt64();
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());

    using namespace ::testing;
    using namespace std::literals;
    auto sliceMergingAction = std::make_unique<MockedSliceMergingAction>();

    ChainedHashmapVerifier expectedResult({{1, 12}, {2, 18}});
    EXPECT_CALL(*sliceMergingAction,
                emitSlice(_,
                          _,
                          Eq(Value<UInt64>(0_u64)),
                          Eq(Value<UInt64>(400_u64)),
                          Eq(Value<UInt64>(1_u64)),
                          Eq(Value<UInt64>(1_u64)),
                          Eq(Value<Boolean>(true)),
                          _))
        .Times(1)
        .WillOnce(Invoke(&expectedResult, &ChainedHashmapVerifier::verify));

    auto merging =
        KeyedSliceMerging(0 /*handler index*/,
                          {std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readV1, "sum")},
                          std::move(sliceMergingAction),
                          std::vector{integerType},
                          8,
                          8);

    auto handler = std::make_shared<KeyedSliceMergingHandler>();
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(reinterpret_cast<int8_t*>(workerContext.get())),
                                Value<MemRef>(reinterpret_cast<int8_t*>(&pipelineContext)));

    auto sliceMergeTaskBuffer = bufferManager->getUnpooledBuffer(sizeof(SliceMergeTask<KeyedSlice>));
    ASSERT_TRUE(sliceMergeTaskBuffer.has_value());
    ASSERT_EQ(reinterpret_cast<uintptr_t>(sliceMergeTaskBuffer->getBuffer()) % alignof(SliceMergeTask<KeyedSlice>), 0);
    new (sliceMergeTaskBuffer->getBuffer()) SliceMergeTask<KeyedSlice>(1,
                                                            1,
                                                            true,
                                                            0,
                                                            400,
                                                            {
                                                                createSlice(0, 100, {{1, 3}, {2, 3}}),
                                                                createSlice(100, 200, {{1, 3}, {2, 6}}),
                                                                createSlice(200, 300, {{1, 3}, {2, 3}}),
                                                                createSlice(300, 400, {{1, 3}, {2, 6}}),
                                                            });

    auto recordBuffer = RecordBuffer(Value<MemRef>(reinterpret_cast<int8_t*>(std::addressof(sliceMergeTaskBuffer))));

    merging.setup(ctx);
    merging.open(ctx, recordBuffer);
    merging.close(ctx, recordBuffer);
}

}// namespace NES::Runtime::Execution::Operators