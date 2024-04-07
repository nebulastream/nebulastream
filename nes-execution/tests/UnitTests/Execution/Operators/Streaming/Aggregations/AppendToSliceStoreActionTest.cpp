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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Runtime/BufferManager.hpp>
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

class AppendToSliceStoreActionTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<BufferManager> bufferManager;
    std::shared_ptr<WorkerContext> workerContext;
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("AppendToSliceStoreActionTest.log", NES::LogLevel::LOG_DEBUG); }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bufferManager = std::make_shared<BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
    }
    std::shared_ptr<NonKeyedSlice> createNonKeyedSlice(size_t start, size_t end, int64_t value) {
        auto integer = DataTypeFactory::createInt64();
        PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
        auto state = std::make_unique<State>(integerType->size());

        std::memcpy(state->ptr, &value, integerType->size());

        return std::make_shared<NonKeyedSlice>(8, start, end, std::move(state));
    }

    std::shared_ptr<KeyedSlice>
    createKeyedSlice(size_t start, size_t end, const std::vector<std::pair<uint64_t, uint64_t>>& values) {
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

TEST_F(AppendToSliceStoreActionTest, NonKeyedSlice) {
    using namespace ::testing;
    using namespace std::literals;

    auto handler = std::make_shared<AppendToSliceStoreHandler<NonKeyedSlice>>(600, 200);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto context = ExecutionContext(Value<MemRef>(reinterpret_cast<int8_t*>(workerContext.get())),
                                Value<MemRef>(reinterpret_cast<int8_t*>(&pipelineContext)));

    auto action = AppendToSliceStoreAction<NonKeyedSlice>(0);
    auto emitSlice = [&action, &context](auto slice) {
        ExecuteOperatorPtr child = nullptr;
        Value<UInt64> start(slice->getStart());
        Value<UInt64> end(slice->getEnd());
        Value<UInt64> seq(1_u64);
        Value<UInt64> chunk(1_u64);
        Value<Boolean> lastChunk(true);
        Value<MemRef> combinedSlice(reinterpret_cast<int8_t*>(slice.get()));
        action.emitSlice(context, child, start, end, seq, chunk, lastChunk, combinedSlice);
    };

    auto slice = createNonKeyedSlice(0, 200, 42);
    auto slice1 = createNonKeyedSlice(200, 400, 42);
    auto slice2 = createNonKeyedSlice(400, 600, 42);
    auto slice3 = createNonKeyedSlice(600, 800, 42);
    emitSlice(slice);
    ASSERT_TRUE(pipelineContext.buffers.empty());

    emitSlice(slice1);
    ASSERT_TRUE(pipelineContext.buffers.empty());

    emitSlice(slice2);

    {
        ASSERT_EQ(pipelineContext.buffers.size(), 1);
        auto buffer = pipelineContext.buffers[0];
        auto task = reinterpret_cast<SliceMergeTask<NonKeyedSlice>*>(buffer.getBuffer());

        ASSERT_EQ(task->sequenceNumber, TupleBuffer::INITIAL_SEQUENCE_NUMBER);
        ASSERT_EQ(task->chunkNumber, TupleBuffer::INITIAL_CHUNK_NUMBER);
        ASSERT_EQ(task->lastChunk, true);
        ASSERT_EQ(task->slices.size(), 3);
    }
    emitSlice(slice3);
    {
        ASSERT_THAT(pipelineContext.buffers.size(), 2);
        auto buffer = pipelineContext.buffers[1];
        auto task = reinterpret_cast<SliceMergeTask<NonKeyedSlice>*>(buffer.getBuffer());
        ASSERT_EQ(task->sequenceNumber, TupleBuffer::INITIAL_SEQUENCE_NUMBER + 1);
        ASSERT_EQ(task->chunkNumber, TupleBuffer::INITIAL_CHUNK_NUMBER);
        ASSERT_EQ(task->lastChunk, true);
        ASSERT_EQ(task->slices.size(), 3);
    }
}

TEST_F(AppendToSliceStoreActionTest, KeyedSlice) {
    using namespace ::testing;
    using namespace std::literals;
    auto handler = std::make_shared<AppendToSliceStoreHandler<KeyedSlice>>(600, 200);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(reinterpret_cast<int8_t*>(workerContext.get())),
                                Value<MemRef>(reinterpret_cast<int8_t*>(&pipelineContext)));

    auto action = AppendToSliceStoreAction<KeyedSlice>(0);

    auto emitSlice = [&action, &ctx](auto slice) {
        ExecuteOperatorPtr child = nullptr;
        Value<UInt64> start(slice->getStart());
        Value<UInt64> end(slice->getEnd());
        Value<UInt64> seq(1_u64);
        Value<UInt64> chunk(1_u64);
        Value<Boolean> lastChunk(true);
        Value<MemRef> combinedSlice(reinterpret_cast<int8_t*>(slice.get()));
        action.emitSlice(ctx, child, start, end, seq, chunk, lastChunk, combinedSlice);
    };

    auto slice = createKeyedSlice(0, 200, {{1, 12}, {2, 43}, {3, 22}});
    auto slice1 = createKeyedSlice(200, 400, {{1, 12}, {2, 43}, {3, 22}});
    auto slice2 = createKeyedSlice(400, 600, {{1, 12}, {2, 43}, {3, 22}});
    auto slice3 = createKeyedSlice(600, 800, {{1, 12}, {2, 43}, {3, 22}});
    emitSlice(slice);
    ASSERT_TRUE(pipelineContext.buffers.empty());

    emitSlice(slice1);
    ASSERT_TRUE(pipelineContext.buffers.empty());

    emitSlice(slice2);

    {
        ASSERT_EQ(pipelineContext.buffers.size(), 1);
        auto buffer = pipelineContext.buffers[0];
        auto task = reinterpret_cast<SliceMergeTask<KeyedSlice>*>(buffer.getBuffer());

        ASSERT_EQ(task->sequenceNumber, 1);
        ASSERT_EQ(task->chunkNumber, 1);
        ASSERT_EQ(task->lastChunk, true);
        ASSERT_EQ(task->slices.size(), 3);
    }

    emitSlice(slice3);

    {
        ASSERT_THAT(pipelineContext.buffers.size(), 2);
        auto buffer = pipelineContext.buffers[1];
        auto task = reinterpret_cast<SliceMergeTask<KeyedSlice>*>(buffer.getBuffer());
        ASSERT_EQ(task->sequenceNumber, 2);
        ASSERT_EQ(task->chunkNumber, 1);
        ASSERT_EQ(task->lastChunk, true);
        ASSERT_EQ(task->slices.size(), 3);
    }
}

}// namespace NES::Runtime::Execution::Operators