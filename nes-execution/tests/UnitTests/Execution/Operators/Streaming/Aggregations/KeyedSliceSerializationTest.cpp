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

#include <API/TestSchemas.hpp>
#include <API/TimeUnit.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <iostream>
#include <random>

namespace NES::Runtime::Execution {

auto constexpr DEFAULT_WINDOW_SIZE = 1000;
auto constexpr DEFAULT_OP_HANDLER_IDX = 0;
static const size_t DEFAULT_PAGE_SIZE = 8024;

class KeyedSliceSerializationTest : public Testing::BaseUnitTest {
  public:
    uint64_t startTS;
    uint64_t endTS;
    uint64_t keySize;
    uint64_t valueSize;
    uint64_t numberOfKeys;
    uint64_t pageSize;
    uint64_t windowSize;
    std::shared_ptr<Runtime::BufferManager> bufferManager;
    std::unique_ptr<std::pmr::memory_resource> allocator;
    std::vector<OriginId> origins;
    std::shared_ptr<WorkerContext> workerCtx;
    DefaultPhysicalTypeFactory physicalDataTypeFactory;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("KeyedSliceSerializationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup KeyedSliceSerializationTest test class.");
    }

    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup KeyedSliceSerializationTest test case.");

        startTS = 100;
        endTS = 200;
        keySize = 8;
        valueSize = 16;
        numberOfKeys = 10;
        pageSize = DEFAULT_PAGE_SIZE;
        windowSize = DEFAULT_WINDOW_SIZE;

        bufferManager = std::make_shared<BufferManager>(1024, 5000);
        allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
        workerCtx = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bufferManager, 100);
        origins = {static_cast<OriginId>(DEFAULT_OP_HANDLER_IDX)};
    }

    void emitRecord(const Operators::KeyedSlicePreAggregation& slicePreAggregation,
                    ExecutionContext& ctx,
                    Record record) {
        slicePreAggregation.execute(ctx, record);
    }


    void emitWatermark(const Operators::KeyedSlicePreAggregation& slicePreAggregation,
                       ExecutionContext& context,
                       uint64_t wts,
                       uint64_t originId,
                       uint64_t sequenceNumber) {
        auto buffer = bufferManager->getBufferBlocking();
        buffer.setWatermark(wts);
        buffer.setOriginId(OriginId(originId));
        buffer.setSequenceNumber(sequenceNumber);

        auto recordBuffer = RecordBuffer(Value<MemRef>(
            reinterpret_cast<int8_t*>(std::addressof(buffer)))
        );
        context.setWatermarkTs(wts);
        context.setOrigin(originId);
        slicePreAggregation.close(context, recordBuffer);
    }

    void TearDown() override {
        BaseUnitTest::TearDown();
    }
};

TEST_F(KeyedSliceSerializationTest, SerializeAndDeserialize) {
    auto hashMap = std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfKeys, std::move(allocator));
    auto originalSlice = std::make_unique<Operators::KeyedSlice>(std::move(hashMap), startTS, endTS);
    auto* entry = originalSlice->getState()->insertEntry(123);
    memcpy(reinterpret_cast<uint8_t*>(entry) + sizeof(Nautilus::Interface::ChainedHashMap::Entry), "testdata", 8);

    auto serializedBuffers = originalSlice->serialize(bufferManager);
    ASSERT_FALSE(serializedBuffers.empty());

    auto deserializedSlice = Operators::KeyedSlice::deserialize(serializedBuffers);
    ASSERT_EQ(deserializedSlice->getStart(), startTS);
    ASSERT_EQ(deserializedSlice->getEnd(), endTS);

    auto* restoredEntry = deserializedSlice->getState()->findChain(123);
    ASSERT_NE(restoredEntry, nullptr);
    ASSERT_EQ(memcmp(reinterpret_cast<uint8_t*>(restoredEntry) + sizeof(Nautilus::Interface::ChainedHashMap::Entry), "testdata", 8), 0);
}

TEST_F(KeyedSliceSerializationTest, StateMigration) {
    auto readTs  = std::make_shared<Expressions::ReadFieldExpression>("ts");
    auto readKey = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto readVal = std::make_shared<Expressions::ReadFieldExpression>("v1");
    auto integer = DataTypeFactory::createInt64();
    auto integerType = physicalDataTypeFactory.getPhysicalType(integer);

    Operators::KeyedSlicePreAggregation slicePreAggregation(
        0,
        std::make_unique<Operators::EventTimeFunction>(readTs, Windowing::TimeUnit::Milliseconds()),
        std::vector<Expressions::ExpressionPtr>{ readKey },
        std::vector<PhysicalTypePtr>{ integerType },
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>>{
            std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readVal, "sum")
        },
        std::make_unique<Nautilus::Interface::MurMur3HashFunction>()
    );

    std::vector<OriginId> origins = {INVALID_ORIGIN_ID};
    auto handler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(10, 10, origins);
    auto pipelineCtx = MockedPipelineExecutionContext({handler}, false, bufferManager);
    auto ctx = ExecutionContext(
        Value<MemRef>(reinterpret_cast<int8_t*>(workerCtx.get())),
        Value<MemRef>(reinterpret_cast<int8_t*>(&pipelineCtx))
    );

    auto buffer = bufferManager->getBufferBlocking();
    auto rb = RecordBuffer(Value<MemRef>(reinterpret_cast<int8_t*>(std::addressof(buffer))));
    slicePreAggregation.setup(ctx);

    auto sliceStore = handler->getThreadLocalSliceStore(workerCtx->getId());
    ASSERT_NE(sliceStore, nullptr);

    slicePreAggregation.open(ctx, rb);
    emitRecord(slicePreAggregation, ctx, Record({{"ts", 11_u64}, {"k1", +11_s64}, {"v1", +2_s64}}));
    emitRecord(slicePreAggregation, ctx, Record({{"ts", 12_u64}, {"k1", +12_s64}, {"v1", +42_s64}}));
    emitRecord(slicePreAggregation, ctx, Record({{"ts", 12_u64}, {"k1", +11_s64}, {"v1", +3_s64}}));

    ASSERT_EQ(sliceStore->getNumberOfSlices(), 1UL);
    auto& firstSlice = sliceStore->getFirstSlice();
    ASSERT_EQ(firstSlice->getStart(), 10ULL);
    ASSERT_EQ(firstSlice->getEnd(), 20ULL);

    auto& hashMap = firstSlice->getState();
    ASSERT_EQ(hashMap->getCurrentSize(), 2ULL);

    struct KVPair : public Interface::ChainedHashMap::Entry {
        uint64_t key;
        uint64_t value;
    };

    auto entries = reinterpret_cast<KVPair*>(hashMap->getPage(0));
    bool found11 = false;
    bool found12 = false;
    for (int i = 0; i < 2; i++) {
        if (entries[i].key == 11 && entries[i].value == 5) {
            found11 = true;
        }
        if (entries[i].key == 12 && entries[i].value == 42) {
            found12 = true;
        }
    }
    ASSERT_TRUE(found11);
    ASSERT_TRUE(found12);

    emitWatermark(slicePreAggregation, ctx, 9, 0, 1);
    ASSERT_EQ(sliceStore->getNumberOfSlices(), 1UL);

    auto migratedBuffers = handler->getStateToMigrate(10, 200);
    auto restoredHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(10, 10, origins);
    auto pipelineCtxRestored = MockedPipelineExecutionContext({restoredHandler}, false, bufferManager);
    auto ctxRestored = ExecutionContext(
        Value<MemRef>(reinterpret_cast<int8_t*>(workerCtx.get())),
        Value<MemRef>(reinterpret_cast<int8_t*>(&pipelineCtxRestored))
    );
    restoredHandler->setup(pipelineCtxRestored, 8, 8);
    restoredHandler->restoreState(migratedBuffers);

    auto restoredSliceStore = restoredHandler->getThreadLocalSliceStore(workerCtx->getId());
    ASSERT_NE(restoredSliceStore, nullptr);

    ASSERT_EQ(restoredSliceStore->getNumberOfSlices(), 1UL);
    auto& restoredSlice = restoredSliceStore->getFirstSlice();
    ASSERT_EQ(restoredSlice->getStart(), 10ULL);
    ASSERT_EQ(restoredSlice->getEnd(), 20ULL);

    auto& restoredHashMap = restoredSlice->getState();
    ASSERT_EQ(restoredHashMap->getCurrentSize(), 2ULL);

    auto restoredEntries = reinterpret_cast<KVPair*>(restoredHashMap->getPage(0));
    bool found11r = false;
    bool found12r = false;
    for (int i = 0; i < 2; i++) {
        if (restoredEntries[i].key == 11 && restoredEntries[i].value == 5) {
            found11r = true;
        }
        if (restoredEntries[i].key == 12 && restoredEntries[i].value == 42) {
            found12r = true;
        }
    }
    ASSERT_TRUE(found11r);
    ASSERT_TRUE(found12r);

    auto restoredWatermark = restoredHandler->getCurrentWatermark();
    auto originalWatermark = handler->getCurrentWatermark();
    ASSERT_EQ(restoredWatermark, originalWatermark);
}





TEST_F(KeyedSliceSerializationTest, OperatorHandlerMigration) {  // TODO: improve test by adding into state and watermarkProcessor
    NES_INFO("Start OperatorHandlerMigration.");
    auto originalHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(windowSize, windowSize, origins);
    auto originalPipelineContext = MockedPipelineExecutionContext({originalHandler}, false, bufferManager);
    originalHandler->setup(originalPipelineContext, keySize, valueSize);

    auto migratedOperatorHandler = originalHandler->serializeOperatorHandlerForMigration();

    ASSERT_FALSE(migratedOperatorHandler.empty());
    ASSERT_EQ(migratedOperatorHandler.size(), 4UL);
}

}// namespace NES::Runtime::Execution