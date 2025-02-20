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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
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

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KeyedSliceSerializationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup KeyedSliceSerializationTest test class.");
    }

    /* Will be called before a test is executed. */
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

        origins = {static_cast<OriginId>(DEFAULT_OP_HANDLER_IDX)};
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        BaseUnitTest::TearDown();
    }
};

TEST_F(KeyedSliceSerializationTest, SerializeAndDeserialize) {
    NES_INFO("Start SerializeAndDeserialize.");
    auto hashMap = std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfKeys, std::move(allocator));
    std::unique_ptr<Operators::KeyedSlice> originalSlice = std::make_unique<Operators::KeyedSlice>(std::move(hashMap), startTS, endTS);

    auto* entry = originalSlice->getState()->insertEntry(123);
    memcpy(reinterpret_cast<uint8_t*>(entry) + sizeof(Nautilus::Interface::ChainedHashMap::Entry), "testdata", 8);

    auto serializedBuffers = originalSlice->serialize(bufferManager);
    ASSERT_FALSE(serializedBuffers.empty());

    std::unique_ptr<Operators::KeyedSlice> deserializedSlice = Operators::KeyedSlice::deserialize(serializedBuffers);

    ASSERT_EQ(deserializedSlice->getStart(), startTS);
    ASSERT_EQ(deserializedSlice->getEnd(), endTS);
    auto* restoredEntry = deserializedSlice->getState()->findChain(123);
    ASSERT_NE(restoredEntry, nullptr);
    ASSERT_EQ(memcmp(reinterpret_cast<uint8_t*>(restoredEntry) + sizeof(Nautilus::Interface::ChainedHashMap::Entry), "testdata", 8), 0);
}

TEST_F(KeyedSliceSerializationTest, StateMigration) {  // TODO: improve test by adding entry into state
    NES_INFO("Start StateMigration.");
    auto originalHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(windowSize, windowSize, origins);
    auto originalPipelineContext = MockedPipelineExecutionContext({originalHandler}, false, bufferManager);
    originalHandler->setup(originalPipelineContext, keySize, valueSize);

    auto restorHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(windowSize, windowSize, origins);
    auto restorePipelineContext = MockedPipelineExecutionContext({restorHandler}, false, bufferManager);
    restorHandler->setup(restorePipelineContext, keySize, valueSize);

    WorkerContextPtr wctx = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bufferManager, 100);
    SequenceData seqData = SequenceData(); // TODO is this sufficient for my context?
    originalHandler->trigger(*wctx, originalPipelineContext, OriginId(0), seqData, startTS);

    // std::vector<std::unique_ptr<Operators::KeyedThreadLocalSliceStore>>
    auto threadLocalSliceStores = originalHandler->getThreadLocalSliceStore(wctx->getId());  // Operators::KeyedThreadLocalSliceStore *
    // auto state = &threadLocalSliceStores->getSlices();  // list<unique_ptr<KeyedSlice>>
    auto statelast = &threadLocalSliceStores->getLastSlice();
    statelast->get()->getState()->insertEntry(456);

    // auto hashMap = std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfKeys, std::move(allocator));
    // std::unique_ptr<Operators::KeyedSlice> slice = std::make_unique<Operators::KeyedSlice>(std::move(hashMap), startTS, endTS);
    // auto* entry = slice->getState()->insertEntry(456);
    // memcpy(reinterpret_cast<uint8_t*>(entry) + sizeof(Nautilus::Interface::ChainedHashMap::Entry), "migrate", 7);

    auto migratedState = originalHandler->getStateToMigrate(startTS, endTS);
    restorHandler->restoreState(migratedState);

    auto temp = 0;

    // Validate restored state
    // auto restoredSlices = handler->getSlicesInRange(startTS, endTS);
    // ASSERT_FALSE(restoredSlices.empty());
    // auto* restoredEntry = restoredSlices.front()->getState()->findChain(456);
    // ASSERT_NE(restoredEntry, nullptr);
    // ASSERT_EQ(memcmp(reinterpret_cast<uint8_t*>(restoredEntry) + sizeof(Nautilus::Interface::ChainedHashMap::Entry), "migrate", 7), 0);
}

TEST_F(KeyedSliceSerializationTest, StateMigration2) {
    NES_INFO("Start StateMigration.");
    // Arrange parameters for the test.
    uint64_t windowSize = 10;
    uint64_t keySize = 8;     // Example key size
    uint64_t valueSize = 8;   // Example value size
    uint64_t startTS = 100;
    uint64_t endTS = 200;
    std::vector<OriginId> origins = {OriginId(0)};  // Example origin
    // Assume a function exists to create or retrieve the BufferManager.

    // Create the original keyed slice handler and set it up.
    auto originalHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(windowSize, windowSize, origins);
    auto originalPipelineContext = MockedPipelineExecutionContext({originalHandler}, false, bufferManager);
    originalHandler->setup(originalPipelineContext, keySize, valueSize);

    // Trigger the handler so that it builds its state.
    WorkerContextPtr wctx = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bufferManager, 100);
    SequenceData seqData; // assume default construction is sufficient
    originalHandler->trigger(*wctx, originalPipelineContext, OriginId(0), seqData, startTS);

    // Retrieve the thread-local slice store from the original handler.
    auto* sliceStore = originalHandler->getThreadLocalSliceStore(wctx->getId());
    ASSERT_NE(sliceStore, nullptr) << "Original handler did not return a valid slice store.";

    // Get the last slice and insert an entry (for example, key 456).
    auto& lastSlice = sliceStore->getLastSlice();
    ASSERT_NE(lastSlice, nullptr) << "No last slice found in original handler.";
    lastSlice->getState()->insertEntry(456);

    // Migrate the state from the original handler.
    auto migratedState = originalHandler->getStateToMigrate(startTS, endTS);

    // Optionally, you can inspect ‘migratedState’ here to confirm the buffers.
    // Create a new handler to restore the migrated state.
    auto restoredHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(windowSize, windowSize, origins);
    auto restorePipelineContext = MockedPipelineExecutionContext({restoredHandler}, false, bufferManager);
    restoredHandler->setup(restorePipelineContext, keySize, valueSize);
    restoredHandler->restoreState(migratedState);

    // Retrieve the thread-local slice store from the restored handler.
    auto* restoredSliceStore = restoredHandler->getThreadLocalSliceStore(wctx->getId());
    ASSERT_NE(restoredSliceStore, nullptr) << "Restored handler did not return a valid slice store.";
    auto& restoredLastSlice = restoredSliceStore->getLastSlice();
    ASSERT_NE(restoredLastSlice, nullptr) << "No last slice found in restored handler.";

    // Validate that the restored state contains the entry with key 456.
    auto* restoredEntry = restoredLastSlice->getState()->findChain(456);
    ASSERT_NE(restoredEntry, nullptr) << "Restored state does not contain the expected entry with key 456.";
    // Optionally, if the entry contains a value, verify it:
    // EXPECT_EQ(restoredEntry->getValue(), expectedValue);
}

TEST_F(KeyedSliceSerializationTest, WatermarkMigration) {  // TODO: improve test by adding into watermarkProcessor
    NES_INFO("Start WatermarkMigration.");
    auto originalHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(windowSize, windowSize, origins);
    auto originalPipelineContext = MockedPipelineExecutionContext({originalHandler}, false, bufferManager);
    originalHandler->setup(originalPipelineContext, keySize, valueSize);

    auto restorHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(windowSize, windowSize, origins);
    auto restorePipelineContext = MockedPipelineExecutionContext({restorHandler}, false, bufferManager);
    restorHandler->setup(restorePipelineContext, keySize, valueSize);

    auto migratedWatermark = originalHandler->getWatermarksToMigrate();
    restorHandler->restoreWatermarks(migratedWatermark);

    // validate restored watermark
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