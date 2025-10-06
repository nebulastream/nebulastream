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

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <Aggregation/SerializableAggregationOperatorHandler.hpp>
#include <Aggregation/SerializableAggregationSlice.hpp>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include <Nautilus/State/Reflection/StateDefinitions.hpp>
#include <Nautilus/State/Serialization/StateSerializer.hpp>
#include <SliceStore/BasicSliceAndWindowStore.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>

using namespace NES;
using namespace NES::DataStructures;
using namespace NES::State;

class SerializableAggregationTest : public ::testing::Test {
protected:
    void SetUp() override {
        bufferManager = BufferManager::create(1024 * 1024, 10);
        
        // Create basic aggregation state config
        config.keySize = sizeof(int64_t);
        config.valueSize = sizeof(int64_t);
        config.pageSize = 4096;
        config.numberOfBuckets = 16;
        
        // Create a simple slice store
        auto sliceStore = std::make_unique<BasicSliceAndWindowStore>();
        
        // Create the serializable aggregation operator handler
        std::vector<OriginId> inputOrigins = {OriginId::create()};
        auto outputOrigin = OriginId::create();
        
        operatorHandler = std::make_shared<SerializableAggregationOperatorHandler>(
            inputOrigins, 
            outputOrigin, 
            std::move(sliceStore));
    }
    
    void TearDown() override {
        operatorHandler.reset();
        bufferManager.reset();
    }
    
    std::shared_ptr<BufferManager> bufferManager;
    std::shared_ptr<SerializableAggregationOperatorHandler> operatorHandler;
    AggregationState::Config config;
};

TEST_F(SerializableAggregationTest, BasicSerializableAggregationSliceCreation) {
    // Create a serializable aggregation slice
    SliceStart sliceStart = 100;
    SliceEnd sliceEnd = 200;
    
    CreateNewHashMapSliceArgs args({}, config.keySize, config.valueSize, config.pageSize, config.numberOfBuckets);
    
    auto slice = std::make_shared<SerializableAggregationSlice>(
        sliceStart, sliceEnd, args, 2, config);
    
    ASSERT_NE(slice, nullptr);
    EXPECT_EQ(slice->getSliceStart(), sliceStart);
    EXPECT_EQ(slice->getSliceEnd(), sliceEnd);
    EXPECT_EQ(slice->getNumberOfHashMaps(), 2);
    EXPECT_EQ(slice->getNumberOfTuples(), 0);
}

TEST_F(SerializableAggregationTest, HashMapStateAccess) {
    SliceStart sliceStart = 0;
    SliceEnd sliceEnd = 100;
    
    CreateNewHashMapSliceArgs args({}, config.keySize, config.valueSize, config.pageSize, config.numberOfBuckets);
    
    auto slice = std::make_shared<SerializableAggregationSlice>(
        sliceStart, sliceEnd, args, 1, config);
    
    // Get hashmap for worker thread 0
    WorkerThreadId workerId = WorkerThreadId::create(0);
    auto* hashMap = slice->getHashMapPtrOrCreate(workerId);
    
    ASSERT_NE(hashMap, nullptr);
    
    // Verify we can access the state
    const auto& state = slice->getHashMapState(workerId);
    EXPECT_EQ(state.keySize, config.keySize);
    EXPECT_EQ(state.valueSize, config.valueSize);
    EXPECT_EQ(state.bucketCount, config.numberOfBuckets);
    EXPECT_EQ(state.tupleCount, 0);
}

TEST_F(SerializableAggregationTest, SerializableOperatorHandlerCreation) {
    ASSERT_NE(operatorHandler, nullptr);
    
    // Test access to the underlying state
    const auto& state = operatorHandler->getState();
    
    // The state should be properly initialized
    EXPECT_EQ(state.hashMaps.size(), 0); // No hashmaps initially
}

TEST_F(SerializableAggregationTest, BasicSerialization) {
    // Create initial state
    AggregationState initialState;
    initialState.config = config;
    
    // Create operator handler with initial state
    std::vector<OriginId> inputOrigins = {OriginId::create()};
    auto outputOrigin = OriginId::create();
    auto sliceStore = std::make_unique<BasicSliceAndWindowStore>();
    
    auto handler = std::make_shared<SerializableAggregationOperatorHandler>(
        inputOrigins, outputOrigin, std::move(sliceStore), initialState);
    
    // Test serialization
    auto bufferOpt = bufferManager->getUnpooledBuffer(1024);
    ASSERT_TRUE(bufferOpt.has_value());
    
    auto buffer = bufferOpt.value();
    
    // For now, just test that serialization doesn't crash
    // Full serialization testing would require implementing the actual serialization logic
    EXPECT_NO_THROW({
        auto serializedBuffer = handler->serialize(bufferManager.get());
    });
}

TEST_F(SerializableAggregationTest, OffsetHashMapBasicOperations) {
    // Test the underlying OffsetBasedHashMap directly
    OffsetBasedHashMap hashMap(sizeof(int64_t), sizeof(int64_t), 16);
    
    // Test basic insertion
    int64_t key = 42;
    int64_t value = 100;
    
    uint32_t offset = hashMap.insert(std::hash<int64_t>{}(key), &key, &value);
    EXPECT_GT(offset, 0);
    EXPECT_EQ(hashMap.size(), 1);
    
    // Test retrieval
    auto* entry = hashMap.find(std::hash<int64_t>{}(key), &key);
    ASSERT_NE(entry, nullptr);
    
    int64_t* retrievedValue = static_cast<int64_t*>(hashMap.getValue(entry));
    EXPECT_EQ(*retrievedValue, value);
}

}