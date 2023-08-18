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
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus::Interface {

class ChainedHashMapTest : public Testing::BaseUnitTest {
  public:
    std::unique_ptr<HashFunction> hf;
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("HashTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup HashTest test class.");
    }
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        hf = std::make_unique<MurMur3HashFunction>();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down HashTest test class."); }
};

TEST_F(ChainedHashMapTest, insertEntryTableTest) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto hashMap = ChainedHashMap(8, 8, 1000, std::move(allocator));
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto keyDataTypes = {integerType};
    auto hashMapRef = ChainedHashMapRef(Value<MemRef>((int8_t*) &hashMap), keyDataTypes, 8, 8);
    auto f1 = Value<Int8>(42_s8);

    // check if entry already exists
    auto res1 = hashMapRef.find(hf->calculate(f1), {f1});
    ASSERT_FALSE(res1 != nullptr);

    // findOrCreate entry
    auto hash = hf->calculate(f1);
    NES_INFO("Hash: {}", hash.getValue().toString());
    hashMapRef.findOrCreate(hash, {f1});
    ASSERT_EQ(hashMap.getCurrentSize(), 1);
    auto res2 = hashMapRef.find(hash, {f1});
    ASSERT_TRUE(res2 != nullptr);
    // next should be null
    ASSERT_FALSE(res2.getNext() != nullptr);

    // findOrCreate same key again -> size should keep the same
    hashMapRef.findOrCreate(hash, {f1});
    ASSERT_EQ(hashMap.getCurrentSize(), 1);
}

TEST_F(ChainedHashMapTest, insertSmallNumberOfUniqueKey) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto hashMap = ChainedHashMap(8, 8, 1000, std::move(allocator));
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto keyDataTypes = {integerType};
    auto hashMapRef = ChainedHashMapRef(Value<MemRef>((int8_t*) &hashMap), keyDataTypes, 8, 8);

    for (uint64_t i = 0; i < 100; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.findOrCreate(hash, {key});
        Value<Int64> value(42_s64);
        entry.getValuePtr().store(value);
    }
    ASSERT_EQ(hashMap.getCurrentSize(), 100);
    for (uint64_t i = 0; i < 100; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.find(hash, {key});
        auto value = entry.getValuePtr().load<Int64>();
        ASSERT_EQ(value, 42_s64);
    }
}

TEST_F(ChainedHashMapTest, insertLargeNumberOfUniqueKey) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto hashMap = ChainedHashMap(8, 8, 1000, std::move(allocator));
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto keyDataTypes = {integerType};
    auto hashMapRef = ChainedHashMapRef(Value<MemRef>((int8_t*) &hashMap), keyDataTypes, 8, 8);

    for (uint64_t i = 0; i < 10000; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.findOrCreate(hash, {key});
        Value<Int64> value(42_s64);
        entry.getValuePtr().store(value);
    }
    ASSERT_EQ(hashMap.getCurrentSize(), 10000);
    for (uint64_t i = 0; i < 10000; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.find(hash, {key});
        auto value = entry.getValuePtr().load<Int64>();
        ASSERT_EQ(value, 42_s64);
    }
}

TEST_F(ChainedHashMapTest, updateValues) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto hashMap = ChainedHashMap(8, 8, 1000, std::move(allocator));
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto keyDataTypes = {integerType};
    auto hashMapRef = ChainedHashMapRef(Value<MemRef>((int8_t*) &hashMap), keyDataTypes, 8, 8);

    // insert
    for (uint64_t i = 0; i < 100; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.findOrCreate(hash, {key});
        Value<Int64> value = (int64_t) i;
        entry.getValuePtr().store(value);
    }
    // update
    for (uint64_t i = 0; i < 100; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.find(hash, {key});
        auto value = entry.getValuePtr().load<Int64>();
        entry.getValuePtr().store(value + 42);
    }
    // check
    for (uint64_t i = 0; i < 100; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.find(hash, {key});
        auto value = entry.getValuePtr().load<Int64>();
        ASSERT_EQ(value, (int64_t) i + 42);
    }
}

TEST_F(ChainedHashMapTest, insertDefaultValueOnCreation) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto hashMap = ChainedHashMap(8, 8, 1000, std::move(allocator));
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto keyDataTypes = {integerType};
    auto hashMapRef = ChainedHashMapRef(Value<MemRef>((int8_t*) &hashMap), keyDataTypes, 8, 8);

    // call findOrCreate multiple times with same key.
    for (uint64_t i = 0; i < 10000; i++) {
        Value<Int64> key = (int64_t) i % 100;
        auto hash = hf->calculate(key);
        hashMapRef.findOrCreate(hash, {key}, [i](ChainedHashMapRef::EntryRef& entry) {
            Value<Int64> value = (int64_t) i;
            entry.getValuePtr().store(value);
        });
    }
    ASSERT_EQ(hashMap.getCurrentSize(), 100);
    // check. Assume that we only inserted the first 100 elements
    for (uint64_t i = 0; i < 100; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.find(hash, {key});
        auto value = entry.getValuePtr().load<Int64>();
        ASSERT_EQ(value, (int64_t) i);
    }
}

TEST_F(ChainedHashMapTest, entryIterator) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto hashMap = ChainedHashMap(8, 8, 1000, std::move(allocator), 64);
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto keyDataTypes = {integerType};
    auto hashMapRef = ChainedHashMapRef(Value<MemRef>((int8_t*) &hashMap), keyDataTypes, 8, 8);

    for (uint64_t i = 0; i < 1000; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.findOrCreate(hash, {key});
        Value<Int64> value = (int64_t) i;
        entry.getValuePtr().store(value);
    }
    ASSERT_EQ(hashMap.getCurrentSize(), 1000);

    for (const auto& entry : hashMapRef) {
        auto value = entry.getValuePtr().load<Int64>();
        entry.getValuePtr().store(value + 42_s64);
    }

    for (uint64_t i = 0; i < 1000; i++) {
        Value<Int64> key = (int64_t) i;
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.find(hash, {key});
        auto value = entry.getValuePtr().load<Int64>();
        ASSERT_EQ(value, (int64_t) i + 42);
    }
}

TEST_F(ChainedHashMapTest, insertChain) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto hashMap = ChainedHashMap(8, 8, 1000, std::move(allocator), 64);
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto keyDataTypes = {integerType};
    auto hashMapRef = ChainedHashMapRef(Value<MemRef>((int8_t*) &hashMap), keyDataTypes, 8, 8);

    // insert 1000 vals with 2 unique keys
    for (uint64_t i = 0; i < 1000; i++) {
        Value<Int64> key = (int64_t) (i % 2);
        auto hash = hf->calculate(key);
        auto entry = hashMapRef.insert(hash, {key});
        Value<Int64> value = (int64_t) i;
        entry.getValuePtr().store(value);
    }
    ASSERT_EQ(hashMap.getCurrentSize(), 1000);

    std::vector<int64_t> resultVals;
    for (const auto& entry : hashMapRef) {
        auto k = entry.getKeyPtr().load<Int64>();
        auto key = k.value.get()->getValue();
        // Keys should be 0 or 1
        ASSERT_TRUE(key == 0 || key == 1);
        auto value = entry.getValuePtr().load<Int64>();
        resultVals.emplace_back(value.value.get()->getValue());
    }

    // Values should cover all values from 0 to 999
    for (int i = 0; i < 1000; i++) {
        if (!std::binary_search(resultVals.begin(), resultVals.end(), i)) {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(ChainedHashMapTest, KeyEntryIterator) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto hashMap = ChainedHashMap(8, 8, 1000, std::move(allocator), 64);
    PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto keyDataTypes = {integerType};
    auto hashMapRef = ChainedHashMapRef(Value<MemRef>((int8_t*) &hashMap), keyDataTypes, 8, 8);

    constexpr int64_t TEST_KEY = 7;
    Value<Int64> key = TEST_KEY;
    auto hash = hf->calculate(key);

    // Insert entries
    for (int64_t i = 0; i < 1000; i++) {
        auto entry = hashMapRef.insert(hash, {key});
        Value<Int64> value = (int64_t) i;
        entry.getValuePtr().store(value);
    }

    // Use KeyEntryIterator and update values
    for (auto iter = hashMapRef.findAll(hash, {key}); iter != nullptr; ++iter) {
        auto value = (*iter).getValuePtr().load<Int64>();
        (*iter).getValuePtr().store(value + 42_s64);
    }

    // Test result
    std::vector<int64_t> resultVals;
    for (const auto& entry : hashMapRef) {
        auto k = entry.getKeyPtr().load<Int64>();
        auto key = k.value.get()->getValue();
        // Keys should be TEST_KEY
        ASSERT_TRUE(key == TEST_KEY);
        auto value = entry.getValuePtr().load<Int64>();
        resultVals.emplace_back(value.value.get()->getValue());
    }

    // All values should be updated
    for (int i = 0; i < 1000; i++) {
        if (!std::binary_search(resultVals.begin(), resultVals.end(), i + 42)) {
            ASSERT_TRUE(false);
        }
    }
}

}// namespace NES::Nautilus::Interface