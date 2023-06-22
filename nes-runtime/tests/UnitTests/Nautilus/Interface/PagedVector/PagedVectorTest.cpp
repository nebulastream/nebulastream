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
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus::Interface {

class PagedVectorTest : public Testing::NESBaseTest {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PagedVectorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PagedVectorTest test class.");
    }
    void SetUp() override { Testing::NESBaseTest::SetUp(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down PagedVectorTest test class."); }
};

TEST_F(PagedVectorTest, appendValue) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto pagedVector = PagedVector(std::move(allocator), entrySize);
    auto pagedVectorRef = PagedVectorRef(Value<MemRef>((int8_t*) &pagedVector), (uint64_t) entrySize);

    for (auto i = 0; i < 1000; i++) {
        pagedVectorRef.allocateEntry();
    }

    ASSERT_EQ(pagedVector.getNumberOfEntries(), 1000);
    ASSERT_EQ(pagedVector.getNumberOfPages(), 8);
}

TEST_F(PagedVectorTest, changePageSize) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto pagedVector = PagedVector(std::move(allocator), entrySize, 2048);
    auto pagedVectorRef = PagedVectorRef(Value<MemRef>((int8_t*) &pagedVector), (uint64_t) entrySize, (uint64_t) 2048);

    for (auto i = 0; i < 1000; i++) {
        pagedVectorRef.allocateEntry();
    }

    ASSERT_EQ(pagedVector.getNumberOfEntries(), 1000);
    ASSERT_EQ(pagedVector.getNumberOfPages(), 16);
}

TEST_F(PagedVectorTest, storeAndRetrieveValues) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto pagedVector = PagedVector(std::move(allocator), entrySize);
    auto pagedVectorRef = PagedVectorRef(Value<MemRef>((int8_t*) &pagedVector), (uint64_t) entrySize);

    for (auto i = 0UL; i < 1000UL; i++) {
        Value<UInt64> val((uint64_t) i);
        auto ref = pagedVectorRef.allocateEntry();
        ref.store(val);
    }

    ASSERT_EQ(pagedVector.getNumberOfEntries(), 1000);
    ASSERT_EQ(pagedVector.getNumberOfPages(), 8);

    uint64_t i = 0;
    for (auto it : pagedVectorRef) {
        Value<UInt64> expectedVal((uint64_t) i++);
        auto resultVal = it.load<UInt64>();
        ASSERT_EQ(resultVal.getValue().getValue(), expectedVal.getValue().getValue());
    }
}

TEST_F(PagedVectorTest, storeAndRetrieveValuesAfterMoveFromTo) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto pagedVector = PagedVector(std::move(allocator), entrySize);
    auto pagedVectorRef = PagedVectorRef(Value<MemRef>((int8_t*) &pagedVector), entrySize);

    for (auto i = 0UL; i < 1000UL; i++) {
        Value<UInt64> val((uint64_t) i);
        auto ref = pagedVectorRef.allocateEntry();
        ref.store(val);
    }

    ASSERT_EQ(pagedVector.getNumberOfEntries(), 1000);
    ASSERT_EQ(pagedVector.getNumberOfPages(), 8);

    pagedVector.moveFromTo(0, 30);
    pagedVector.moveFromTo(10, 40);
    pagedVector.moveFromTo(100, 130);

    uint64_t i = 0;
    for (auto it : pagedVectorRef) {
        uint64_t expected = i++;

        // We replaced the value at these given positions, and therefore have to calculate the new expected value
        if (expected == 30 || expected == 40 || expected == 130) {
            expected -= 30;
        }
        Value<UInt64> expectedVal(expected);
        auto resultVal = it.load<UInt64>();
        ASSERT_EQ(resultVal.getValue().getValue(), expectedVal.getValue().getValue());
    }
}

}// namespace NES::Nautilus::Interface