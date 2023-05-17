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
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/List/List.hpp>
#include <Nautilus/Interface/List/ListRef.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus::Interface {

class ListTest : public Testing::NESBaseTest {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ListTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ListTest test class.");
    }
    void SetUp() override { Testing::NESBaseTest::SetUp(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ListTest test class."); }
};

TEST_F(ListTest, appendValue) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto list = List(std::move(allocator), entrySize);
    auto listRef = ListRef(Value<MemRef>((int8_t*) &list), entrySize);

    for (auto i = 0; i < 1000; i++) {
        listRef.allocateEntry();
    }

    ASSERT_EQ(list.getNumberOfEntries(), 1000);
    ASSERT_EQ(list.getNumberOfPages(), 8);
}

TEST_F(ListTest, storeAndRetrieveValues) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto list = List(std::move(allocator), entrySize);
    auto listRef = ListRef(Value<MemRef>((int8_t*) &list), entrySize);

    for (auto i = 0UL; i < 1000UL; i++) {
        Value<UInt64> val((uint64_t) i);
        auto ref = listRef.allocateEntry();
        ref.store(val);
    }

    ASSERT_EQ(list.getNumberOfEntries(), 1000);
    ASSERT_EQ(list.getNumberOfPages(), 8);

    uint64_t i = 0;
    for (auto it : listRef) {
        Value<UInt64> expectedVal((uint64_t) i++);
        auto resultVal = it.load<UInt64>();
        ASSERT_EQ(resultVal.getValue().getValue(), expectedVal.getValue().getValue());
    }
}

}// namespace NES::Nautilus::Interface