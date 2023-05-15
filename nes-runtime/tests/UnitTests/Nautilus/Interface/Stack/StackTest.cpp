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
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus::Interface {

class StackTest : public Testing::NESBaseTest {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StackTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StackTest test class.");
    }
    void SetUp() override { Testing::NESBaseTest::SetUp(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down StackTest test class."); }
};

TEST_F(StackTest, appendValue) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto stack = Stack(std::move(allocator), entrySize);
    auto stackRef = StackRef(Value<MemRef>((int8_t*) &stack), entrySize);

    for (auto i = 0; i < 1000; i++) {
        stackRef.allocateEntry();
    }

    ASSERT_EQ(stack.getNumberOfEntries(), 1000);
    ASSERT_EQ(stack.getNumberOfPages(), 8);
}

TEST_F(StackTest, storeAndRetrieveValues) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto stack = Stack(std::move(allocator), entrySize);
    auto stackRef = StackRef(Value<MemRef>((int8_t*) &stack), entrySize);

    for (auto i = 0UL; i < 1000UL; i++) {
        Value<UInt64> val(i);
        auto ref = stackRef.allocateEntry();
        ref.store(val);
    }

    ASSERT_EQ(stack.getNumberOfEntries(), 1000);
    ASSERT_EQ(stack.getNumberOfPages(), 8);

    for (auto i = 0UL; i < 1000UL; i++) {
        Value<UInt64> pos(i);
        Value<UInt64> expectedVal(i);
        auto ref = stackRef.getEntry(pos);
        auto resultVal = ref.load<UInt64>();
        ASSERT_EQ(resultVal.getValue().getValue(), expectedVal.getValue().getValue());
    }
}

}// namespace NES::Nautilus::Interface