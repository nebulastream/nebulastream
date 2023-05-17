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
#include <Nautilus/Interface/SequentialData/SequentialData.hpp>
#include <Nautilus/Interface/SequentialData/SequentialDataRef.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus::Interface {

class SequentialDataTest : public Testing::NESBaseTest {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SequentialDataTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SequentialDataTest test class.");
    }
    void SetUp() override { Testing::NESBaseTest::SetUp(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SequentialDataTest test class."); }
};

TEST_F(SequentialDataTest, appendValue) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto sequentialData = SequentialData(std::move(allocator), entrySize);
    auto sequentialDataRef = SequentialDataRef(Value<MemRef>((int8_t*) &sequentialData), entrySize);

    for (auto i = 0; i < 1000; i++) {
        sequentialDataRef.allocateEntry();
    }

    ASSERT_EQ(sequentialData.getNumberOfEntries(), 1000);
    ASSERT_EQ(sequentialData.getNumberOfPages(), 8);
}

TEST_F(SequentialDataTest, storeAndRetrieveValues) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = 32;
    auto sequentialData = SequentialData(std::move(allocator), entrySize);
    auto sequentialDataRef = SequentialDataRef(Value<MemRef>((int8_t*) &sequentialData), entrySize);

    for (auto i = 0UL; i < 1000UL; i++) {
        Value<UInt64> val((uint64_t) i);
        auto ref = sequentialDataRef.allocateEntry();
        ref.store(val);
    }

    ASSERT_EQ(sequentialData.getNumberOfEntries(), 1000);
    ASSERT_EQ(sequentialData.getNumberOfPages(), 8);

    uint64_t i = 0;
    for (auto it : sequentialDataRef) {
        Value<UInt64> expectedVal((uint64_t) i++);
        auto resultVal = it.load<UInt64>();
        ASSERT_EQ(resultVal.getValue().getValue(), expectedVal.getValue().getValue());
    }
}

}// namespace NES::Nautilus::Interface