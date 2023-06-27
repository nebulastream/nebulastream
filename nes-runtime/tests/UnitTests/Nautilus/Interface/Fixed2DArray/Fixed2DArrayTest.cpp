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

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArray.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArrayRef.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>

namespace NES::Nautilus::Interface {
class Fixed2DArrayTest : public Testing::NESBaseTest {
  public:
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FixedArrayTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup FixedArrayTest test class.");
    }
    void SetUp() override { Testing::NESBaseTest::SetUp(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down FixedArrayTest test class."); }
};

TEST_F(Fixed2DArrayTest, insertSimpleDataTypes) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto numRows = 5UL;
    auto numCols = 5UL;
    auto entrySize = sizeof(uint64_t);

    Fixed2DArray fixed2DArray(*allocator, numRows, numCols, entrySize);
    Fixed2DArrayRef fixed2DArrayRef(Value<MemRef>((int8_t*) &fixed2DArray), entrySize, numCols);

    for (Value<UInt64> row = (uint64_t) 0UL; row < (uint64_t) numRows; row = row + 1) {
        for (Value<UInt64> col = (uint64_t) 0UL; col < (uint64_t) numCols; col = col + 1) {
            Value<UInt64> expectedValue(col.getValue().getValue() + row.getValue().getValue() * numCols);
            auto cell = fixed2DArrayRef[row][col];
            cell.store(expectedValue);
        }
    }

    for (Value<UInt64> row = (uint64_t) 0UL; row < (uint64_t) numRows; row = row + 1) {
        for (Value<UInt64> col = (uint64_t) 0UL; col < (uint64_t) numCols; col = col + 1) {
            Value<UInt64> expectedValue(col.getValue().getValue() + row.getValue().getValue() * numCols);
            auto cell = fixed2DArrayRef[row][col];
            auto resultValue = cell.load<UInt64>();
            EXPECT_EQ(expectedValue, resultValue);
        }
    }
}

TEST_F(Fixed2DArrayTest, insertCustomClass) {
    struct CustomClass {
        uint64_t id;
        int32_t val1;
        double val2;
    };

    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto numRows = 5UL;
    auto numCols = 5UL;
    auto entrySize = sizeof(CustomClass);

    Fixed2DArray fixed2DArray(*allocator, numRows, numCols, entrySize);
    Fixed2DArrayRef fixed2DArrayRef(Value<MemRef>((int8_t*) &fixed2DArray), entrySize, numCols);

    for (Value<UInt64> row = (uint64_t) 0UL; row < (uint64_t) numRows; row = row + 1) {
        for (Value<UInt64> col = (uint64_t) 0UL; col < (uint64_t) numCols; col = col + 1) {
            auto cellCustomClass = static_cast<CustomClass*>(fixed2DArrayRef[row][col].getValue().getValue());
            cellCustomClass->id = col.getValue().getValue() + row.getValue().getValue() * numCols;
            cellCustomClass->val1 = 42;
            cellCustomClass->val2 = 42.0 / col.getValue().getValue();
        }
    }

    for (Value<UInt64> row = (uint64_t) 0UL; row < (uint64_t) numRows; row = row + 1) {
        for (Value<UInt64> col = (uint64_t) 0UL; col < (uint64_t) numCols; col = col + 1) {
            auto cellCustomClass = static_cast<CustomClass*>(fixed2DArrayRef[row][col].getValue().getValue());
            EXPECT_EQ(cellCustomClass->id, col.getValue().getValue() + row.getValue().getValue() * numCols);
            EXPECT_EQ(cellCustomClass->val1, 42);
            EXPECT_EQ(cellCustomClass->val2, 42.0 / col.getValue().getValue());
        }
    }
}

}// namespace NES::Nautilus::Interface