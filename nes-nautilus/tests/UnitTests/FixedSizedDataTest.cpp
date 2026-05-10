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

#include <array>
#include <cstdint>
#include <cstring>
#include <ios>
#include <sstream>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <nautilus/function.hpp>
#include <nautilus/std/sstream.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <val_ptr.hpp>

namespace NES
{

class FixedSizedDataTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("FixedSizedDataTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup FixedSizedDataTest class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down FixedSizedDataTest class."); }
};

TEST_F(FixedSizedDataTest, GettersReflectConstructorArgs)
{
    constexpr size_t numElements = 16;
    std::array<uint16_t, numElements> data{};
    const nautilus::val<int8_t*> ptr(reinterpret_cast<int8_t*>(data.data()));
    const FixedSizedData arr(ptr, numElements, DataType::Type::UINT16);

    EXPECT_EQ(arr.getNumElements(), numElements);
    EXPECT_EQ(arr.getElementType(), DataType::Type::UINT16);
    EXPECT_EQ(arr.getTotalSizeInBytes(), numElements * sizeof(uint16_t));
}

/// Mirrors the thermal_frames_4x4 use case: 16 uint16 values loaded by index.
TEST_F(FixedSizedDataTest, AtReadsTypedUint16Elements)
{
    std::array<uint16_t, 16> data
        = {30100, 30180, 30220, 30150, 30210, 41900, 52400, 30260, 30230, 53100, 65535, 30290, 30170, 30240, 30205, 30130};
    const nautilus::val<int8_t*> ptr(reinterpret_cast<int8_t*>(data.data()));
    const FixedSizedData arr(ptr, data.size(), DataType::Type::UINT16);

    for (uint64_t i = 0; i < data.size(); ++i)
    {
        const VarVal element = arr.at(nautilus::val<uint64_t>(i));
        EXPECT_EQ(element.getRawValueAs<nautilus::val<uint16_t>>(), data[i]);
    }
}

TEST_F(FixedSizedDataTest, AtReadsTypedInt32Elements)
{
    std::array<int32_t, 4> data = {-1, 0, 1, 2147483647};
    const nautilus::val<int8_t*> ptr(reinterpret_cast<int8_t*>(data.data()));
    const FixedSizedData arr(ptr, data.size(), DataType::Type::INT32);

    for (uint64_t i = 0; i < data.size(); ++i)
    {
        const VarVal element = arr.at(nautilus::val<uint64_t>(i));
        EXPECT_EQ(element.getRawValueAs<nautilus::val<int32_t>>(), data[i]);
    }
}

TEST_F(FixedSizedDataTest, AtReadsTypedDoubleElements)
{
    std::array<double, 3> data = {-1.5, 0.0, 3.14159};
    const nautilus::val<int8_t*> ptr(reinterpret_cast<int8_t*>(data.data()));
    const FixedSizedData arr(ptr, data.size(), DataType::Type::FLOAT64);

    for (uint64_t i = 0; i < data.size(); ++i)
    {
        const VarVal element = arr.at(nautilus::val<uint64_t>(i));
        EXPECT_EQ(element.getRawValueAs<nautilus::val<double>>(), data[i]);
    }
}

TEST_F(FixedSizedDataTest, AtReadsTypedCharElements)
{
    std::array<char, 5> data = {'h', 'e', 'l', 'l', 'o'};
    const nautilus::val<int8_t*> ptr(reinterpret_cast<int8_t*>(data.data()));
    const FixedSizedData arr(ptr, data.size(), DataType::Type::CHAR);

    for (uint64_t i = 0; i < data.size(); ++i)
    {
        const VarVal element = arr.at(nautilus::val<uint64_t>(i));
        EXPECT_EQ(element.getRawValueAs<nautilus::val<char>>(), data[i]);
    }
}

TEST_F(FixedSizedDataTest, AtThrowsOnOutOfBounds)
{
    std::array<uint16_t, 4> data = {1, 2, 3, 4};
    const nautilus::val<int8_t*> ptr(reinterpret_cast<int8_t*>(data.data()));
    const FixedSizedData arr(ptr, data.size(), DataType::Type::UINT16);

    /// `EXCEPTION(name, ...)` generates a factory function, not a type, so we
    /// catch the common `Exception` and assert on the error code.
    for (const uint64_t outOfBoundsIdx : {data.size(), data.size() + 100})
    {
        try
        {
            (void)arr.at(nautilus::val<uint64_t>(outOfBoundsIdx));
            ADD_FAILURE() << "expected at(" << outOfBoundsIdx << ") to throw";
        }
        catch (const Exception& e)
        {
            EXPECT_EQ(e.code(), ErrorCode::OutOfRangeAccess);
        }
    }
}

TEST_F(FixedSizedDataTest, EqualityIdenticalContent)
{
    std::array<uint16_t, 4> a = {1, 2, 3, 4};
    std::array<uint16_t, 4> b = a;
    const FixedSizedData lhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(a.data())), a.size(), DataType::Type::UINT16);
    const FixedSizedData rhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(b.data())), b.size(), DataType::Type::UINT16);
    EXPECT_TRUE(lhs == rhs);
    EXPECT_FALSE(lhs != rhs);
}

TEST_F(FixedSizedDataTest, EqualityDifferentContent)
{
    std::array<uint16_t, 4> a = {1, 2, 3, 4};
    std::array<uint16_t, 4> b = {1, 2, 3, 5};
    const FixedSizedData lhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(a.data())), a.size(), DataType::Type::UINT16);
    const FixedSizedData rhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(b.data())), b.size(), DataType::Type::UINT16);
    EXPECT_FALSE(lhs == rhs);
    EXPECT_TRUE(lhs != rhs);
}

TEST_F(FixedSizedDataTest, EqualityDifferentElementType)
{
    std::array<uint16_t, 4> a = {1, 2, 3, 4};
    std::array<int16_t, 4> b = {1, 2, 3, 4};
    const FixedSizedData lhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(a.data())), a.size(), DataType::Type::UINT16);
    const FixedSizedData rhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(b.data())), b.size(), DataType::Type::INT16);
    EXPECT_FALSE(lhs == rhs);
}

TEST_F(FixedSizedDataTest, EqualityDifferentNumElements)
{
    std::array<uint16_t, 4> a = {1, 2, 3, 4};
    std::array<uint16_t, 5> b = {1, 2, 3, 4, 5};
    const FixedSizedData lhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(a.data())), a.size(), DataType::Type::UINT16);
    const FixedSizedData rhs(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(b.data())), b.size(), DataType::Type::UINT16);
    EXPECT_FALSE(lhs == rhs);
}

TEST_F(FixedSizedDataTest, CopyAndAssign)
{
    std::array<uint16_t, 4> data = {10, 20, 30, 40};
    const FixedSizedData original(
        nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(data.data())), data.size(), DataType::Type::UINT16);

    /// NOLINTNEXTLINE(performance-unnecessary-copy-initialization) - intentional
    const FixedSizedData copy = original;
    EXPECT_EQ(copy.getNumElements(), 4U);
    EXPECT_EQ(copy.getElementType(), DataType::Type::UINT16);
    EXPECT_EQ(copy.at(nautilus::val<uint64_t>(2)).getRawValueAs<nautilus::val<uint16_t>>(), 30);
}

void compareStringProxy(const char* actual, const char* expected)
{
    const std::string actualStr(actual);
    const std::string expectedStr(expected);
    EXPECT_EQ(expectedStr, actualStr);
}

TEST_F(FixedSizedDataTest, OstreamHexDumpsBytes)
{
    std::array<uint16_t, 2> data = {0x0102, 0x0304};
    const FixedSizedData arr(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(data.data())), data.size(), DataType::Type::UINT16);

    std::stringstream expectedOutput;
    expectedOutput << "FixedSizedData(elements=" << data.size() << ", bytes=" << (data.size() * sizeof(uint16_t)) << "): ";
    const auto* bytes = reinterpret_cast<const int8_t*>(data.data());
    for (size_t i = 0; i < data.size() * sizeof(uint16_t); ++i)
    {
        expectedOutput << std::hex << static_cast<int>(bytes[i] & 0xff) << " ";
    }
    nautilus::stringstream expected;
    expected << expectedOutput.str().c_str();

    nautilus::stringstream output;
    output << arr;

    nautilus::invoke(compareStringProxy, output.str().c_str(), expected.str().c_str());
}

}
