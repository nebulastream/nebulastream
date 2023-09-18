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
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Nautilus/Interface/DataTypes/TimeStamp/TimeStamp.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class AddExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AddExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AddExpressionTest test class.");
    }
};

TEST_F(AddExpressionTest, addIntegers) {
    auto addExpression = BinaryExpressionWrapper<AddExpression>();

    // Int8
    {
        auto resultValue = addExpression.eval(Value<Int8>(42_s8), Value<Int8>(42_s8));
        ASSERT_EQ(resultValue, 84);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int8>());
    }
    // Int16
    {
        auto resultValue = addExpression.eval(Value<Int16>(42_s16), Value<Int16>(42_s16));
        ASSERT_EQ(resultValue, 84);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int16>());
    }// Int32
    {
        auto resultValue = addExpression.eval(Value<Int32>(42), Value<Int32>(42));
        ASSERT_EQ(resultValue, 84);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int32>());
    }
    // Int64
    {
        auto resultValue = addExpression.eval(Value<Int64>(42_s64), Value<Int64>(42_s64));
        ASSERT_EQ(resultValue, 84);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int64>());
    }
}

TEST_F(AddExpressionTest, addUnsignedIntegers) {
    auto addExpression = BinaryExpressionWrapper<AddExpression>();

    // UInt8
    {
        auto resultValue = addExpression.eval(Value<UInt8>(42_u8), Value<UInt8>(42_u8));
        ASSERT_EQ(resultValue, 84_u8);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt8>());
    }
    // UInt16
    {
        auto resultValue = addExpression.eval(Value<UInt16>(42_u16), Value<UInt16>(42_u16));
        ASSERT_EQ(resultValue, 84_u16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt16>());
    }// UInt32
    {
        auto resultValue = addExpression.eval(Value<UInt32>(42u), Value<UInt32>(42u));
        ASSERT_EQ(resultValue, 84_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }// UInt64
    {
        auto resultValue = addExpression.eval(Value<UInt64>(42_u64), Value<UInt64>(42_u64));
        ASSERT_EQ(resultValue, 84_u64);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt64>());
    }
}

TEST_F(AddExpressionTest, addFloat) {
    auto addExpression = BinaryExpressionWrapper<AddExpression>();
    // Float
    {
        auto resultValue = addExpression.eval(Value<Float>((float) 42), Value<Float>((float) 42));
        ASSERT_EQ(resultValue, (float) 84);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Float>());
    }
    // Double
    {
        auto resultValue = addExpression.eval(Value<Double>((double) 42), Value<Double>((double) 42));
        ASSERT_EQ(resultValue, (float) 84);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(AddExpressionTest, addTimeStamps) {
    auto addExpression = BinaryExpressionWrapper<AddExpression>();
    long ms = 1666798551744;// Wed Oct 26 2022 15:35:51
    std::chrono::hours dur(ms);
    NES_DEBUG("{}", dur.count());
    auto c1 = Value<TimeStamp>(TimeStamp((uint64_t) dur.count()));
    // TimeStamp
    {
        auto resultValue = addExpression.eval(Value<TimeStamp>(TimeStamp((uint64_t) dur.count())),
                                              Value<TimeStamp>(TimeStamp((uint64_t) dur.count())));
        EXPECT_EQ(resultValue.as<TimeStamp>()->getMilliSeconds(), 3333597103488_u64);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<TimeStamp>());
    }
}

}// namespace NES::Runtime::Execution::Expressions