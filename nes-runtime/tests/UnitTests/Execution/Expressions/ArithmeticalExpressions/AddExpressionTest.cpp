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

#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Nautilus/Interface/DataTypes/TimeStamp/TimeStamp.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class AddExpressionTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AddExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AddExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down AddExpressionTest test class."); }
};

TEST_F(AddExpressionTest, addIntegers) {
    auto addExpression = BinaryExpressionWrapper<AddExpression>();

    // Int8
    {
        auto resultValue = addExpression.eval(Value<Int8>((int8_t) 42), Value<Int8>((int8_t) 42));
        ASSERT_EQ(resultValue, 84);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int8>());
    }
    // Int16
    {
        auto resultValue = addExpression.eval(Value<Int16>((int16_t) 42), Value<Int16>((int16_t) 42));
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
        auto resultValue = addExpression.eval(Value<Int64>((int64_t) 42), Value<Int64>((int64_t) 42));
        ASSERT_EQ(resultValue, 84);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int64>());
    }
}

TEST_F(AddExpressionTest, addUnsignedIntegers) {
    auto addExpression = BinaryExpressionWrapper<AddExpression>();

    // UInt8
    {
        auto resultValue = addExpression.eval(Value<UInt8>((uint8_t) 42), Value<UInt8>((uint8_t) 42));
        ASSERT_EQ(resultValue, (uint8_t) 84u);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt8>());
    }
    // UInt16
    {
        auto resultValue = addExpression.eval(Value<UInt16>((uint16_t) 42), Value<UInt16>((uint16_t) 42));
        ASSERT_EQ(resultValue, (uint16_t) 84u);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt16>());
    }// UInt32
    {
        auto resultValue = addExpression.eval(Value<UInt32>(42u), Value<UInt32>(42u));
        ASSERT_EQ(resultValue, (uint32_t) 84u);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }// UInt64
    {
        auto resultValue = addExpression.eval(Value<UInt64>((uint64_t) 42), Value<UInt64>((uint64_t) 42));
        ASSERT_EQ(resultValue, (uint64_t) 84u);
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
    NES_DEBUG(dur.count());
    auto c1 = Value<TimeStamp>(TimeStamp((int64_t) dur.count()));
    // TimeStamp
    {
        auto resultValue = addExpression.eval(Value<TimeStamp>(TimeStamp((int64_t) dur.count())),
                                              Value<TimeStamp>(TimeStamp((int64_t) dur.count())));
        ASSERT_EQ(resultValue.getValue().toString(), std::to_string(3333597103488));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<TimeStamp>());
    }
}

}// namespace NES::Runtime::Execution::Expressions