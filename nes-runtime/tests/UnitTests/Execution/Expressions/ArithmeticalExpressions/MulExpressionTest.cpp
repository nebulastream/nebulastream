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
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class MulExpressionTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MulExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MulExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down MulExpressionTest test class."); }
};

TEST_F(MulExpressionTest, mulIntegers) {
    auto expression = BinaryExpressionWrapper<MulExpression>();

    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>((int8_t) 4), Value<Int8>((int8_t) 4));
        ASSERT_EQ(resultValue, 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int8>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>((int16_t) 4), Value<Int16>((int16_t) 4));
        ASSERT_EQ(resultValue, 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int16>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(4), Value<Int32>(4));
        ASSERT_EQ(resultValue, 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int32>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>((int64_t) 4), Value<Int64>((int64_t) 4));
        ASSERT_EQ(resultValue, 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int64>());
    }
}

TEST_F(MulExpressionTest, addUnsignedIntegers) {
    auto expression = BinaryExpressionWrapper<MulExpression>();

    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>((uint8_t) 4), Value<UInt8>((uint8_t) 4));
        ASSERT_EQ(resultValue, (uint8_t) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt8>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>((uint16_t) 4), Value<UInt16>((uint16_t) 4));
        ASSERT_EQ(resultValue, (uint16_t) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt16>());
    }// UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(4u), Value<UInt32>(4u));
        ASSERT_EQ(resultValue, (uint32_t) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }// UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>((uint64_t) 4), Value<UInt64>((uint64_t) 4));
        ASSERT_EQ(resultValue, (uint64_t) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt64>());
    }
}

TEST_F(MulExpressionTest, addFloat) {
    auto expression = BinaryExpressionWrapper<MulExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 4), Value<Float>((float) 4));
        ASSERT_EQ(resultValue, (float) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Float>());
    }

    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 4), Value<Double>((double) 4));
        ASSERT_EQ(resultValue, (double) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

}// namespace NES::Runtime::Execution::Expressions