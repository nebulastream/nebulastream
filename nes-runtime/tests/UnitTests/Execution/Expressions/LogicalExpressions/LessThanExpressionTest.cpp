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
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class LessThanExpressionTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LessThanExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LessThanExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down LessThanExpressionTest test class."); }
};

TEST_F(LessThanExpressionTest, signedIntegers) {
    auto expression = BinaryExpressionWrapper<LessThanExpression>();

    // equal values
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(42_s8), Value<Int8>(42_s8));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(42_s16), Value<Int16>(42_s16));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(42), Value<Int32>(42));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(42_s64), Value<Int64>(42_s64));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // positive case
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(42_s8), Value<Int8>((int8_t) -4));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(42_s16), Value<Int16>((int16_t) -4));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(42), Value<Int32>(-4));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(42_s64), Value<Int64>((int64_t) -4));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // negative case
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>((int8_t) -2), Value<Int8>(4_s8));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>((int16_t) -2), Value<Int16>(4_s16));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(-2), Value<Int32>(4));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>((int64_t) -2), Value<Int64>(4_s64));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}

TEST_F(LessThanExpressionTest, UnsignedIntegers) {
    auto expression = BinaryExpressionWrapper<LessThanExpression>();

    // equal values
    // Int8
    {
        auto resultValue = expression.eval(Value<UInt8>(42_u8), Value<UInt8>(42_u8));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // Int16
    {
        auto resultValue = expression.eval(Value<UInt16>(42_u16), Value<UInt16>(42_u16));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<UInt32>(42_u32), Value<UInt32>(42_u32));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<UInt64>(42_u64), Value<UInt64>(42_u64));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // positive case
    // Int8
    {
        auto resultValue = expression.eval(Value<UInt8>(42_u8), Value<UInt8>(4_u8));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // Int16
    {
        auto resultValue = expression.eval(Value<UInt16>(42_u16), Value<UInt16>(4_u16));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<UInt32>(42_u32), Value<UInt32>(4_u32));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<UInt64>(42_u64), Value<UInt64>(4_u64));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // negative case
    // Int8
    {
        auto resultValue = expression.eval(Value<UInt8>(2_u8), Value<UInt8>(4_u8));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // Int16
    {
        auto resultValue = expression.eval(Value<UInt16>(2_u16), Value<UInt16>(4_u16));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<UInt32>(2_u32), Value<UInt32>(4_u32));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<UInt64>(2_u64), Value<UInt64>(4_u64));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}

TEST_F(LessThanExpressionTest, FloatingPoints) {
    auto expression = BinaryExpressionWrapper<LessThanExpression>();

    // equal values
    // float
    {
        auto resultValue = expression.eval(Value<Float>((float) 42), Value<Float>((float) 42));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // double
    {
        auto resultValue = expression.eval(Value<Double>((double) 42), Value<Double>((double) 42));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
    // positive case
    // float
    {
        auto resultValue = expression.eval(Value<Float>((float) 42), Value<Float>((float) 2.3));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // double
    {
        auto resultValue = expression.eval(Value<Double>((double) 42), Value<Double>((double) 2.3));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // negative case
    // float
    {
        auto resultValue = expression.eval(Value<Float>((float) 1.8), Value<Float>((float) 2.3));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }

    // double
    {
        auto resultValue = expression.eval(Value<Double>((double) 1.8), Value<Double>((double) 2.3));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}

}// namespace NES::Runtime::Execution::Expressions