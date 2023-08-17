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

#include <Execution/Expressions/Functions/RandomExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Expressions {

class RandomExpressionTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RandomExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RandomExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down RandomExpressionTest test class."); }
};

TEST_F(RandomExpressionTest, evaluateRandomExpressionInteger) {
    auto expression = UnaryExpressionWrapper<RandomExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(1_s8));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(2_s16));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(3_s32));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(4_s64));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>(1_u8));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>(2_u16));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(3_u32));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>(4_u64));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>((uint8_t) 1));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>((uint16_t) 2));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>((uint32_t) 3));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>((uint64_t) 4));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(RandomExpressionTest, evaluateRandomExpressionFloat) {
    auto expression = UnaryExpressionWrapper<RandomExpression>();
    {
        auto resultValue = expression.eval(Value<Float>((float) 5));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    {
        auto resultValue = expression.eval(Value<Double>((double) 6));
        ASSERT_TRUE((bool) (resultValue > 0.0 && resultValue < 1.0));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}
/**
        * @brief If we execute the expression on a boolean it should throw an exception.
        */
TEST_F(RandomExpressionTest, evaluateSinExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<RandomExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
