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

#include <Execution/Expressions/Functions/PowerExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class PowerExpressionTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PowerExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PowerExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down PowerExpressionTest test class."); }
};

TEST_F(PowerExpressionTest, evaluatePowerExpressionInteger) {
    auto expression = BinaryExpressionWrapper<PowerExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(2_s8), Value<Int8>(4_s8));
        ASSERT_EQ(resultValue, (float) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(2_s32), Value<Int32>(4_s32));
        ASSERT_EQ(resultValue, (float) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(2_s64), Value<Int64>(4_s64));
        ASSERT_EQ(resultValue, (float) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(PowerExpressionTest, evaluatePowerExpressionFloat) {
    auto expression = BinaryExpressionWrapper<PowerExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 4), Value<Float>((float) 0));
        ASSERT_EQ(resultValue, (float) 1);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 2), Value<Float>((float) 4));
        ASSERT_EQ(resultValue, (float) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 2), Value<Double>((double) 4));
        ASSERT_EQ(resultValue, (float) 16);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 4), Value<Double>((double) 0));
        ASSERT_EQ(resultValue, (double) 1);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
 * @brief If we execute the expression on a boolean it should throw an exception.
 */
TEST_F(PowerExpressionTest, evaluatePowerExpressionOnWrongType) {
    auto expression = BinaryExpressionWrapper<PowerExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true), Value<Boolean>(false)););
}

}// namespace NES::Runtime::Execution::Expressions