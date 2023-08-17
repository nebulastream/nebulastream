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

#include <Execution/Expressions/Functions/LnExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class LnExpressionTest : public Testing::TestWithErrorHandling {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LnExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LnExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down LnExpressionTest test class."); }
};

TEST_F(LnExpressionTest, evaluateLnExpressionInteger) {
    auto expression = UnaryExpressionWrapper<LnExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(2_s8));
        ASSERT_EQ(resultValue, (double) 0.6931471805599453);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(2_s16));
        ASSERT_EQ(resultValue, (double) 0.6931471805599453);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(2_s32));
        ASSERT_EQ(resultValue, (double) 0.6931471805599453);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(2_s64));
        ASSERT_EQ(resultValue, (double) 0.6931471805599453);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(LnExpressionTest, evaluateLnExpressionFloat) {
    auto expression = UnaryExpressionWrapper<LnExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 2));
        ASSERT_EQ(resultValue, (double) 0.6931471805599453);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 2));
        ASSERT_EQ(resultValue, (double) 0.6931471805599453);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
    * @brief If we execute the expression on a boolean it should throw an exception.
    */
TEST_F(LnExpressionTest, evaluateCotExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<LnExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
