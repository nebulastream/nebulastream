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
#include <Execution/Expressions/Functions/SinExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class SinExpressionTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SinExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SinExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SinExpressionTest test class."); }
};

TEST_F(SinExpressionTest, evaluateSinExpressionInteger) {
    auto expression = UnaryExpressionWrapper<SinExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(90_s8));
        ASSERT_EQ(resultValue, (double) 0.8939966636005579);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(90_s16));
        ASSERT_EQ(resultValue, (double) 0.8939966636005579);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(90_s32));
        ASSERT_EQ(resultValue, (double) 0.8939966636005579);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(90_s64));
        ASSERT_EQ(resultValue, (double) 0.8939966636005579);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(SinExpressionTest, evaluateSinExpressionFloat) {
    auto expression = UnaryExpressionWrapper<SinExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 90));
        ASSERT_EQ(resultValue, (double) 0.8939966636005579);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 90));
        ASSERT_EQ(resultValue, (double) 0.8939966636005579);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
  * @brief If we execute the expression on a boolean it should throw an exception.
  */
TEST_F(SinExpressionTest, evaluateSinExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<SinExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
