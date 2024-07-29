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
#include <Execution/Expressions/Functions/CotExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Expressions {

class CotExpressionTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CotExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup CotExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down CotExpressionTest test class."); }
};

/**
 * @brief This method returns the cot of x for a reference in the tests.
 * @param x
 * @return cot
 */
double cot(double x) { return std::cos(x) / std::sin(x); }

TEST_F(CotExpressionTest, evaluateCotExpressionInteger) {
    auto expression = UnaryExpressionWrapper<CotExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(90_s8));
        ASSERT_EQ(resultValue, cot(90));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(90_s16));
        ASSERT_EQ(resultValue, cot(90));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(90_s32));
        ASSERT_EQ(resultValue, cot(90));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(90_s64));
        ASSERT_EQ(resultValue, cot(90));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(CotExpressionTest, evaluateCotExpressionFloat) {
    auto expression = UnaryExpressionWrapper<CotExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 90));
        ASSERT_EQ(resultValue, cot(90));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 90));
        ASSERT_EQ(resultValue, cot(90));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(CotExpressionTest, evaluateCotExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<CotExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
