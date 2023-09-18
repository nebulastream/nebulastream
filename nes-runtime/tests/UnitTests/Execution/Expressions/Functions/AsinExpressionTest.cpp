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
#include <Execution/Expressions/Functions/AsinExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class AsinExpressionTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SinExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SinExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SinExpressionTest test class."); }
};

double calculateAsin(double x) { return std::asin(x); }

TEST_F(AsinExpressionTest, evaluateSinExpressionFloat) {
    auto expression = UnaryExpressionWrapper<AsinExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 1));
        ASSERT_EQ(resultValue, (double) calculateAsin((float) 1));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 1));
        ASSERT_EQ(resultValue, (double) calculateAsin(1));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(AsinExpressionTest, evaluateAsinExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<AsinExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
