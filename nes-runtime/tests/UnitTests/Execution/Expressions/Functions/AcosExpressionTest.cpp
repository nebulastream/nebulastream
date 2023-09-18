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
#include <Execution/Expressions/Functions/AcosExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class AcosExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AcosExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AcosExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down AcosExpressionTest test class."); }
};

TEST_F(AcosExpressionTest, evaluateAcosExpressionInteger) {
    auto expression = UnaryExpressionWrapper<AcosExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(1_s8));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(1_s32));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(1_s64));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(AcosExpressionTest, evaluateAcosExpressionFloat) {
    auto expression = UnaryExpressionWrapper<AcosExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 0.5));
        ASSERT_EQ(resultValue, std::acos(0.5));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 0.5));
        ASSERT_EQ(resultValue, std::acos(0.5));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 0.5));
        ASSERT_EQ(resultValue, std::acos(0.5));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 0.5));
        ASSERT_EQ(resultValue, std::acos(0.5));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}
/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(AcosExpressionTest, evaluateAcosExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<AcosExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(false)););
}
}// namespace NES::Runtime::Execution::Expressions
