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
#include <Execution/Expressions/Functions/DegreesExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class DegreesExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DegreesExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DegreesExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down DegreesExpressionTest test class."); }
};

double calculateDegreesTest(double x) { return (x * 180.0) / M_PI; }

TEST_F(DegreesExpressionTest, evaluateDegreesExpressionInteger) {
    auto expression = UnaryExpressionWrapper<DegreesExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>((int8_t) M_PI));
        ASSERT_EQ(resultValue, calculateDegreesTest((int8_t) M_PI));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>((int16_t) M_PI_2));
        ASSERT_EQ(resultValue, calculateDegreesTest((int16_t) M_PI_2));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>((int32_t) M_PI_4));
        ASSERT_EQ(resultValue, calculateDegreesTest((int32_t) M_PI_4));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>((int64_t) M_PI));
        ASSERT_EQ(resultValue, calculateDegreesTest((int64_t) M_PI));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(DegreesExpressionTest, evaluateDegreesExpressionFloat) {
    auto expression = UnaryExpressionWrapper<DegreesExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) M_PI));
        ASSERT_EQ(resultValue.as<Double>(), calculateDegreesTest((float) M_PI));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 2 * M_PI));
        ASSERT_EQ(resultValue, 360.00);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}
}// namespace NES::Runtime::Execution::Expressions
