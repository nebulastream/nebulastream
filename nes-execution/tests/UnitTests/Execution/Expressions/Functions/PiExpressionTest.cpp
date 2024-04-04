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
#include <Execution/Expressions/Functions/PiExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Expressions {

class PiExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PiExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PiExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down PiExpressionTest test class."); }
};

TEST_F(PiExpressionTest, evaluatePiExpressionInteger) {
    auto expression = UnaryExpressionWrapper<PiExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(1_s8));
        ASSERT_EQ(resultValue, M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(2_s16));
        ASSERT_EQ(resultValue, M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(3_s32));
        ASSERT_EQ(resultValue, M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(4_s64));
        ASSERT_EQ(resultValue, M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(PiExpressionTest, evaluatePiExpressionFloat) {
    auto expression = UnaryExpressionWrapper<PiExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 5));
        ASSERT_EQ(resultValue, M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 6));
        ASSERT_EQ(resultValue, M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}
}// namespace NES::Runtime::Execution::Expressions
