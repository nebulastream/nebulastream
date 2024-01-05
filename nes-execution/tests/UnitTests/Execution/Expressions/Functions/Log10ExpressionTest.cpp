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
#include <Execution/Expressions/Functions/Log10Expression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class Log10ExpressionTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Log10ExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Log10ExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down Log10ExpressionTest test class."); }
};

TEST_F(Log10ExpressionTest, evaluateLog10ExpressionInteger) {
    auto expression = UnaryExpressionWrapper<Log10Expression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(100_s8));
        ASSERT_EQ(resultValue, (double) 2.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(100_s16));
        ASSERT_EQ(resultValue, (double) 2.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(100_s32));
        ASSERT_EQ(resultValue, (double) 2.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(100_s64));
        ASSERT_EQ(resultValue, (double) 2.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(Log10ExpressionTest, evaluateLog10ExpressionFloat) {
    auto expression = UnaryExpressionWrapper<Log10Expression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 100));
        ASSERT_EQ(resultValue, (double) 2.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 100));
        ASSERT_EQ(resultValue, (double) 2.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
    * @brief If we execute the expression on a boolean it should throw an exception.
    */
TEST_F(Log10ExpressionTest, evaluateLog10ExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<Log10Expression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
