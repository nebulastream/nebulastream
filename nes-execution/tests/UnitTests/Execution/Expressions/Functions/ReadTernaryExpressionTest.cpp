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
#include <Execution/Expressions/Functions/ReadTernaryExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class ReadTernaryExpressionTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ReadTernaryExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ReadTernaryExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ReadTernaryExpressionTest test class."); }
};

TEST_F(ReadTernaryExpressionTest, evaluateReadTernaryExpressionInteger) {
    auto expression = TernaryExpressionWrapper<ReadTernaryExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(8_s8), Value<Int8>(8_s8), Value<Int8>(8_s8));
        ASSERT_EQ(resultValue, 3.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(8_s16), Value<Int16>(8_s16), Value<Int16>(8_s16));
        ASSERT_EQ(resultValue, 3.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(8_s32), Value<Int32>(8_s32), Value<Int32>(8_s32));
        ASSERT_EQ(resultValue, 3.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(8_s64), Value<Int64>(8_s64), Value<Int64>(8_s64));
        ASSERT_EQ(resultValue, 3.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(ReadTernaryExpressionTest, evaluateReadTernaryExpressionFloat) {
    auto expression = TernaryExpressionWrapper<ReadTernaryExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 8), Value<Float>((float) 8), Value<Float>((float) 8));
        ASSERT_EQ(resultValue, 3.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 8), Value<Double>((double) 8), Value<Double>((double) 8));
        ASSERT_EQ(resultValue, 3.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
    * @brief If we execute the expression on a boolean it should throw an exception.
    */
TEST_F(ReadTernaryExpressionTest, evaluateAsinExpressionOnWrongType) {
    auto expression = TernaryExpressionWrapper<ReadTernaryExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true), Value<Boolean>(true), Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
