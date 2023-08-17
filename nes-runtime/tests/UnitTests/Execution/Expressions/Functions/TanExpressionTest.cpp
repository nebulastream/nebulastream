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

#include <Execution/Expressions/Functions/TanExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class TanExpressionTest : public Testing::TestWithErrorHandling {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TanExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TanExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TanExpressionTest test class."); }
};

TEST_F(TanExpressionTest, evaluateTanExpressionInteger) {
    auto expression = UnaryExpressionWrapper<TanExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(0_s8));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    //Int16
    {
        auto resultValue = expression.eval(Value<Int16>(0_s16));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(0_s32));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(0_s64));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>(0_u8));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    //UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>(0_u16));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    //UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(0_u32));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    //UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>(0_u64));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(TanExpressionTest, evaluateTanExpressionFloat) {
    auto expression = UnaryExpressionWrapper<TanExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 90));
        ASSERT_EQ(resultValue, (double) -1.995200412208242);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 90));
        ASSERT_EQ(resultValue, (double) -1.995200412208242);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
 * @brief If we execute the expression on a boolean it should throw an exception.
 */
TEST_F(TanExpressionTest, evaluateTanExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<TanExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions