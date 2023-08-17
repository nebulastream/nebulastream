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

#include <Execution/Expressions/Functions/TruncExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class TruncExpressionTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TruncExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TruncExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TruncExpressionTest test class."); }
};

TEST_F(TruncExpressionTest, evaluateTruncExpressionInteger) {
    auto expression = UnaryExpressionWrapper<TruncExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>((int8_t) 1.5));
        ASSERT_EQ(resultValue, (float) 1.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>((int16_t) 1.5));
        ASSERT_EQ(resultValue, (float) 1.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>((int32_t) 1.5));
        ASSERT_EQ(resultValue, (float) 1.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>((int64_t) 1.5));
        ASSERT_EQ(resultValue, (float) 1.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>((uint8_t) 1.5));
        ASSERT_EQ(resultValue, (float) 1.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>((uint16_t) 1.5));
        ASSERT_EQ(resultValue, (float) 1.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>((uint32_t) 1.5));
        ASSERT_EQ(resultValue, (float) 1.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>((uint64_t) 1.5));
        ASSERT_EQ(resultValue, (float) 1.0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(TruncExpressionTest, evaluateTruncExpressionFloat) {
    auto expression = UnaryExpressionWrapper<TruncExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 1.5));
        ASSERT_EQ(resultValue, std::trunc(1.5));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 1.5));
        ASSERT_EQ(resultValue, std::trunc(1.5));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
 * @brief If we execute the expression on a boolean it should throw an exception.
 */
TEST_F(TruncExpressionTest, evaluateTruncExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<TruncExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(false)););
}

}// namespace NES::Runtime::Execution::Expressions
