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
#include <Execution/Expressions/Functions/AbsExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class AbsExpressionTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("AbsExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SubExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SubExpressionTest test class."); }
};

TEST_F(AbsExpressionTest, evaluateAbsExpressionFloat) {

    auto expression = UnaryExpressionWrapper<AbsExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) -17.4));
        ASSERT_EQ(resultValue, (float) 17.4);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) -17.4));
        ASSERT_EQ(resultValue, (double) 17.4);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(AbsExpressionTest, evaluateAbsExpressionUnsignedInt) {

    auto expression = UnaryExpressionWrapper<AbsExpression>();
    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>((uint8_t) 17));
        ASSERT_EQ(resultValue, (uint8_t) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt8>());
    }

    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>(static_cast<uint16_t>(UINT8_MAX)));
        ASSERT_EQ(resultValue, static_cast<uint16_t>(UINT8_MAX));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt16>());
    }

    // UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(static_cast<uint32_t>(UINT16_MAX)));
        ASSERT_EQ(resultValue, static_cast<uint32_t>(UINT16_MAX));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }

    // UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>(static_cast<uint64_t>(UINT32_MAX)));
        ASSERT_EQ(resultValue, static_cast<uint64_t>(UINT32_MAX));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt64>());
    }
}

TEST_F(AbsExpressionTest, evaluateAbsExpressionSignedInt) {

    auto expression = UnaryExpressionWrapper<AbsExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(static_cast<int8_t>(17)));
        ASSERT_EQ(resultValue, static_cast<int8_t>(17));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int8>());
    }

    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(static_cast<int16_t>(-INT8_MAX)));
        ASSERT_EQ(resultValue, static_cast<int16_t>(INT8_MAX));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int16>());
    }

    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(static_cast<int32_t>(-INT16_MAX)));
        ASSERT_EQ(resultValue, static_cast<int32_t>(INT16_MAX));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int32>());
    }

    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(static_cast<int64_t>(-INT32_MAX)));
        ASSERT_EQ(resultValue, static_cast<int64_t>(INT32_MAX));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int64>());
    }
}

/**
  * @brief If we execute the expression on a boolean it should throw an exception.
  */

TEST_F(AbsExpressionTest, evaluateAbsExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<AbsExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
