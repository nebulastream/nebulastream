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
#include <Execution/Expressions/Functions/BitcounterExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class BitcounterExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BitcounterExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BitcounterExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down BitcounterExpressionTest test class."); }
};
TEST_F(BitcounterExpressionTest, divIntegers) {
    auto expression = UnaryExpressionWrapper<BitcounterExpression>();

    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(31_s8));
        ASSERT_EQ(resultValue, 5_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(31_s16));
        ASSERT_EQ(resultValue, 5_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(31_s32));
        ASSERT_EQ(resultValue, 5_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(31_s64));
        ASSERT_EQ(resultValue, 5_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
}

TEST_F(BitcounterExpressionTest, divUIntegers) {
    auto expression = UnaryExpressionWrapper<BitcounterExpression>();

    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>(31_u8));
        ASSERT_EQ(resultValue, 5_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>(31_u16));
        ASSERT_EQ(resultValue, 5_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }// UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(31_u32));
        ASSERT_EQ(resultValue, 5_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
    // UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>(31_u64));
        ASSERT_EQ(resultValue, 5_u32);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
}

/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(BitcounterExpressionTest, evaluateBitCounterExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<BitcounterExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
