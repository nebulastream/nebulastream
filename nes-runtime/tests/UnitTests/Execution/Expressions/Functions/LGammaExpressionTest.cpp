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
#include <Execution/Expressions/Functions/LGammaExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class LGammaExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LGammaExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LGammaExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down LGammaExpressionTest test class."); }
};

TEST_F(LGammaExpressionTest, lGammaIntegers) {
    auto expression = UnaryExpressionWrapper<LGammaExpression>();

    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(2_s8));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(2_s16));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(2));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(2_s64));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(LGammaExpressionTest, lGammaUnsignedIntegers) {
    auto expression = UnaryExpressionWrapper<LGammaExpression>();

    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>(2_u8));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>(2_u16));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(2u));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>(2_u64));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    {
        auto resultValue = expression.eval(Value<UInt64>(2_u64));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(LGammaExpressionTest, lGammaFloat) {
    auto expression = UnaryExpressionWrapper<LGammaExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 2));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 2));
        ASSERT_EQ(resultValue, (double) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

}// namespace NES::Runtime::Execution::Expressions