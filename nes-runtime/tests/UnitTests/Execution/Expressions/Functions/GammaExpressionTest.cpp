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

#include <Execution/Expressions/Functions/GammaExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class GammaExpressionTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GammaExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GammaExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down GammaExpressionTest test class."); }
};

TEST_F(GammaExpressionTest, gammaIntegers) {
    auto expression = UnaryExpressionWrapper<GammaExpression>();

    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(5_s8));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(5_s16));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(5_s32));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(5_s64));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(GammaExpressionTest, gammaUnsignedIntegers) {
    auto expression = UnaryExpressionWrapper<GammaExpression>();

    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>(5_u8));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>(5_u16));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(5u));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>(5_u64));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    {
        auto resultValue = expression.eval(Value<UInt64>(5_u64));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(GammaExpressionTest, gammaFloat) {
    auto expression = UnaryExpressionWrapper<GammaExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 5.5));
        ASSERT_EQ(resultValue, (double) 52.34277778455352);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 5.5));
        ASSERT_EQ(resultValue, (double) 52.34277778455352);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

}// namespace NES::Runtime::Execution::Expressions