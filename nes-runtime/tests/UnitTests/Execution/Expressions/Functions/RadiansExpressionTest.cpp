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
#include <Execution/Expressions/Functions/RadiansExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class RadiansExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RadiansExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RadiansExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down RadiansExpressionTest test class."); }
};

TEST_F(RadiansExpressionTest, evaluateRadiansExpressionInteger) {
    auto expression = UnaryExpressionWrapper<RadiansExpression>();
    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>(90_s8));
        ASSERT_EQ(resultValue, M_PI_2);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>(180_s16));
        ASSERT_EQ(resultValue, M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int32
    {
        auto resultValue = expression.eval(Value<Int32>(360_s32));
        ASSERT_EQ(resultValue, 2 * M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>(45_s64));
        ASSERT_EQ(resultValue, M_PI_4);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}
TEST_F(RadiansExpressionTest, evaluateRadiansExpressionFloat) {
    auto expression = UnaryExpressionWrapper<RadiansExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 90));
        ASSERT_EQ(resultValue, M_PI_2);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 180));
        ASSERT_EQ(resultValue, M_PI);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}
}// namespace NES::Runtime::Execution::Expressions