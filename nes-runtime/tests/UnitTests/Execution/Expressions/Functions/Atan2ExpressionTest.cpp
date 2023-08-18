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
#include <Execution/Expressions/Functions/Atan2Expression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class Atan2ExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Atan2ExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Atan2ExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down Atan2ExpressionTest test class."); }
};

TEST_F(Atan2ExpressionTest, evaluateAtan2ExpressionFloat) {
    auto expression = BinaryExpressionWrapper<Atan2Expression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 0.5), Value<Float>((float) 0.5));
        ASSERT_EQ(resultValue, 0.7853981633974483);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}
TEST_F(Atan2ExpressionTest, evaluateAtan2ExpressionDouble) {
    auto expression = BinaryExpressionWrapper<Atan2Expression>();
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 0.5), Value<Double>((double) 0.5));
        ASSERT_EQ(resultValue, 0.7853981633974483);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(Atan2ExpressionTest, evaluateAtan2ExpressionOnWrongType) {
    auto expression = BinaryExpressionWrapper<Atan2Expression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true), Value<Boolean>(false)););
    ASSERT_ANY_THROW(expression.eval(Value<Double>(0.5), Value<Int8>((Int8) 1)););
}

}// namespace NES::Runtime::Execution::Expressions