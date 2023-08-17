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

#include <Execution/Expressions/Functions/FloorExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class FloorExpressionTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FloorExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup FloorExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down FloorExpressionTest test class."); }
};

TEST_F(FloorExpressionTest, evaluateFloorExpressionFloat) {
    auto expression = UnaryExpressionWrapper<FloorExpression>();
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 17.9));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 17.5));
        ASSERT_EQ(resultValue, (float) 17);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(FloorExpressionTest, evaluateFloorExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<FloorExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions
