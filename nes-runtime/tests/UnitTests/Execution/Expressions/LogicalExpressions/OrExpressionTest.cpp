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
#include <Execution/Expressions/LogicalExpressions/NegateExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class OrExpressionTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("OrExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup OrExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down OrExpressionTest test class."); }
};

TEST_F(OrExpressionTest, baseBoolCases) {
    auto expression = UnaryExpressionWrapper<NegateExpression>();

    {
        auto resultValue = expression.eval(Value<Boolean>(true));
        ASSERT_EQ(resultValue, (bool) false);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
    {
        auto resultValue = expression.eval(Value<Boolean>(false));
        ASSERT_EQ(resultValue, (bool) true);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}

}// namespace NES::Runtime::Execution::Expressions