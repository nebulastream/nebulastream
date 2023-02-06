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

#include <Execution/Expressions/PatternMatching/ReplacingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class ReplacingRegexTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ReplacingRegexTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ReplacingRegexTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }
};

TEST_F(ReplacingRegexTest, evaluateReplacingRegex) {
    auto expression = TernaryExpressionWrapper<ReplacingRegex>();
    // Replace
    {
        auto resultValue = expression.eval(transformReturnValues(TextValue::create("abc")), transformReturnValues(TextValue::create("(b|c)")), transformReturnValues(TextValue::create("X")));
        ASSERT_EQ(resultValue, "aXc");
    }

    // Replacement notation
    {
        auto resultValue = expression.eval(transformReturnValues(TextValue::create("abc")), transformReturnValues(TextValue::create("(b|c)")), transformReturnValues(TextValue::create("\1\1\1\1")));
        ASSERT_EQ(resultValue, "abbbbc");
    }

    // Regex test
    {
        auto resultValue = expression.eval(transformReturnValues(TextValue::create("abc")), transformReturnValues(TextValue::create("(.*)c")), transformReturnValues(TextValue::create("\1e")));
        ASSERT_EQ(resultValue, "abe");
    }

    // Regex test
    {
        auto resultValue = expression.eval(transformReturnValues(TextValue::create("abc")), transformReturnValues(TextValue::create("(a)(b)")), transformReturnValues(TextValue::create("\2\1")));
        ASSERT_EQ(resultValue, "bac");
    }

}

/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(ReplacingRegexTest, evaluateReplacingRegexOnWrongType) {
    auto expression = TernaryExpressionWrapper<ReplacingRegex>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true), Value<Boolean>(true), Value<Boolean>(false)););
}

}// namespace NES::Runtime::Execution::Expressions