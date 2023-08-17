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

#include <Execution/Expressions/TextFunctions/PatternMatching/ExtractingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class ExtractingRegexTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExtractingRegexTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ExtractingRegexTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup ExtractingRegexTest test case.")
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ExtractingRegexTest test class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

TEST_F(ExtractingRegexTest, evaluateExtractingRegex1) {
    auto expression = TernaryExpressionWrapper<ExtractingRegex>();
    // Simple Replace
    {
        auto l = Value<Text>("abc");
        auto m = Value<Text>(".b.");
        auto idx = 0;
        auto resultValue = expression.eval(l, m, idx);
        NES_DEBUG("{}", resultValue.as<Text>()->toString());
        EXPECT_EQ(resultValue, Value<Text>("abc"));
    }
}
TEST_F(ExtractingRegexTest, evaluateExtractingRegex2) {
    auto expression = TernaryExpressionWrapper<ExtractingRegex>();
    // Replacement notation for groups
    {
        auto l = Value<Text>("abc");
        auto m = Value<Text>(".b.");
        auto idx = 1;
        auto resultValue = expression.eval(l, m, idx);
        EXPECT_EQ(resultValue, Value<Text>(""));
    }
}
TEST_F(ExtractingRegexTest, evaluateExtractingRegex3) {
    auto expression = TernaryExpressionWrapper<ExtractingRegex>();
    // Regex string correction
    {
        auto l = Value<Text>("abc");
        auto m = Value<Text>("([a-z])(b)");
        auto idx = 1;
        auto resultValue = expression.eval(l, m, idx);
        EXPECT_EQ(resultValue, Value<Text>("a"));
    }
}
TEST_F(ExtractingRegexTest, evaluateExtractingRegex4) {
    auto expression = TernaryExpressionWrapper<ExtractingRegex>();
    // Regex swap
    {
        auto l = Value<Text>("abc");
        auto m = Value<Text>("([a-z])(b)");
        auto idx = 2;
        auto resultValue = expression.eval(l, m, idx);
        EXPECT_EQ(resultValue, Value<Text>("b"));
    }
}
}// namespace NES::Runtime::Execution::Expressions