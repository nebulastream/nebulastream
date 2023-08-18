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
#include <Execution/Expressions/TextFunctions/PatternMatching/ReplacingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class ReplacingRegexTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ReplacingRegexTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ReplacingRegexTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup ReplacingRegexTest test case.")
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ReplacingRegexTest test class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

/** @brief The ReplacingRegex Class provides functionality that replaces the first occurrence of regex with the replacement */

TEST_F(ReplacingRegexTest, evaluateReplacingRegex1) {
    auto expression = TernaryExpressionWrapper<ReplacingRegex>();
    // Simple Replace
    {
        auto l = Value<Text>("abc");
        auto m = Value<Text>("(b|c)");
        auto r = Value<Text>("X");
        auto resultValue = expression.eval(l, m, r);
        NES_DEBUG("{}", resultValue.as<Text>()->toString());
        EXPECT_EQ(resultValue, Value<Text>("aXX"));
    }
}
TEST_F(ReplacingRegexTest, evaluateReplacingRegex2) {
    auto expression = TernaryExpressionWrapper<ReplacingRegex>();
    // Replacement notation for groups
    {
        auto l = Value<Text>("abc");
        auto m = Value<Text>("[b]");
        auto r = Value<Text>("X");
        auto resultValue = expression.eval(l, m, r);
        EXPECT_EQ(resultValue, Value<Text>("aXc"));
    }
}
TEST_F(ReplacingRegexTest, evaluateReplacingRegex3) {
    auto expression = TernaryExpressionWrapper<ReplacingRegex>();
    // Regex string correction
    {
        auto l = Value<Text>("there is a subsequence in the string");
        auto m = Value<Text>("\\b(sub)([^ ]*)");
        auto r = Value<Text>("sub-$2");
        auto resultValue = expression.eval(l, m, r);
        EXPECT_EQ(resultValue, Value<Text>("there is a sub-sequence in the string"));
    }
}
TEST_F(ReplacingRegexTest, evaluateReplacingRegex4) {
    auto expression = TernaryExpressionWrapper<ReplacingRegex>();
    // Regex swap
    {
        auto l = Value<Text>("abbbc");
        auto m = Value<Text>("b{2,}");
        auto r = Value<Text>("X");
        auto resultValue = expression.eval(l, m, r);
        EXPECT_EQ(resultValue, Value<Text>("aXc"));
    }
}

}// namespace NES::Runtime::Execution::Expressions