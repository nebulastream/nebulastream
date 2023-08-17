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

#include <Execution/Expressions/TextFunctions/PatternMatching/SearchingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class SearchingRegexTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SearchingRegexTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SearchingRegexTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup SearchingRegexTest test case.")
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SearchingRegexTest test class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

/** @brief The SearchingRegex Class provides functionality that checks whether some sub-sequence in the target
 * sequence (the subject) matches the regular expression rgx (the pattern).
 */

TEST_F(SearchingRegexTest, evaluateSearchingRegex1) {
    auto expression = BinaryExpressionWrapper<SearchingRegex>();
    // Positive Test full match
    {
        auto l = Value<Text>("This is a Test");
        auto r = Value<Text>("This is a Test");
        auto resultValue = expression.eval(l, r);
        EXPECT_TRUE((Boolean) resultValue);
    }
}
TEST_F(SearchingRegexTest, evaluateSearchingRegex2) {
    auto expression = BinaryExpressionWrapper<SearchingRegex>();
    // Negative Test regex overflow
    {
        auto l = Value<Text>("This is a Test");
        auto r = Value<Text>("This is a Test!");
        auto resultValue = expression.eval(l, r);
        EXPECT_FALSE((Boolean) resultValue);
    }
}
TEST_F(SearchingRegexTest, evaluateSearchingRegex3) {
    auto expression = BinaryExpressionWrapper<SearchingRegex>();
    // Positive Test Subsequence
    {
        auto l = Value<Text>("This is a Test");
        auto r = Value<Text>("Test");
        auto resultValue = expression.eval(l, r);
        EXPECT_TRUE((Boolean) resultValue);
    }
}
TEST_F(SearchingRegexTest, evaluateSearchingRegex4) {
    auto expression = BinaryExpressionWrapper<SearchingRegex>();
    //Negative Test Subsequence
    {
        auto l = Value<Text>("This is a Test");
        auto r = Value<Text>("bbbb");
        auto resultValue = expression.eval(l, r);
        EXPECT_FALSE((Boolean) resultValue);
    }
}

}// namespace NES::Runtime::Execution::Expressions