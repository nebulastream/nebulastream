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
#include <Execution/Expressions/TextFunctions/PatternMatching/MatchingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class MatchingRegexTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MatchingRegexTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MatchingRegexTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup MatchingRegexTest test case.")
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down MatchingRegexTest test class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

TEST_F(MatchingRegexTest, evaluateRegexMatch1) {
    auto expression = TernaryExpressionWrapper<MatchingRegex>();
    // TextValue
    {
        auto l = Value<Text>("test");
        auto r = Value<Text>("test");
        auto resultValue = expression.eval(l, r, (Boolean) false);
        EXPECT_EQ(resultValue, (Boolean) true);
        EXPECT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}
TEST_F(MatchingRegexTest, evaluateRegexMatch2) {
    auto expression = TernaryExpressionWrapper<MatchingRegex>();
    // TextValue
    {
        auto l = Value<Text>("test");
        auto r = Value<Text>("t");
        auto resultValue = expression.eval(l, r, (Boolean) false);
        EXPECT_EQ(resultValue, (Boolean) false);
        EXPECT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}
TEST_F(MatchingRegexTest, evaluateRegexMatch3) {
    auto expression = TernaryExpressionWrapper<MatchingRegex>();
    // TextValue
    {
        auto l = Value<Text>("test");
        auto r = Value<Text>("^t$");
        auto resultValue = expression.eval(l, r, (Boolean) false);
        EXPECT_EQ(resultValue, (Boolean) false);
        EXPECT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}
TEST_F(MatchingRegexTest, evaluateRegexMatch4) {
    auto expression = TernaryExpressionWrapper<MatchingRegex>();
    // TextValue
    {
        auto l = Value<Text>("test");
        auto r = Value<Text>(".*(e|s).*");
        auto resultValue = expression.eval(l, r, (Boolean) false);
        EXPECT_EQ(resultValue, (Boolean) true);
        EXPECT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}
TEST_F(MatchingRegexTest, evaluateRegexMatch5) {
    auto expression = TernaryExpressionWrapper<MatchingRegex>();
    // TextValue
    {
        auto l = Value<Text>("test");
        auto r = Value<Text>("(e|s).*");
        auto resultValue = expression.eval(l, r, (Boolean) false);
        EXPECT_EQ(resultValue, (Boolean) false);
        EXPECT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}
TEST_F(MatchingRegexTest, evaluateRegexMatch6) {
    auto expression = TernaryExpressionWrapper<MatchingRegex>();
    // TextValue
    {
        auto l = Value<Text>("test");
        auto r = Value<Text>("^(e|s).*");
        auto resultValue = expression.eval(l, r, (Boolean) false);
        EXPECT_EQ(resultValue, (Boolean) false);
        EXPECT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}
TEST_F(MatchingRegexTest, evaluateRegexMatch7) {
    auto expression = TernaryExpressionWrapper<MatchingRegex>();
    // TextValue
    {
        auto l = Value<Text>("tESt");
        auto r = Value<Text>(".*(e|s).*");
        auto resultValue = expression.eval(l, r, (Boolean) false);
        EXPECT_EQ(resultValue, (Boolean) false);
        EXPECT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}
TEST_F(MatchingRegexTest, evaluateRegexMatch8) {
    auto expression = TernaryExpressionWrapper<MatchingRegex>();
    // TextValue
    {
        auto l = Value<Text>("tESt");
        auto r = Value<Text>(".*(e|s).*");
        auto resultValue = expression.eval(l, r, (Boolean) true);
        EXPECT_EQ(resultValue, (Boolean) true);
        EXPECT_TRUE(resultValue->getTypeIdentifier()->isType<Boolean>());
    }
}

}// namespace NES::Runtime::Execution::Expressions