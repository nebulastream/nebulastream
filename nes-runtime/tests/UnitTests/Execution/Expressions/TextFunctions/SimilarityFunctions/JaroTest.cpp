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

#include <Execution/Expressions/TextFunctions/SimilarityFunctions/JaroSimilarity.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class JaroTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JaroTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup JaroTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup JaroTest test case.")
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down JaccardTest class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

/** @brief The Jaro Similarity Class provides functionality to compare two text objects and return their difference */

TEST_F(JaroTest, BaseJaroTest) {
    auto expression = TernaryExpressionWrapper<JaroSimilarity>();
    auto flagValue = Value<Boolean>(false);
    auto textValue = Value<Text>("duck");
    auto textValue0 = Value<Text>("duckdb");
    auto dist1 = expression.eval(textValue, textValue0, flagValue);
    EXPECT_EQ(dist1, (double) 8 / 9);

    auto textValue1 = Value<Text>("duck");
    auto dist2 = expression.eval(textValue, textValue1, flagValue);
    EXPECT_EQ(dist2, (double) 1.0);

    auto textValue2 = Value<Text>("test");
    auto textValue3 = Value<Text>("case");
    auto dist3 = expression.eval(textValue3, textValue2, flagValue);
    EXPECT_EQ(dist3, (double) 0.5);

    auto textValue4 = Value<Text>("testcase");
    auto textValue5 = Value<Text>("du");
    auto dist4 = expression.eval(textValue4, textValue5, flagValue);
    EXPECT_EQ(dist4, (double) 0.0);
}

TEST_F(JaroTest, FailJaroTest) {
    auto expression = TernaryExpressionWrapper<JaroSimilarity>();
    auto flagValue = Value<Boolean>(false);
    auto textValue0 = Value<Float>((float) 17.5);
    auto textValue1 = Value<Text>("duck");
    EXPECT_ANY_THROW(expression.eval(textValue0, textValue1, flagValue));
}

TEST_F(JaroTest, BaseJaroWinklerTest) {
    auto expression = TernaryExpressionWrapper<JaroSimilarity>();
    auto flagValue = Value<Boolean>(true);
    auto textValue = Value<Text>("duck");
    auto textValue0 = Value<Text>("duckdb");
    auto dist1 = expression.eval(textValue, textValue0, flagValue);
    EXPECT_EQ(dist1, (double) 14 / 15);

    auto textValue1 = Value<Text>("duck");
    auto dist2 = expression.eval(textValue, textValue1, flagValue);
    EXPECT_EQ(dist2, (double) 1.0);

    auto textValue2 = Value<Text>("test");
    auto textValue3 = Value<Text>("case");
    auto dist3 = expression.eval(textValue3, textValue2, flagValue);
    EXPECT_EQ(dist3, (double) 0.5);

    auto textValue4 = Value<Text>("testcase");
    auto textValue5 = Value<Text>("du");
    auto dist4 = expression.eval(textValue4, textValue5, flagValue);
    EXPECT_EQ(dist4, (double) 0.0);
}

TEST_F(JaroTest, FailJaroWinklerTest) {
    auto expression = TernaryExpressionWrapper<JaroSimilarity>();
    auto flagValue = Value<Boolean>(true);
    auto textValue0 = Value<Float>((float) 17.5);
    auto textValue1 = Value<Text>("duck");
    EXPECT_ANY_THROW(expression.eval(textValue0, textValue1, flagValue));
}

}// namespace NES::Runtime::Execution::Expressions