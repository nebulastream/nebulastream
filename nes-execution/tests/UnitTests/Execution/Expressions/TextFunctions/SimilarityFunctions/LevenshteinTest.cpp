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
#include <Execution/Expressions/TextFunctions/SimilarityFunctions/LevenshteinDistance.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class LevenshteinTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LevenshteinTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LevenshteinTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup LevenshteinTest test case.")
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down LevenshteinTest class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

/** @brief The LevenshteinDistance Class provides functionality to compare two text objects and return their difference */

TEST_F(LevenshteinTest, BaseTest) {
    auto expression = BinaryExpressionWrapper<LevenshteinDistance>();
    auto textValue0 = Value<Text>("db");
    auto textValue1 = Value<Text>("duck");
    auto dist1 = expression.eval(textValue0, textValue1);
    EXPECT_EQ(dist1, 3_u64);

    auto textValue2 = Value<Text>("aro");
    auto dist2 = expression.eval(textValue2, Value<Text>("roa"));
    EXPECT_EQ(dist2, 2_u64);

    auto dist3 = expression.eval(textValue2, Value<Text>("oar"));
    EXPECT_EQ(dist3, 2_u64);

    auto dist4 = expression.eval(textValue2, Value<Text>("daro"));
    EXPECT_EQ(dist4, 1_u64);

    auto dist5 = expression.eval(textValue2, Value<Text>("ao"));
    EXPECT_EQ(dist5, 1_u64);

    auto textValue3 = Value<Text>("duck");
    auto dist6 = expression.eval(textValue1, textValue3);
    EXPECT_EQ(dist6, 0_u64);

    auto textValue4 = Value<Text>("d");
    auto textValue5 = Value<Text>("z");
    auto dist7 = expression.eval(textValue4, textValue5);
    EXPECT_EQ(dist7, 1_u64);
}

TEST_F(LevenshteinTest, FailTest) {
    auto expression = BinaryExpressionWrapper<LevenshteinDistance>();
    auto textValue0 = Value<Float>((float) 17.5);
    auto textValue1 = Value<Text>("duck");
    EXPECT_ANY_THROW(expression.eval(textValue0, textValue1));
}

}// namespace NES::Runtime::Execution::Expressions