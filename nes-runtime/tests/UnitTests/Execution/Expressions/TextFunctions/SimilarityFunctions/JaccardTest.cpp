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

#include <Execution/Expressions/TextFunctions/SimilarityFunctions/JaccardDistance.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class JaccardTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JaccardTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup JaccardTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup JaccardTest test case.")
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down JaccardTest class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

/** @brief The JaccardDistance Class provides functionality to compare two text objects and return their difference */

TEST_F(JaccardTest, BaseTest) {
    auto expression = BinaryExpressionWrapper<JaccardDistance>();
    auto textValue = Value<Text>("duck");
    auto textValue0 = Value<Text>("pluck");
    auto dist1 = expression.eval(textValue, textValue0);
    EXPECT_EQ(dist1, (double) 0.5);

    auto textValue1 = Value<Text>("du");
    auto textValue2 = Value<Text>("testcase");
    auto dist2 = expression.eval(textValue1, textValue2);
    EXPECT_EQ(dist2, (double) 0.0);

    auto textValue3 = Value<Text>("da");
    auto dist3 = expression.eval(textValue3, textValue2);
    //EXPECT_EQ(dist3, (double) 0.1);
    //TODO ASSERT_NEAR does not work out of the box, dist3 is identified as boolean

    auto textValue4 = Value<Text>("duck");
    auto dist4 = expression.eval(textValue, textValue4);
    EXPECT_EQ(dist4, (double) 1.0);
}

TEST_F(JaccardTest, FailTest) {
    auto expression = BinaryExpressionWrapper<JaccardDistance>();
    auto textValue0 = Value<Float>((float) 17.5);
    auto textValue1 = Value<Text>("duck");
    EXPECT_ANY_THROW(expression.eval(textValue0, textValue1));
}

}// namespace NES::Runtime::Execution::Expressions