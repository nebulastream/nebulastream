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
#include <Execution/Expressions/TextFunctions/SimilarityFunctions/HammingDistance.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class HammingTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("HammingTestTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup HammingTestTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup HammingTestTest test case.")
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down HammingTestTest class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

/** @brief The HammingDistance Class provides functionality to compare two text objects and return their difference */

TEST_F(HammingTest, BaseTest) {
    auto expression = BinaryExpressionWrapper<HammingDistance>();
    auto textValue0 = Value<Text>("lurk");
    auto textValue1 = Value<Text>("duck");
    auto dist1 = expression.eval(textValue0, textValue1);
    EXPECT_EQ(dist1, 2_u64);

    auto textValue2 = Value<Text>("duk");
    auto textValue3 = Value<Text>("duk");
    auto dist2 = expression.eval(textValue2, textValue3);
    EXPECT_EQ(dist2, 0_u64);
}

TEST_F(HammingTest, FailTestInputType) {
    auto expression = BinaryExpressionWrapper<HammingDistance>();
    auto textValue0 = Value<Float>((float) 17.5);
    auto textValue1 = Value<Text>("duck");
    EXPECT_ANY_THROW(expression.eval(textValue0, textValue1));
}

TEST_F(HammingTest, FailTestInputLength) {
    auto expression = BinaryExpressionWrapper<HammingDistance>();
    auto textValue1 = Value<Text>("duck");
    auto textValue2 = Value<Text>("duk");
    EXPECT_ANY_THROW(expression.eval(textValue2, textValue1));
}

}// namespace NES::Runtime::Execution::Expressions