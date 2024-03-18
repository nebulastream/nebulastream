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

#include <API/QueryAPI.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeUnit.hpp>

#include <BaseIntegrationTest.hpp>
#include <Operators/LogicalOperators/Windows/Types/SlidingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/ThresholdWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <gtest/gtest.h>

using namespace NES::Windowing;

class WindowTypeHashTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WindowTypeHashTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup WindowTypeHashTest test class.");
    }
};
TEST_F(WindowTypeHashTest, SlidingWindowHashTest) {
    auto timeCharacteristic1 = EventTime(Attribute("time"));
    auto timeCharacteristic2 = EventTime(Attribute("timestamp"));
    auto slidingWindow0 = SlidingWindow::of(timeCharacteristic1, Seconds(10), Seconds(5));
    auto slidingWindow1 = SlidingWindow::of(timeCharacteristic1, Seconds(10), Seconds(5));
    auto slidingWindow2 = SlidingWindow::of(timeCharacteristic2, Seconds(10), Seconds(5));
    auto slidingWindow3 = SlidingWindow::of(timeCharacteristic1, Seconds(10), Milliseconds(5000));
    auto slidingWindow4 = SlidingWindow::of(timeCharacteristic1, Seconds(5), Seconds(5));
    auto slidingWindow5 = SlidingWindow::of(timeCharacteristic1, Seconds(10), Seconds(10));

    //tests hash values are the same for slidingWindows with same parameters
    EXPECT_EQ(slidingWindow0->hash(), slidingWindow1->hash());
    //tests that size=Seconds(5) and size=Milliseconds(5000) gives the same hash value
    EXPECT_EQ(slidingWindow1->hash(), slidingWindow3->hash());

    auto windows = {slidingWindow1, slidingWindow2, slidingWindow4, slidingWindow5};
    for (const auto& w1 : windows) {
        for (const auto& w2 : windows) {
            if (w1 != w2) {
                EXPECT_NE(w1->hash(), w2->hash());
            }
        }
    }
}

TEST_F(WindowTypeHashTest, TumblingWindowHashTest) {
    auto timeCharacteristic1 = EventTime(Attribute("time"));
    auto timeCharacteristic2 = EventTime(Attribute("timestamp"));
    auto tumblingWindow0 = TumblingWindow::of(timeCharacteristic1, Seconds(10));
    auto tumblingWindow1 = TumblingWindow::of(timeCharacteristic1, Seconds(10));
    auto tumblingWindow2 = TumblingWindow::of(timeCharacteristic2, Seconds(10));
    auto tumblingWindow3 = TumblingWindow::of(timeCharacteristic1, Seconds(5));
    auto tumblingWindow4 = TumblingWindow::of(timeCharacteristic1, Milliseconds(5000));

    //tests hash values are the same for tumblingWindows with same parameters
    EXPECT_EQ(tumblingWindow0->hash(), tumblingWindow1->hash());
    //tests that size=Seconds(5) and size=Milliseconds(5000) gives the same hash value
    EXPECT_EQ(tumblingWindow3->hash(), tumblingWindow4->hash());

    auto windows = {tumblingWindow1, tumblingWindow2, tumblingWindow3};
    for (const auto& w1 : windows) {
        for (const auto& w2 : windows) {
            if (w1 != w2) {
                EXPECT_NE(w1->hash(), w2->hash());
            }
        }
    }
}

TEST_F(WindowTypeHashTest, ThresholdWindowHashTest) {
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "10"));
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "11"));
    auto expression1 = EqualsExpressionNode::create(left, right);
    auto expression2 = LessEqualsExpressionNode::create(left, right);

    auto thresholdWindow0 = ThresholdWindow::of(expression1, 10);
    auto thresholdWindow1 = ThresholdWindow::of(expression2, 10);
    auto thresholdWindow2 = ThresholdWindow::of(expression1, 5);
    auto thresholdWindow3 = ThresholdWindow::of(expression1, 5);

    //tests hash values are the same for thresholdWindows with same parameters
    EXPECT_EQ(thresholdWindow2->hash(), thresholdWindow3->hash());

    auto windows = {thresholdWindow1, thresholdWindow2, thresholdWindow0};
    for (const auto& w1 : windows) {
        for (const auto& w2 : windows) {
            if (w1 != w2) {
                EXPECT_NE(w1->hash(), w2->hash());
            }
        }
    }
}

TEST_F(WindowTypeHashTest, AllWindowTypesHashTest) {
    auto timeCharacteristic1 = EventTime(Attribute("time"));
    auto timeCharacteristic2 = EventTime(Attribute("timestamp"));//t2 makes no difference

    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "10"));
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "11"));
    auto expression1 = EqualsExpressionNode::create(left, right);
    auto expression2 = LessEqualsExpressionNode::create(left, right);

    auto slidingWindow0 = SlidingWindow::of(timeCharacteristic1, Seconds(10), Seconds(5));
    auto slidingWindow1 = SlidingWindow::of(timeCharacteristic1, Seconds(10), Seconds(5));
    auto slidingWindow2 = SlidingWindow::of(timeCharacteristic2, Seconds(10), Seconds(5));
    auto slidingWindow3 = SlidingWindow::of(timeCharacteristic1, Seconds(10), Seconds(8));
    auto slidingWindow4 = SlidingWindow::of(timeCharacteristic1, Seconds(8), Seconds(5));

    auto tumblingWindow0 = TumblingWindow::of(timeCharacteristic1, Seconds(10));
    auto tumblingWindow1 = TumblingWindow::of(timeCharacteristic1, Seconds(10));
    auto tumblingWindow2 = TumblingWindow::of(timeCharacteristic1, Seconds(11));
    auto tumblingWindow3 = TumblingWindow::of(timeCharacteristic2, Seconds(10));
    auto tumblingWindow4 = TumblingWindow::of(timeCharacteristic1, Milliseconds(10));

    auto thresholdWindow0 = ThresholdWindow::of(expression1, 10);
    auto thresholdWindow1 = ThresholdWindow::of(expression1, 10);
    auto thresholdWindow2 = ThresholdWindow::of(expression1, 5);
    auto thresholdWindow3 = ThresholdWindow::of(expression2, 10);

    auto windows = {slidingWindow1,
                    slidingWindow2,
                    slidingWindow3,
                    slidingWindow4,
                    tumblingWindow1,
                    tumblingWindow2,
                    tumblingWindow3,
                    tumblingWindow4,
                    thresholdWindow1,
                    thresholdWindow2,
                    thresholdWindow3};

    for (const auto& w1 : windows) {
        for (const auto& w2 : windows) {
            if (w1 != w2) {
                ASSERT_NE(w1->hash(), w2->hash());
            }
        }
    }
    //tests hash values are the same WindowTypes with same parameters
    EXPECT_EQ(slidingWindow0->hash(), slidingWindow1->hash());
    EXPECT_EQ(thresholdWindow0->hash(), thresholdWindow1->hash());
    EXPECT_EQ(tumblingWindow0->hash(), tumblingWindow1->hash());
}