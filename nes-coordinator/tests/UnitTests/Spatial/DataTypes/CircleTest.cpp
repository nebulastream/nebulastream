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
#include <Common/Spatial/Circle.hpp>
#include <Common/Spatial/Point.hpp>
#include <Common/Spatial/Rectangle.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class CircleTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CircleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup CircleTest test class.");
    }
};

// This tests the circle datatype
TEST_F(CircleTest, testCircleDatatype) {

    // create the test circle
    auto circle = Circle(52.5153, 13.3267, 100.0);

    // create other shapes to test various methods in the circle class
    auto circle1 = Circle(29.386844738083383, 79.4592465137537, 30.0);
    auto pointInsideCircle1 = Point(52.5153, 13.3267);
    auto pointInsideCircle2 = Point(52.51531, 13.32671);
    auto degenerateRectangle1 = Rectangle(52.51531, 13.32671, 52.51531, 13.32671);
    auto degenerateRectangle2 = Rectangle(29.386844738083383, 79.4592465137537, 29.386844738083383, 79.4592465137537);

    // test accessor methods
    EXPECT_DOUBLE_EQ(circle.getLatitude(), pointInsideCircle1.getLatitude());
    EXPECT_DOUBLE_EQ(circle.getLongitude(), pointInsideCircle1.getLongitude());
    EXPECT_DOUBLE_EQ(circle.getRadius(), 100.0);
    EXPECT_TRUE(circle.getLatitude() != pointInsideCircle2.getLatitude());
    EXPECT_TRUE(circle.getLongitude() != pointInsideCircle2.getLongitude());
    EXPECT_TRUE(circle.getCenter() == pointInsideCircle1);
    EXPECT_FALSE(circle.getCenter() == pointInsideCircle2);

    // test predicates
    EXPECT_TRUE(circle.contains(pointInsideCircle1));
    EXPECT_TRUE(circle.contains(pointInsideCircle2));
    EXPECT_TRUE(circle.contains(pointInsideCircle1.getLatitude(), pointInsideCircle1.getLongitude()));
    EXPECT_TRUE(circle.contains(pointInsideCircle2.getLatitude(), pointInsideCircle2.getLongitude()));
    EXPECT_TRUE(circle.contains(degenerateRectangle1));
    EXPECT_FALSE(circle.contains(degenerateRectangle2));
    EXPECT_FALSE(circle1.contains(pointInsideCircle1));
    EXPECT_FALSE(circle1.contains(pointInsideCircle2));
    EXPECT_FALSE(circle1.contains(pointInsideCircle1.getLatitude(), pointInsideCircle1.getLongitude()));
    EXPECT_FALSE(circle1.contains(pointInsideCircle2.getLatitude(), pointInsideCircle2.getLongitude()));
    EXPECT_FALSE(circle1.contains(degenerateRectangle1));
    EXPECT_TRUE(circle1.contains(degenerateRectangle2));
}

}// namespace NES