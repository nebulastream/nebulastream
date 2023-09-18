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
#include <Exceptions/InvalidArgumentException.hpp>
#include <Spatial/DataTypes/Point.hpp>
#include <Spatial/DataTypes/Rectangle.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class RectangleTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RectangleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RectangleTest test class.");
    }
};
// This tests the rectangle datatype
TEST_F(RectangleTest, testRectangleDatatype) {
    // create the test rectangles
    auto rectangle = Rectangle(52.5138, 13.3265, 52.5157, 13.3284);
    auto invertedRectangle = Rectangle(52.5138, 1.3265, 52.5157, -179.3284);
    auto defaultRectangle = Rectangle();

    // other shapes to test topological relationship with the test rectangle
    // a rectangle that intersects with the test rectangle
    auto rectangleIntersects = Rectangle(52.5137, 13.3266, 52.5157, 13.3284);
    // rectangle that does not intersect with rectangle
    auto rectangleNotIntersects = Rectangle(62.5137, 11.3266, 62.5157, 11.3284);

    // a rectangle that is inside the test rectangle
    auto rectangleInside = Rectangle(52.5139, 13.3269, 52.5156, 13.3281);
    // rectangle that is not inside rectangle
    auto rectangleNotInside = Rectangle(32.6756, 17.8263, 33.5139, 17.9288);

    // point that is inside the test rectangle
    auto pointInside = Point(52.5146604, 13.3271885);
    // point that is not inside rectangle1
    auto pointNotInside = Point(62.5146604, 15.3271885);

    // test accessor methods
    EXPECT_DOUBLE_EQ(rectangle.getLatitudeLow(), 52.5138);
    EXPECT_DOUBLE_EQ(rectangle.getLongitudeLow(), 13.3265);
    EXPECT_DOUBLE_EQ(rectangle.getLatitudeHigh(), 52.5157);
    EXPECT_DOUBLE_EQ(rectangle.getLongitudeHigh(), 13.3284);
    EXPECT_DOUBLE_EQ(rectangle.getLow().getLatitude(), 52.5138);
    EXPECT_DOUBLE_EQ(rectangle.getLow().getLongitude(), 13.3265);
    EXPECT_DOUBLE_EQ(rectangle.getHigh().getLatitude(), 52.5157);
    EXPECT_DOUBLE_EQ(rectangle.getHigh().getLongitude(), 13.3284);

    // test the predicates
    EXPECT_TRUE(rectangle.intersects(rectangleIntersects));
    EXPECT_FALSE(rectangle.intersects(rectangleNotIntersects));
    EXPECT_TRUE(rectangle.contains(rectangleInside));
    EXPECT_FALSE(rectangle.contains(rectangleNotInside));
    EXPECT_TRUE(rectangle.contains(pointInside));
    EXPECT_FALSE(rectangle.contains(pointNotInside));

    // test inverted rectangle
    EXPECT_TRUE(invertedRectangle.isRectangleInverted());
    EXPECT_FALSE(defaultRectangle.isRectangleInverted());
    EXPECT_FALSE(rectangle.isRectangleInverted());

    // test the default empty rectangle
    EXPECT_DOUBLE_EQ(defaultRectangle.getLatitudeLow(), -90.0);
    EXPECT_DOUBLE_EQ(defaultRectangle.getLongitudeLow(), -180.0);
    EXPECT_DOUBLE_EQ(defaultRectangle.getLatitudeHigh(), 90);
    EXPECT_DOUBLE_EQ(defaultRectangle.getLongitudeHigh(), 180.0);
    EXPECT_DOUBLE_EQ(defaultRectangle.getLow().getLatitude(), -90.0);
    EXPECT_DOUBLE_EQ(defaultRectangle.getLow().getLongitude(), -180.0);
    EXPECT_DOUBLE_EQ(defaultRectangle.getHigh().getLatitude(), 90.0);
    EXPECT_DOUBLE_EQ(defaultRectangle.getHigh().getLongitude(), 180.0);
    EXPECT_TRUE(defaultRectangle.contains(Point()));
    EXPECT_TRUE(defaultRectangle.contains(pointNotInside));
}

}// namespace NES