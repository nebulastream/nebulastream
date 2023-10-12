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
#include <Common/Spatial/Point.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class PointTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PointTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PointTest test class.");
    }
};
// This tests the point datatype
TEST_F(PointTest, testPointDatatype) {

    // create the test point
    auto point = Point(52.5138, 13.3265);
    auto defaultPoint = Point();

    // create other points for various comparison operator tests
    auto pointEqual = Point(52.5138, 13.3265);
    auto pointNotEqual1 = Point(52.5137, 13.3265);
    auto pointNotEqual2 = Point(52.5138, 13.3269);
    auto pointNotEqual3 = Point(52.5137, 13.3269);
    auto pointLessThan = Point(52.5137, 13.3264);
    auto pointGreaterThan = Point(52.5139, 13.3267);

    // test comparison operators
    // test == operator
    EXPECT_TRUE(point == pointEqual);
    EXPECT_FALSE(point == pointNotEqual1);
    EXPECT_FALSE(point == pointNotEqual2);
    EXPECT_FALSE(point == pointNotEqual3);

    // test != operator
    EXPECT_FALSE(point != pointEqual);
    EXPECT_TRUE(point != pointNotEqual1);
    EXPECT_TRUE(point != pointNotEqual2);
    EXPECT_TRUE(point != pointNotEqual3);

    // test < operator
    EXPECT_TRUE(point < pointGreaterThan);
    EXPECT_TRUE(pointLessThan < point);
    EXPECT_FALSE(point < pointLessThan);
    EXPECT_FALSE(pointGreaterThan < point);
    EXPECT_FALSE(point < pointNotEqual1);
    EXPECT_FALSE(point < pointNotEqual2);
    EXPECT_FALSE(point < pointNotEqual3);

    // test > operator
    EXPECT_TRUE(point > pointLessThan);
    EXPECT_TRUE(pointGreaterThan > point);
    EXPECT_FALSE(point > pointGreaterThan);
    EXPECT_FALSE(pointLessThan > point);
    EXPECT_FALSE(point > pointNotEqual1);
    EXPECT_FALSE(point > pointNotEqual2);
    EXPECT_FALSE(point > pointNotEqual3);

    // test <= operator
    EXPECT_TRUE(point <= pointGreaterThan);
    EXPECT_TRUE(pointLessThan <= point);
    EXPECT_TRUE(point <= pointEqual);
    EXPECT_TRUE(pointEqual <= point);
    EXPECT_FALSE(point <= pointLessThan);
    EXPECT_FALSE(pointGreaterThan <= point);
    EXPECT_FALSE(point <= pointNotEqual1);
    EXPECT_TRUE(point <= pointNotEqual2);
    EXPECT_FALSE(point <= pointNotEqual3);

    // test >= operator
    EXPECT_FALSE(point >= pointGreaterThan);
    EXPECT_FALSE(pointLessThan >= point);
    EXPECT_TRUE(point >= pointEqual);
    EXPECT_TRUE(pointEqual >= point);
    EXPECT_TRUE(point >= pointLessThan);
    EXPECT_TRUE(pointGreaterThan >= point);
    EXPECT_TRUE(point >= pointNotEqual1);
    EXPECT_FALSE(point >= pointNotEqual2);
    EXPECT_FALSE(point >= pointNotEqual3);

    // test accessor methods
    EXPECT_DOUBLE_EQ(point.getLatitude(), 52.5138);
    EXPECT_DOUBLE_EQ(point.getLongitude(), 13.3265);
    EXPECT_DOUBLE_EQ(defaultPoint.getLatitude(), 0.0);
    EXPECT_DOUBLE_EQ(defaultPoint.getLongitude(), 0.0);

    // test haversine distance
    EXPECT_NEAR(point.haversineDistance(pointGreaterThan), 17.5357, 0.0001);
    EXPECT_DOUBLE_EQ(point.haversineDistance(pointEqual), 0.0);
}

}// namespace NES