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
#include <Common/Spatial/Point.hpp>
#include <Common/Spatial/Polygon.hpp>
#include <Common/Spatial/Rectangle.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class PolygonTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PolygonTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PolygonTest test class.");
    }
};

// This tests the polygon datatype
TEST_F(PolygonTest, testPolygonDatatype) {

    // create the test polygon
    auto polygon = Polygon({29.3920780354312,
                            79.43787034746113,
                            29.385534338173525,
                            79.44980081315937,
                            29.379401576991956,
                            79.45246156450214,
                            29.378335785066195,
                            79.46218188997211,
                            29.382729767644843,
                            79.46808274980488,
                            29.3897597451206,
                            79.46645196672382,
                            29.397649194459948,
                            79.4595855116457,
                            29.398434366832806,
                            79.45074495073261,
                            29.398696089609903,
                            79.44231208558979});
    std::vector coords = std::vector({29.3920780354312,
                                      79.43787034746113,
                                      29.385534338173525,
                                      79.44980081315937,
                                      29.379401576991956,
                                      79.45246156450214,
                                      29.378335785066195,
                                      79.46218188997211,
                                      29.382729767644843,
                                      79.46808274980488,
                                      29.3897597451206,
                                      79.46645196672382,
                                      29.397649194459948,
                                      79.4595855116457,
                                      29.398434366832806,
                                      79.45074495073261,
                                      29.398696089609903,
                                      79.44231208558979});

    // create other shapes for testing predicates
    auto pointInside = Point(29.386844738083383, 79.4592465137537);
    auto pointInside1 = Point(29.390722779483202, 79.45436689251822);
    auto pointOutside = Point(29.354226572424423, 79.47221005281747);
    auto rectangleInside = Rectangle(29.38949962608978, 79.45333492508487, 29.391490746626044, 79.45564162483768);
    auto rectangleOutside = Rectangle(29.37000238301592, 79.46573058288895, 29.37306909298983, 79.47092333954178);

    // test predicates
    EXPECT_TRUE(polygon.contains(pointInside));
    EXPECT_TRUE(polygon.contains(pointInside1));
    EXPECT_FALSE(polygon.contains(pointOutside));
    EXPECT_TRUE(polygon.contains(pointInside.getLatitude(), pointInside.getLongitude()));
    EXPECT_TRUE(polygon.contains(pointInside1.getLatitude(), pointInside1.getLongitude()));
    EXPECT_FALSE(polygon.contains(pointOutside.getLatitude(), pointOutside.getLongitude()));
    EXPECT_TRUE(polygon.contains(rectangleInside));
    EXPECT_FALSE(polygon.contains(rectangleOutside));

    // test accessor methods
    for (size_t itr = 0; itr < coords.size() && itr + 1 < coords.size(); itr = itr + 2) {
        Point p = Point(coords[itr], coords[itr + 1]);
        Point p1 = polygon.getVertex(itr / 2);
        EXPECT_TRUE(p == p1);
    }

    // access out of bounds vertices
    EXPECT_THROW(polygon.getVertex(-1), InvalidArgumentException);
    EXPECT_THROW(polygon.getVertex(coords.size()), InvalidArgumentException);
}

}// namespace NES