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
#include <BaseIntegrationTest.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/CircleExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PointExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PolygonExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/RectangleExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <gtest/gtest.h>

namespace NES {

class SpatialExpressionNodeTest : public Testing::BaseUnitTest {
  protected:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SpatialExpressionNodeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SpatialExpressionNodeTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down SpatialExpressionNodeTest test class."); }
};

// This tests the circle expression node
TEST_F(SpatialExpressionNodeTest, testCircleExpressionNode) {

    // create a circle expression node
    auto circleShapeExpressionNode = CircleExpressionNode::create(52.5153, 13.3267, 100.0);
    auto circleExpressionNode = circleShapeExpressionNode->as<CircleExpressionNode>();

    // check shape type
    EXPECT_EQ(circleShapeExpressionNode->getShapeType(), ShapeType::Circle);

    // check returned values by accessor methods
    EXPECT_EQ(circleExpressionNode->getLatitude(), 52.5153);
    EXPECT_EQ(circleExpressionNode->getLongitude(), 13.3267);
    EXPECT_EQ(circleExpressionNode->getRadius(), 100.0);
    EXPECT_EQ(circleExpressionNode->toString(), "CIRCLE(lat: 52.5153, lon: 13.3267, radius: 100)");

    // create a copy of the circle
    auto copyCircleShapeExpressionNode = circleExpressionNode->copy();
    auto copyCircleExpressionNode = copyCircleShapeExpressionNode->as<CircleExpressionNode>();

    // check for equality with the copy
    EXPECT_TRUE(circleExpressionNode->equal(copyCircleShapeExpressionNode));

    // create various circle nodes that differ in latitude, longitude, and radius to
    // the original circleShapeExpressionNode
    auto circleShapeExpressionNode1 = CircleExpressionNode::create(52.5152, 13.3267, 100.0);// different latitude
    auto circleShapeExpressionNode2 = CircleExpressionNode::create(52.5153, 13.3268, 100.0);// different longitude
    auto circleShapeExpressionNode3 = CircleExpressionNode::create(52.5153, 13.3267, 101.0);// different radius

    // check for non-equality of the circles that differ in latitude, longitude, and
    // radius to the original circleShapeExpressionNode
    EXPECT_FALSE(circleExpressionNode->equal(circleShapeExpressionNode1));
    EXPECT_FALSE(circleExpressionNode->equal(circleShapeExpressionNode2));
    EXPECT_FALSE(circleExpressionNode->equal(circleShapeExpressionNode3));

    // use the explicit constructor and check for equality
    auto circleShapeExpressionNode4 = std::make_shared<CircleExpressionNode>(CircleExpressionNode(52.5153, 13.3267, 100.0));
    EXPECT_TRUE(circleExpressionNode->equal(circleShapeExpressionNode4));
}

// This tests the point expression node
TEST_F(SpatialExpressionNodeTest, testPointExpressionNode) {

    // create a Point expression node
    auto pointShapeExpressionNode = PointExpressionNode::create(52.5153, 13.3267);
    auto pointExpressionNode = pointShapeExpressionNode->as<PointExpressionNode>();

    // check shape type
    EXPECT_EQ(pointShapeExpressionNode->getShapeType(), ShapeType::Point);

    // check returned values by accessor methods
    EXPECT_EQ(pointExpressionNode->getLatitude(), 52.5153);
    EXPECT_EQ(pointExpressionNode->getLongitude(), 13.3267);
    EXPECT_EQ(pointExpressionNode->toString(), "POINT(lat: 52.5153, lon: 13.3267)");

    // create a copy of the Point
    auto copyPointShapeExpressionNode = pointExpressionNode->copy();
    auto copyPointExpressionNode = copyPointShapeExpressionNode->as<PointExpressionNode>();

    // check for equality with the copy
    EXPECT_TRUE(pointExpressionNode->equal(copyPointShapeExpressionNode));

    // create various Point nodes that differ in latitude, and longitude to
    // the original pointShapeExpressionNode
    auto PointShapeExpressionNode1 = PointExpressionNode::create(52.5152, 13.3267);// different latitude
    auto PointShapeExpressionNode2 = PointExpressionNode::create(52.5153, 13.3268);// different longitude

    // check for non-equality of the Points that differ in latitude and longitude
    // to the original PointShapeExpressionNode
    EXPECT_FALSE(pointExpressionNode->equal(PointShapeExpressionNode1));
    EXPECT_FALSE(pointExpressionNode->equal(PointShapeExpressionNode2));

    // use the explicit constructor and check for equality
    auto pointShapeExpressionNode4 = std::make_shared<PointExpressionNode>(PointExpressionNode(52.5153, 13.3267));
    EXPECT_TRUE(pointExpressionNode->equal(pointShapeExpressionNode4));
}

// This tests the polygon expression node
TEST_F(SpatialExpressionNodeTest, testPolygonExpressionNode) {

    // create a Polygon expression node
    auto polygonShapeExpressionNode =
        PolygonExpressionNode::create({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto polygonExpressionNode = polygonShapeExpressionNode->as<PolygonExpressionNode>();

    std::vector<double> coordinates{52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274};

    // check shape type
    EXPECT_EQ(polygonShapeExpressionNode->getShapeType(), ShapeType::Polygon);

    // check returned values by accessor methods
    EXPECT_EQ(polygonExpressionNode->getCoordinates(), coordinates);
    EXPECT_EQ(polygonExpressionNode->toString(),
              "POLYGON(52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274)");

    // create a copy of the Polygon
    auto copyPolygonShapeExpressionNode = polygonExpressionNode->copy();
    auto copyPolygonExpressionNode = copyPolygonShapeExpressionNode->as<PolygonExpressionNode>();

    // check for equality with the copy
    EXPECT_TRUE(polygonExpressionNode->equal(copyPolygonShapeExpressionNode));

    // create various Polygon nodes that differ in some latitude and longitude values to
    // the original polygonShapeExpressionNode
    auto PolygonShapeExpressionNode1 =
        PolygonExpressionNode::create({52.5156, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto PolygonShapeExpressionNode2 =
        PolygonExpressionNode::create({52.5155, 13.3263, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto PolygonShapeExpressionNode3 =
        PolygonExpressionNode::create({52.5155, 13.3262, 52.5146, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto PolygonShapeExpressionNode4 =
        PolygonExpressionNode::create({52.5155, 13.3262, 52.5145, 13.3266, 52.5138, 13.3267, 52.5148, 13.3274});
    auto PolygonShapeExpressionNode5 =
        PolygonExpressionNode::create({52.5155, 13.3262, 52.5145, 13.3264, 52.5134, 13.3267, 52.5148, 13.3274});
    auto PolygonShapeExpressionNode6 =
        PolygonExpressionNode::create({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3268, 52.5148, 13.3274});
    auto PolygonShapeExpressionNode7 =
        PolygonExpressionNode::create({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5143, 13.3274});
    auto PolygonShapeExpressionNode8 =
        PolygonExpressionNode::create({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3271});

    // check for non-equality of the Polygons that differ in some latitude and longitude
    // values to the original polygonShapeExpressionNode
    EXPECT_FALSE(polygonExpressionNode->equal(PolygonShapeExpressionNode1));
    EXPECT_FALSE(polygonExpressionNode->equal(PolygonShapeExpressionNode2));
    EXPECT_FALSE(polygonExpressionNode->equal(PolygonShapeExpressionNode3));
    EXPECT_FALSE(polygonExpressionNode->equal(PolygonShapeExpressionNode4));
    EXPECT_FALSE(polygonExpressionNode->equal(PolygonShapeExpressionNode5));
    EXPECT_FALSE(polygonExpressionNode->equal(PolygonShapeExpressionNode6));
    EXPECT_FALSE(polygonExpressionNode->equal(PolygonShapeExpressionNode7));
    EXPECT_FALSE(polygonExpressionNode->equal(PolygonShapeExpressionNode8));

    // use the explicit constructors to create a duplicate and check for equality
    auto polygonShapeExpressionNode9 = std::make_shared<PolygonExpressionNode>(
        PolygonExpressionNode({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274}));
    EXPECT_TRUE(polygonExpressionNode->equal(polygonShapeExpressionNode9));
    auto polygonShapeExpressionNode10 = std::make_shared<PolygonExpressionNode>(PolygonExpressionNode(coordinates));
    EXPECT_TRUE(polygonExpressionNode->equal(polygonShapeExpressionNode10));

    // use the other create constructors to create a duplicate and check for equality
    auto polygonShapeExpressionNode11 = PolygonExpressionNode::create(coordinates);
    EXPECT_TRUE(polygonExpressionNode->equal(polygonShapeExpressionNode11));
}

// This tests the rectangle expression node
TEST_F(SpatialExpressionNodeTest, testRectangleExpressionNode) {

    // create a Rectangle expression node
    auto rectangleShapeExpressionNode = RectangleExpressionNode::create(52.5138, 13.3265, 52.5157, 13.3284);
    auto rectangleExpressionNode = rectangleShapeExpressionNode->as<RectangleExpressionNode>();

    // check shape type
    EXPECT_EQ(rectangleShapeExpressionNode->getShapeType(), ShapeType::Rectangle);

    // check returned values by accessor methods
    EXPECT_EQ(rectangleExpressionNode->getLatitudeLow(), 52.5138);
    EXPECT_EQ(rectangleExpressionNode->getLongitudeLow(), 13.3265);
    EXPECT_EQ(rectangleExpressionNode->getLatitudeHigh(), 52.5157);
    EXPECT_EQ(rectangleExpressionNode->getLongitudeHigh(), 13.3284);
    EXPECT_EQ(rectangleExpressionNode->toString(),
              "RECTANGLE(lat_low: 52.5138, lon_low: 13.3265, lat_high: 52.5157, lon_high: 13.3284)");

    // create a copy of the Rectangle
    auto copyRectangleShapeExpressionNode = rectangleExpressionNode->copy();
    auto copyRectangleExpressionNode = copyRectangleShapeExpressionNode->as<RectangleExpressionNode>();

    // check for equality with the copy
    EXPECT_TRUE(rectangleExpressionNode->equal(copyRectangleShapeExpressionNode));

    // create various Rectangle nodes that differ in lat_low/lat_high, and lon_low/lon_high to
    // the original rectangleShapeExpressionNode
    auto RectangleShapeExpressionNode1 = RectangleExpressionNode::create(52.5139, 13.3265, 52.5157, 13.3284);// different lat_low
    auto RectangleShapeExpressionNode2 = RectangleExpressionNode::create(52.5138, 13.3269, 52.5157, 13.3284);// different lon_low
    auto RectangleShapeExpressionNode3 = RectangleExpressionNode::create(52.5138, 13.3265, 52.5158, 13.3284);// different lat_high
    auto RectangleShapeExpressionNode4 = RectangleExpressionNode::create(52.5138, 13.3265, 52.5157, 13.3285);// different lon_high

    // check for non-equality of the Rectangles that differ in lat_low/lat_high, and
    // lon_low/lon_high to the original RectangleShapeExpressionNode
    EXPECT_FALSE(rectangleExpressionNode->equal(RectangleShapeExpressionNode1));
    EXPECT_FALSE(rectangleExpressionNode->equal(RectangleShapeExpressionNode2));
    EXPECT_FALSE(rectangleExpressionNode->equal(RectangleShapeExpressionNode3));
    EXPECT_FALSE(rectangleExpressionNode->equal(RectangleShapeExpressionNode4));

    // use the explicit constructor to create a duplicate and check for equality
    auto rectangleShapeExpressionNode4 =
        std::make_shared<RectangleExpressionNode>(RectangleExpressionNode(52.5138, 13.3265, 52.5157, 13.3284));
    EXPECT_TRUE(rectangleExpressionNode->equal(rectangleShapeExpressionNode4));
}

}// namespace NES
