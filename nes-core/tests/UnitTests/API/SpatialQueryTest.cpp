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
#include <Exceptions/InvalidArgumentException.hpp>
#include <NesBaseTest.hpp>
#include <Nodes/Expressions/GeographyExpressions/STDWithinExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/STWithinExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/CircleExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/PointExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/PolygonExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/RectangleExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <gtest/gtest.h>

namespace NES {

class SpatialQueryTest : public Testing::TestWithErrorHandling {
  protected:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SpatialQueryTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SpatialQueryTest test class.");
    }
};

// This tests various shape expressions and spatial predicate expressions
TEST_F(SpatialQueryTest, testSpatialQueryExpressions) {

    // create shape expressions
    auto circle = CIRCLE(52.5153, 13.3267, 100.0);
    auto point = POINT(52.5153, 13.3267);
    auto polygon = POLYGON({52.5155, 13.3262, 52.5145, 13.3264, 52.5138, 13.3267, 52.5148, 13.3274});
    auto rectangle = RECTANGLE(52.5138, 13.3265, 52.5157, 13.3284);

    // create spatial predicate expressions
    auto stWithinCircle = ST_WITHIN(Attribute("latitude"), Attribute("longitude"), circle);
    auto stWithinPolygon = ST_WITHIN(Attribute("latitude"), Attribute("longitude"), polygon);
    auto stWithinRectangle = ST_WITHIN(Attribute("latitude"), Attribute("longitude"), rectangle);
    auto stDWithinCircle = ST_DWITHIN(Attribute("latitude"), Attribute("longitude"), circle);

    // test shape expression nodes
    EXPECT_TRUE(circle->instanceOf<CircleExpressionNode>());
    EXPECT_TRUE(point->instanceOf<PointExpressionNode>());
    EXPECT_TRUE(polygon->instanceOf<PolygonExpressionNode>());
    EXPECT_TRUE(rectangle->instanceOf<RectangleExpressionNode>());

    // test spatial predicate nodes
    EXPECT_TRUE(stWithinPolygon->instanceOf<STWithinExpressionNode>());
    EXPECT_TRUE(stWithinRectangle->instanceOf<STWithinExpressionNode>());

    // both st_within with a circle and st_dwithin expressions should return
    // expression of type STDWithinExpressionNode
    EXPECT_TRUE(stWithinCircle->instanceOf<STDWithinExpressionNode>());
    EXPECT_TRUE(stDWithinCircle->instanceOf<STDWithinExpressionNode>());

    // define some invalid field access expressions
    auto invalidLatitudeAccessExpression1 = Attribute("latitude") >= 45.0;
    auto invalidLatitudeAccessExpression2 = Attribute("latitude") < 45.0;
    auto invalidLatitudeAccessExpression3 = Attribute("latitude") + 45.0;

    auto invalidLongitudeAccessExpression1 = Attribute("longitude") >= 90.0;
    auto invalidLongitudeAccessExpression2 = Attribute("longitude") < 90.0;
    auto invalidLongitudeAccessExpression3 = Attribute("longitude") + 90.0;

    // 1. Check that exceptions are thrown for ST_WITHIN expression
    // case 1: invalid latitude expressions
    EXPECT_THROW(ST_WITHIN(invalidLatitudeAccessExpression1, Attribute("longitude"), circle);, InvalidArgumentException);
    EXPECT_THROW(ST_WITHIN(invalidLatitudeAccessExpression2, Attribute("longitude"), circle);, InvalidArgumentException);
    EXPECT_THROW(ST_WITHIN(invalidLatitudeAccessExpression3, Attribute("longitude"), circle);, InvalidArgumentException);

    // case 2: invalid longitude expressions
    EXPECT_THROW(ST_WITHIN(Attribute("latitude"), invalidLongitudeAccessExpression1, circle);, InvalidArgumentException);
    EXPECT_THROW(ST_WITHIN(Attribute("latitude"), invalidLongitudeAccessExpression2, circle);, InvalidArgumentException);
    EXPECT_THROW(ST_WITHIN(Attribute("latitude"), invalidLongitudeAccessExpression3, circle);, InvalidArgumentException);

    // case 3: invalid shape expression in ST_WITHIN
    EXPECT_THROW(ST_WITHIN(Attribute("latitude"), Attribute("longitude"), point);, InvalidArgumentException);

    // 2. Check that exceptions are thrown for ST_WITHIN expression
    // case 1: invalid latitude expressions
    EXPECT_THROW(ST_DWITHIN(invalidLatitudeAccessExpression1, Attribute("longitude"), circle);, InvalidArgumentException);
    EXPECT_THROW(ST_DWITHIN(invalidLatitudeAccessExpression2, Attribute("longitude"), circle);, InvalidArgumentException);
    EXPECT_THROW(ST_DWITHIN(invalidLatitudeAccessExpression3, Attribute("longitude"), circle);, InvalidArgumentException);

    // case 2: invalid longitude expressions
    EXPECT_THROW(ST_DWITHIN(Attribute("latitude"), invalidLongitudeAccessExpression1, circle);, InvalidArgumentException);
    EXPECT_THROW(ST_DWITHIN(Attribute("latitude"), invalidLongitudeAccessExpression2, circle);, InvalidArgumentException);
    EXPECT_THROW(ST_DWITHIN(Attribute("latitude"), invalidLongitudeAccessExpression3, circle);, InvalidArgumentException);

    // case 3: invalid shape expressions in ST_DWITHIN
    EXPECT_THROW(ST_DWITHIN(Attribute("latitude"), Attribute("longitude"), point);, InvalidArgumentException);
    EXPECT_THROW(ST_DWITHIN(Attribute("latitude"), Attribute("longitude"), polygon);, InvalidArgumentException);
    EXPECT_THROW(ST_DWITHIN(Attribute("latitude"), Attribute("longitude"), rectangle);, InvalidArgumentException);
}

}// namespace NES
