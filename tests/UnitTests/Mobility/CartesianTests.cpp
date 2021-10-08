/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include "gtest/gtest.h"

#include <Mobility/Geo/Cartesian/CartesianLine.h>
#include <Mobility/Geo/Node/GeoSink.h>
#include <Mobility/Geo/Projection/GeoCalculator.h>
#include <Mobility/Utils/MathUtils.h>

namespace NES {

TEST(CartesianTest, CartesianLine) {
    const uint32_t slope  = 2;
    const uint32_t intercept  = 1;

    CartesianLinePtr line = std::make_shared<CartesianLine>(slope, intercept);

    CartesianPointPtr inlinePoint = std::make_shared<CartesianPoint>(1, 3);
    EXPECT_TRUE(line->contains(inlinePoint));

    CartesianPointPtr outsidePoint = std::make_shared<CartesianPoint>(1, 2);
    EXPECT_FALSE(line->contains(outsidePoint));
}

TEST(CartesianUtils, ShiftCartesianLine) {
    const uint32_t slope  = 2;
    const uint32_t intercept  = 1;

    CartesianLinePtr line = std::make_shared<CartesianLine>(slope, intercept);
    CartesianLinePtr shiftedLine = MathUtils::shift(line, -2, -3);

    EXPECT_DOUBLE_EQ(shiftedLine->getSlope(), line->getSlope());
    EXPECT_DOUBLE_EQ(shiftedLine->getIntercept(), 2);

    line->setSlope(-1);
    line->setIntercept(0);
    shiftedLine = MathUtils::shift(line, 5, -3);
    EXPECT_DOUBLE_EQ(shiftedLine->getSlope(), line->getSlope());
    EXPECT_DOUBLE_EQ(shiftedLine->getIntercept(), 2);
}

TEST(CartesianUtils, InterstCircleAndLine) {
    const uint32_t slope  = 2;
    const uint32_t intercept  = 1;
    const CartesianPointPtr center = std::make_shared<CartesianPoint>(2,3);
    CartesianLinePtr line = std::make_shared<CartesianLine>(slope, intercept);
    CartesianCirclePtr circle = std::make_shared<CartesianCircle>(center, 2);
    EXPECT_TRUE(MathUtils::intersect(line, circle));

    line->setIntercept(5);
    EXPECT_FALSE(MathUtils::intersect(line, circle));
}

TEST(CartesianUtils, LeastSquaresRegression) {
    std::vector<CartesianPointPtr> points;
    points.push_back(std::make_shared<CartesianPoint>(1, 6));
    points.push_back(std::make_shared<CartesianPoint>(2, 5));
    points.push_back(std::make_shared<CartesianPoint>(3, 7));
    points.push_back(std::make_shared<CartesianPoint>(4, 10));

    CartesianLinePtr predictedLine = MathUtils::leastSquaresRegression(points);
    EXPECT_DOUBLE_EQ(1.4, predictedLine->getSlope());
    EXPECT_DOUBLE_EQ(3.5, predictedLine->getIntercept());
}

TEST(GeoSink, GenerateTrajectory) {
    GeoSinkPtr sink = std::make_shared<GeoSink>("test_sink", 500, 4);
    sink->setCurrentLocation(GeoCalculator::cartesianToGeographic(std::make_shared<CartesianPoint>(1, 6)));
    sink->setCurrentLocation(GeoCalculator::cartesianToGeographic(std::make_shared<CartesianPoint>(2, 5)));
    sink->setCurrentLocation(GeoCalculator::cartesianToGeographic(std::make_shared<CartesianPoint>(3, 7)));
    sink->setCurrentLocation(GeoCalculator::cartesianToGeographic(std::make_shared<CartesianPoint>(4, 10)));

    CartesianLinePtr trajectory = MathUtils::leastSquaresRegression(sink->getLocationHistory()->getCartesianPoints());
    EXPECT_DOUBLE_EQ(1.4000000001626063, trajectory->getSlope());
    EXPECT_DOUBLE_EQ(3.4999999996375708, trajectory->getIntercept());
}

}
