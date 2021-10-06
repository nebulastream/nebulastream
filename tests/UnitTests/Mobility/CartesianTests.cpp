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
#include <Mobility/Utils/MathUtils.h>

namespace NES {

TEST(CartesianTest, CartesianLine) {
    const uint32_t gradient  = 2;
    const uint32_t constant  = 1;

    CartesianLinePtr line = std::make_shared<CartesianLine>(gradient, constant);

    CartesianPointPtr inlinePoint = std::make_shared<CartesianPoint>(1, 3);
    EXPECT_TRUE(line->contains(inlinePoint));

    CartesianPointPtr outsidePoint = std::make_shared<CartesianPoint>(1, 2);
    EXPECT_FALSE(line->contains(outsidePoint));
}

TEST(CartesianUtils, ShiftCartesianLine) {
    const uint32_t gradient  = 2;
    const uint32_t constant  = 1;

    CartesianLinePtr line = std::make_shared<CartesianLine>(gradient, constant);
    CartesianLinePtr shiftedLine = MathUtils::shift(line, -2, -3);

    EXPECT_DOUBLE_EQ(shiftedLine->getGradient(), line->getGradient());
    EXPECT_DOUBLE_EQ(shiftedLine->getConstant(), 2);

    line->setGradient(-1);
    line->setConstant(0);
    shiftedLine = MathUtils::shift(line, 5, -3);
    EXPECT_DOUBLE_EQ(shiftedLine->getGradient(), line->getGradient());
    EXPECT_DOUBLE_EQ(shiftedLine->getConstant(), 2);
}

TEST(CartesianUtils, InterstCircleAndLine) {
    const uint32_t gradient  = 2;
    const uint32_t constant  = 1;
    const CartesianPointPtr center = std::make_shared<CartesianPoint>(2,3);
    CartesianLinePtr line = std::make_shared<CartesianLine>(gradient, constant);
    CartesianCirclePtr circle = std::make_shared<CartesianCircle>(center, 2);
    EXPECT_TRUE(MathUtils::intersect(line, circle));

    line->setConstant(5);
    EXPECT_FALSE(MathUtils::intersect(line, circle));
}

}