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

#include <Mobility/Utils//MathUtils.h>

#define _USE_MATH_DEFINES
#define CONVERSION_DEGREES 180
#include <cmath>
#include <limits>

namespace NES {

double MathUtils::toDegrees(double radians) {
    return ( radians * CONVERSION_DEGREES ) / M_PI ;
}

double MathUtils::toRadians(double degrees) {
    return ( degrees * M_PI ) / CONVERSION_DEGREES;
}

double MathUtils::clamp(double d, double min, double max) {
    if (d > max) { return max; }
    if (d < min) { return min; }

    return d;
}

double MathUtils::distance(const CartesianPointPtr& p1, const CartesianPointPtr& p2) {
    double dx = p2->getX() - p1->getX();
    double dy = p2->getY() - p1->getY();
    return std::sqrt(dx * dx + dy * dy);
}

double MathUtils::wrapAnglePiPi(double a) {
    if (a > M_PI) {
        int mul = (int) (a / M_PI) + 1;
        a -= M_PI * mul;
    } else if (a < -M_PI) {
        int mul = (int) (a / M_PI) - 1;
        a -= M_PI * mul;
    }

    return a;
}

bool MathUtils::intersect(const CartesianLinePtr& line, const CartesianCirclePtr& circle) {
    double offsetX = -circle->getCenter()->getX();
    double offsetY = -circle->getCenter()->getY();

    CartesianLinePtr shiftedLine = MathUtils::shift(line, offsetX, offsetY);
    CartesianCirclePtr shiftedCircle = std::make_shared<CartesianCircle>(circle->getRadius());

    double x1 = std::numeric_limits<double>::min() / std::pow(100000, 50);
    double y1 = shiftedLine->getY(x1);
    double x2 = std::numeric_limits<double>::max() / std::pow(100000, 50);
    double y2 = shiftedLine->getY(x2);

    double dx = x2 - x1;
    double dy = y2 - y1;
    double dr = std::sqrt(dx * dx + dy * dy);
    double D = x1 * y2 - x2 * y1;

    double discriminant = circle->getRadius() * circle->getRadius() * dr * dr - D * D;
    return discriminant >= 0;
}

CartesianLinePtr MathUtils::shift(const CartesianLinePtr& line, double offsetX, double offsetY) {
    double x = offsetX;
    double y = line->getConstant() + offsetY;
    double shiftedConstant = y - line->getGradient() * x;
    return std::make_shared<CartesianLine>(line->getGradient(), shiftedConstant);
}

}
