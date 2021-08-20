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
    return std::sqrt(std::pow(p2->getX() - p1->getX(), 2) + std::sqrt(std::pow(p2->getY() - p2->getY(), 2)));
}

double MathUtils::distance(const GeoPointPtr& p1, const GeoPointPtr& p2) {
    return std::sqrt(std::pow(p2->getLongitude() - p1->getLongitude(), 2) + std::sqrt(std::pow(p2->getLatitude() - p2->getLatitude(), 2)));
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

}
