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

#ifndef NES_MATHUTILS_H
#define NES_MATHUTILS_H

#include <Mobility/Geo/CartesianPoint.h>
#include <Mobility/Geo/GeoPoint.h>
#include <Mobility/Geo/Cartesian/CartesianLine.h>
#include <Mobility/Geo/Cartesian/CartesianCircle.h>

namespace NES {

class MathUtils {
  public:
    static double toDegrees(double radians);
    static double toRadians(double degrees);
    static double clamp(double d, double min, double max);
    static double distance(const CartesianPointPtr& p1, const CartesianPointPtr& p2);
    static double wrapAnglePiPi(double a);

    static bool intersect(const CartesianLinePtr& line, const CartesianCirclePtr& circle);
    static CartesianLinePtr shift(const CartesianLinePtr& line, double offsetX, double offsetY);
    static CartesianLinePtr leastSquaresRegression(const std::vector<CartesianPointPtr>& points);
};

}

#endif//NES_MATHUTILS_H