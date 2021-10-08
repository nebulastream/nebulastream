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

#include <cmath>
#include <Mobility/Geo/Area/GeoCircle.h>
#include <Mobility/Geo/GeoPoint.h>

namespace NES {

GeoCircle::GeoCircle(const GeoPointPtr& center, const CartesianPointPtr& cartesianCenter, double area)
: GeoArea(center, cartesianCenter, area) {}

double GeoCircle::getDistanceToBound() const { return std::sqrt(getArea() / M_PI); }

bool GeoCircle::contains(const GeoPointPtr& location) {
    // (x - center_x)^2 + (y - center_y)^2 < radius^2
    double r = getDistanceToBound();
    double latDistance = std::pow(location->getLatitude() - getCenter()->getLatitude(), 2);
    double longDistance = std::pow(location->getLongitude() - getCenter()->getLongitude(), 2);
    return (longDistance + latDistance) < std::pow(r, 2);
}

bool GeoCircle::contains(const GeoAreaPtr& area) { return area->isCircle(); }

bool GeoCircle::isCircle() { return true; }

bool GeoCircle::isSquare() { return false; }

GeoCircle::~GeoCircle() = default;

}// namespace NES
