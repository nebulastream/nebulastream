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

#include <Mobility/Geo/GeoSquare.h>
#include <Mobility/Geo/GeoCalculator.h>

namespace NES::Mobility {

GeoSquare::GeoSquare(const GeoPoint& center, double area) : center(center), area(area) {}

double GeoSquare::getDistanceToBound() const { return this->area / 4; }

const GeoPoint& GeoSquare::getCenter() const { return center; }

GeoPoint GeoSquare::getNorthBound() {
    return GeoCalculator::pointFromDirection(center, GeoPoint( - getDistanceToBound(), 0));
}

GeoPoint GeoSquare::getSouthBound() {
    return GeoCalculator::pointFromDirection(center, GeoPoint(  getDistanceToBound(), 0));
}

GeoPoint GeoSquare::getEastBound() {
    return GeoCalculator::pointFromDirection(center, GeoPoint(0, getDistanceToBound()));
}

GeoPoint GeoSquare::getWestBound() {
    return GeoCalculator::pointFromDirection(center, GeoPoint(0, - getDistanceToBound()));
}

bool GeoSquare::contains(GeoPoint location) {

    if (location.getLatitude() > getNorthBound().getLatitude()) {
        return false;
    }

    if (location.getLatitude() < getSouthBound().getLatitude()) {
        return false;
    }

    if (location.getLongitude() > getEastBound().getLongitude()) {
        return false;
    }

    if (location.getLongitude() < getWestBound().getLongitude()) {
        return false;
    }

    return true;
}

}
