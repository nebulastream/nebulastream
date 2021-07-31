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

#include <Catalogs/GeoLocation.h>
#include "Services/GeoSquare.h"
#include "Services/GeoCalculator.h"

NES::GeoSquare::GeoSquare(const NES::GeoLocation& center, double area) : center(center), area(area) {}

double NES::GeoSquare::getDistanceToBound() const { return this->area / 4; }

const NES::GeoLocation& NES::GeoSquare::getCenter() const { return center; }

NES::GeoLocation NES::GeoSquare::getNorthBound() {
    return Geo::GeoCalculator::pointFromDirection(center, GeoLocation( - getDistanceToBound(), 0));
}

NES::GeoLocation NES::GeoSquare::getSouthBound() {
    return Geo::GeoCalculator::pointFromDirection(center, GeoLocation(  getDistanceToBound(), 0));
}

NES::GeoLocation NES::GeoSquare::getEastBound() {
    return Geo::GeoCalculator::pointFromDirection(center, GeoLocation(0, getDistanceToBound()));
}

NES::GeoLocation NES::GeoSquare::getWestBound() {
    return Geo::GeoCalculator::pointFromDirection(center, GeoLocation(0, - getDistanceToBound()));
}

bool NES::GeoSquare::contains(NES::GeoLocation location) {

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
