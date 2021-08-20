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

#include <utility>
#include <Mobility/Geo/Area/GeoSquare.h>
#include <Mobility/Geo/CartesianPoint.h>
#include <Mobility/Geo/Projection/GeoCalculator.h>
#include <Mobility/Utils/MathUtils.h>

namespace NES {

GeoSquare::GeoSquare(const GeoPointPtr& center,
                     const CartesianPointPtr& cartesianCenter,
                     double area,
                     SquareCartesianEdgesPtr  cartesianEdges,
                     SquareGeoEdgesPtr  geoEdges)
                     : GeoArea(center, cartesianCenter, area), cartesianEdges(std::move(cartesianEdges)), geoEdges(std::move(geoEdges)) {}

const SquareCartesianEdgesPtr& GeoSquare::getCartesianEdges() const { return cartesianEdges; }

const SquareGeoEdgesPtr& GeoSquare::getGeoEdges() const { return geoEdges; }

double GeoSquare::getDistanceToBound() const { return std::sqrt(this->getArea()) / 2; }

bool GeoSquare::contains(const GeoAreaPtr& area) {
    if (area->isCircle()) {
        double x = MathUtils::clamp(area->getCenter()->getLongitude(), cartesianEdges->getSouthWest()->getX(), cartesianEdges->getSouthEast()->getX());
        double y = MathUtils::clamp(area->getCenter()->getLatitude(), cartesianEdges->getSouthWest()->getY(), cartesianEdges->getNorthWest()->getY());
        CartesianPointPtr ref = std::make_shared<CartesianPoint>(x, y);
        return (MathUtils::distance(area->getCartesianCenter(), ref) <= area->getDistanceToBound());
    }
    return false;
}

bool GeoSquare::contains(const GeoPointPtr& location) {
    if (location->getLongitude() < geoEdges->getSouthWest()->getLongitude()) {
        return false;
    }

    if (location->getLatitude() < geoEdges->getSouthWest()->getLatitude()) {
        return false;
    }

    if (location->getLongitude() > geoEdges->getNorthEast()->getLongitude()) {
        return false;
    }

    if (location->getLatitude() > geoEdges->getNorthEast()->getLatitude()) {
        return false;
    }

    return true;
}

bool GeoSquare::isCircle() { return false; }

bool GeoSquare::isSquare() { return true; }

GeoSquare::~GeoSquare() = default;

}
