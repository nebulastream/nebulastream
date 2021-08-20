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


//#define _USE_MATH_DEFINES
#include <cmath>

#include <Mobility/Geo/Projection/GeoCalculator.h>
#include "Mobility/Utils/MathUtils.h"

#define TOTAL_DEGREES 360
#define WS_84_EQUATORIAL_RADIUS  6378137
#define BERLIN_ORIGIN_LAT 52.5
#define BERLIN_ORIGIN_LONG 13.4

namespace NES {

Wgs84Projection GeoCalculator::projection(std::make_shared<GeoPoint>(BERLIN_ORIGIN_LAT, BERLIN_ORIGIN_LONG));

GeoPointPtr GeoCalculator::pointFromDirection(const GeoPointPtr& source, GeoPoint direction) {
    double radius = cos(MathUtils::toRadians(source->getLatitude())) * WS_84_EQUATORIAL_RADIUS;
    double scaleY = (M_PI * 2 * WS_84_EQUATORIAL_RADIUS) / TOTAL_DEGREES;
    double scaleX = (M_PI * 2 * radius) / TOTAL_DEGREES;

    double latitude = source->getLatitude() - direction.getLatitude() / scaleY;
    double longitude = source->getLongitude() + direction.getLongitude() / scaleX;

    double latSafe = MathUtils::clamp(latitude, -90.0, 90.0);
    double longSafe = MathUtils::toDegrees(MathUtils::wrapAnglePiPi(MathUtils::toRadians(longitude)));

    return std::make_shared<GeoPoint>(latSafe, longSafe);
}
GeoPointPtr GeoCalculator::cartesianToGeographic(const CartesianPointPtr& cartesianPoint) {
    return projection.cartesianToGeographic(cartesianPoint);
}
CartesianPointPtr GeoCalculator::geographicToCartesian(const GeoPointPtr& geoPoint) {
    return projection.geographicToCartesian(geoPoint);
}

}
