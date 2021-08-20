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

#include <Mobility/Geo/Projection/Wgs84Projection.h>
#include <Mobility/Utils/MathUtils.h>

namespace NES {

Wgs84Projection::Wgs84Projection(GeoPointPtr  origin) : origin(std::move(origin)) {}

GeoPointPtr Wgs84Projection::cartesianToGeographic(const CartesianPointPtr& cartesianPoint) {
    double radius = cos(MathUtils::toRadians(origin->getLatitude())) * WS_84_EQUATORIAL_RADIUS;
    double scaleY = (M_PI * 2 * WS_84_EQUATORIAL_RADIUS) / TOTAL_DEGREES;
    double scaleX = (M_PI * 2 * radius) / TOTAL_DEGREES;

    double latitude = origin->getLatitude() + cartesianPoint->getY() / scaleY;
    double longitude = origin->getLongitude() + cartesianPoint->getX() / scaleX;

    double latSafe = MathUtils::clamp(latitude, -90.0, 90.0);
    double longSafe = MathUtils::toDegrees(MathUtils::wrapAnglePiPi(MathUtils::toRadians(longitude)));

    return std::make_shared<GeoPoint>(latSafe, longSafe);
}

CartesianPointPtr Wgs84Projection::geographicToCartesian(const GeoPointPtr& geoPoint) {
    double radius = std::cos(MathUtils::toRadians(origin->getLatitude())) * WS_84_EQUATORIAL_RADIUS;
    double scaleY = (M_PI * 2 * WS_84_EQUATORIAL_RADIUS) / TOTAL_DEGREES;
    double scaleX = (M_PI * 2 * radius) / TOTAL_DEGREES;
    double dY = (geoPoint->getLatitude() - origin->getLatitude()) * scaleY;
    double dX = (geoPoint->getLongitude() - origin->getLongitude()) * scaleX;

    return std::make_shared<CartesianPoint>(dX, dY);
}

}
