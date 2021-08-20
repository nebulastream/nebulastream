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

#include <Mobility/Geo/Area/GeoAreaFactory.h>
#include <Mobility/Geo/Projection/GeoCalculator.h>

namespace NES {

GeoCirclePtr GeoAreaFactory::createCircle(const GeoPointPtr& center, double area) {
    CartesianPointPtr cartesianCenter = GeoCalculator::geographicToCartesian(center);
    return std::make_shared<GeoCircle>(center, cartesianCenter, area);
}

GeoSquarePtr GeoAreaFactory::createSquare(const GeoPointPtr& center, double area) {
    CartesianPointPtr cartesianCenter = GeoCalculator::geographicToCartesian(center);

    double distanceToBound = std::sqrt(area) / 2;
    CartesianPointPtr northWest = std::make_shared<CartesianPoint>(cartesianCenter->getX() - distanceToBound, cartesianCenter->getY() + distanceToBound);
    CartesianPointPtr northEast = std::make_shared<CartesianPoint>(cartesianCenter->getX() + distanceToBound, cartesianCenter->getY() + distanceToBound);
    CartesianPointPtr southWest = std::make_shared<CartesianPoint>(cartesianCenter->getX() - distanceToBound, cartesianCenter->getY() - distanceToBound);
    CartesianPointPtr southEast = std::make_shared<CartesianPoint>(cartesianCenter->getX() + distanceToBound, cartesianCenter->getY() - distanceToBound);

    SquareCartesianEdgesPtr cartesianEdges = std::make_shared<SquareEdges<CartesianPointPtr>>(northWest, northEast, southWest, southEast);
    SquareGeoEdgesPtr geoEdges = std::make_shared<SquareEdges<GeoPointPtr>>(
        GeoCalculator::cartesianToGeographic(northWest), GeoCalculator::cartesianToGeographic(northEast),
        GeoCalculator::cartesianToGeographic(southWest), GeoCalculator::cartesianToGeographic(southEast));

    return std::make_shared<GeoSquare>(center, cartesianCenter, area, cartesianEdges, geoEdges);
}

}
