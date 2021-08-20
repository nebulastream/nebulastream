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

#ifndef NES_WGS84PROJECTION_H
#define NES_WGS84PROJECTION_H

#include <Mobility/Geo/CartesianPoint.h>
#include <Mobility/Geo/GeoPoint.h>

#define TOTAL_DEGREES 360
#define WS_84_EQUATORIAL_RADIUS  6378137

namespace NES {

class Wgs84Projection {

  private:
    GeoPointPtr origin;

  public:
    explicit Wgs84Projection(GeoPointPtr  origin);

    GeoPointPtr  cartesianToGeographic(const CartesianPointPtr& cartesianPoint);
    CartesianPointPtr geographicToCartesian(const GeoPointPtr& geoPoint);
};

}

#endif//NES_WGS84PROJECTION_H
