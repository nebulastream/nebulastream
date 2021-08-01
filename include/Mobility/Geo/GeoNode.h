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

#ifndef NES_GEONODE_H
#define NES_GEONODE_H

#include <vector>

#include "Mobility/Geo/GeoPoint.h"

namespace NES::Mobility {

class GeoNode {
  private:
    int id;
    GeoPoint currentLocation;
    std::vector<GeoPoint> locationHistory;

  public:
    explicit GeoNode(int id);
    [[nodiscard]] int getId() const;
    [[nodiscard]] GeoPoint& getCurrentLocation();
    [[nodiscard]] const std::vector<GeoPoint>& getLocationHistory() const;

    void setCurrentLocation(GeoPoint location);
};

}

#endif//NES_GEONODE_H
