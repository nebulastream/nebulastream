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

#ifndef NES_LOCATIONSERVICE_H
#define NES_LOCATIONSERVICE_H

#include <cpprest/json.h>

#include "Mobility/LocationCatalog.h"

namespace NES::Mobility {

class LocationService {
  private:
    LocationCatalog locationCatalog;

  public:
    LocationService() = default;
    [[nodiscard]] const LocationCatalog& getLocationCatalog() const;
    void addNode(int nodeId);
    void updateNodeLocation(int nodeId, GeoPoint location);
    bool checkIfPointInRange(int nodeId, double area, GeoPoint location);
};

}

#endif//NES_LOCATIONSERVICE_H
