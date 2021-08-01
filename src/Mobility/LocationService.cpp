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

#include <Mobility/LocationCatalog.h>

#include <utility>
#include "Mobility/LocationService.h"
#include "Mobility/Geo/GeoSquare.h"

namespace NES::Mobility {

const LocationCatalog& LocationService::getLocationCatalog() const { return locationCatalog; }


void LocationService::addNode(int nodeId) {
    this->locationCatalog.addNode(nodeId);
}

bool LocationService::checkIfPointInRange(int nodeId, double area, GeoPoint location) {

    if (this->locationCatalog.contains(nodeId)) {
        GeoNode node = this->locationCatalog.getNode(nodeId);
        GeoSquare range(node.getCurrentLocation(), area);
        return range.contains(location);
    }

    return false;
}

void LocationService::updateNodeLocation(int nodeId, GeoPoint location) {
    this->locationCatalog.updateNodeLocation(nodeId, location);
}

}
