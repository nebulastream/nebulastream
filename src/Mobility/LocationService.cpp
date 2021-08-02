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

#include <Mobility/LocationCatalog.h>
#include "Mobility/Geo/GeoSquare.h"
#include "Mobility/LocationService.h"
#include <Util/Logger.hpp>

namespace NES {

LocationService::LocationService() {
    locationCatalog = std::make_shared<LocationCatalog>();
}


void LocationService::addNode(string nodeId) {
    this->locationCatalog->addNode(std::move(nodeId));
}

bool LocationService::checkIfPointInRange(const string& nodeId, double area, const GeoPointPtr& location) {

    if (this->locationCatalog->contains(nodeId)) {
        GeoNodePtr node = this->locationCatalog->getNode(nodeId);
        GeoSquare range(node->getCurrentLocation(), area);
        return range.contains(location);
    }

    return false;
}

void LocationService::updateNodeLocation(string nodeId, const GeoPointPtr& location) {
    this->locationCatalog->updateNodeLocation(std::move(nodeId), location);
}

LocationServicePtr LocationService::create() { return std::make_shared<LocationService>(); }

const LocationCatalogPtr& LocationService::getLocationCatalog() const { return locationCatalog; }

}
