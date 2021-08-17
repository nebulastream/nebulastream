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

LocationService::LocationService() { locationCatalog = std::make_shared<LocationCatalog>(); }

void LocationService::addSink(const string& nodeId, const double movingRangeArea) {
    this->locationCatalog->addSink(nodeId, movingRangeArea);
}

void LocationService::addSource(const string& nodeId) { this->locationCatalog->addSource(nodeId); }

bool LocationService::checkIfPointInRange(const GeoPointPtr& location) {
    for (auto const& [nodeId, sink] : locationCatalog->getSinks()){
        if (sink->getMovingRange()->contains(location)) {
            return true;
        }
    }
    return false;
}

void LocationService::updateNodeLocation(const string& nodeId, const GeoPointPtr& location) {
    this->locationCatalog->updateNodeLocation(nodeId, location);
}

const LocationCatalogPtr& LocationService::getLocationCatalog() const { return locationCatalog; }

LocationServicePtr LocationService::getInstance() {
    if (instance == nullptr) {
        instance = std::make_shared<LocationService>();
    }
    return instance;
}

}
