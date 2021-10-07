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

#include <memory>
#include <utility>
#include <Mobility/Geo/Node/GeoNode.h>

namespace NES {


GeoNode::GeoNode(string id, uint32_t storageSize) : GeoNode(std::move(id), 0, storageSize) {}

GeoNode::GeoNode(string  id, double rangeArea, uint32_t storageSize) : id(std::move(id)), rangeArea(rangeArea), range(nullptr) {
    currentLocation = std::make_shared<GeoPoint>();
    locationHistory = std::make_shared<LocationStorage>(storageSize);
}

string GeoNode::getId() const { return id; }

GeoPointPtr GeoNode::getCurrentLocation() { return currentLocation; }

const LocationStoragePtr& GeoNode::getLocationHistory() const { return locationHistory; }

void GeoNode::setCurrentLocation(const GeoPointPtr& location) {
    if (currentLocation->isValid()) {
        locationHistory->add(currentLocation);
    }
    currentLocation = location;
}

const GeoAreaPtr& GeoNode::getRange() const { return range; }

double GeoNode::getRangeArea() const { return rangeArea; }

}
