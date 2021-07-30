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

#include <Catalogs/GeoNode.h>
NES::GeoNode::GeoNode(int id) : id(id) {}

int NES::GeoNode::getId() const { return id; }
NES::GeoLocation& NES::GeoNode::getCurrentLocation() { return currentLocation; }
const std::vector<NES::GeoLocation>& NES::GeoNode::getLocationHistory() const { return locationHistory; }

void NES::GeoNode::setCurrentLocation(NES::GeoLocation location) {
    if (this->getCurrentLocation().isValid()) {
        locationHistory.push_back(this->getCurrentLocation());
    }
    this->currentLocation = location;
}
