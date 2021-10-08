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

#include <Mobility/Geo/Projection/GeoCalculator.h>
#include <Mobility/Storage/LocationStorage.h>

namespace NES {

LocationStorage::LocationStorage(int maxNumberOfTuples) : maxNumberOfTuples(maxNumberOfTuples) {}

void LocationStorage::add(const GeoPointPtr& point) {
    std::unique_lock lock(storageLock);
    if (this->geoStorage.size() == this->maxNumberOfTuples) {
        geoStorage.erase(geoStorage.begin());
    }
    this->geoStorage.push_back(point);

    if (this->cartesianStorage.size() == this->maxNumberOfTuples) {
        cartesianStorage.erase(cartesianStorage.begin());
    }
    this->cartesianStorage.push_back(GeoCalculator::geographicToCartesian(point));
}

CartesianPointPtr LocationStorage::getCartesian(int index) {
    std::unique_lock lock(storageLock);
    return cartesianStorage.at(index);
}

GeoPointPtr LocationStorage::getGeo(int index) {
    std::unique_lock lock(storageLock);
    return geoStorage.at(index);
}

const std::vector<CartesianPointPtr>& LocationStorage::getCartesianPoints() const { return cartesianStorage; }

uint64_t LocationStorage::size() {
    std::unique_lock lock(storageLock);
    return geoStorage.size();
}

bool LocationStorage::empty() { return geoStorage.empty(); }

}
