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

#include <Mobility/Storage/LocationStorage.h>

namespace NES {

LocationStorage::LocationStorage(int maxNumberOfTuples) : maxNumberOfTuples(maxNumberOfTuples) {}

void LocationStorage::add(const GeoPointPtr& point) {
    std::lock_guard lock(storageLock);
    if (this->storage.size() == this->maxNumberOfTuples) {
        storage.erase(storage.begin());
    }
    this->storage.push_back(point);
}

GeoPointPtr LocationStorage::get(int index) {
    std::lock_guard lock(storageLock);
    return storage.at(index);
}

uint64_t LocationStorage::size() {
    std::lock_guard lock(storageLock);
    return storage.size();
}

bool LocationStorage::empty() { return storage.empty(); }

}
