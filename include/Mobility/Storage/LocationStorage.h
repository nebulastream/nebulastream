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

#ifndef NES_LOCATIONSTORAGE_H
#define NES_LOCATIONSTORAGE_H

#include <mutex>
#include <vector>

#include <Mobility/Geo/CartesianPoint.h>
#include <Mobility/Geo/GeoPoint.h>

namespace NES {

class LocationStorage {
  private:
    uint64_t maxNumberOfTuples;
    std::vector<CartesianPointPtr> cartesianStorage;
    std::vector<GeoPointPtr> geoStorage;
    std::mutex storageLock;

  public:
    explicit LocationStorage(int maxNumberOfTuples);
    void add(const GeoPointPtr& point);
    CartesianPointPtr getCartesian(int index);
    GeoPointPtr getGeo(int index);
    const std::vector<CartesianPointPtr>& getCartesianPoints() const;
    uint64_t size();
    bool empty();

};

using LocationStoragePtr = std::shared_ptr<LocationStorage>;

}

#endif//NES_LOCATIONSTORAGE_H
