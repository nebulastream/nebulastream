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
#include <Mobility/LocationCatalog.h>

namespace NES {

class GeoPoint;
using GeoPointPtr = std::shared_ptr<GeoPoint>;

class LocationCatalog;
using LocationCatalogPtr = std::shared_ptr<LocationCatalog>;

class LocationService;
using LocationServicePtr = std::shared_ptr<LocationService>;

static LocationServicePtr instance = nullptr;

class LocationService {
  private:
    LocationCatalogPtr locationCatalog;

  public:
    static LocationServicePtr getInstance();
    explicit LocationService();

    void addSink(const string& nodeId, double movingRangeArea);
    void addSource(const string& nodeId);
    void updateNodeLocation(const string& nodeId, const GeoPointPtr& location);
    bool checkIfPointInRange(const GeoPointPtr& location);
    [[nodiscard]] const LocationCatalogPtr& getLocationCatalog() const;
};

using LocationServicePtr = std::shared_ptr<LocationService>;

}

#endif//NES_LOCATIONSERVICE_H
