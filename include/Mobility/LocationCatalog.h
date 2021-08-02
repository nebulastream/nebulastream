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

#ifndef NES_LOCATIONCATALOG_H
#define NES_LOCATIONCATALOG_H

#include "Geo/GeoNode.h"
#include <map>
#include <cpprest/json.h>



namespace NES {

class GeoNode;
using GeoNodePtr = std::shared_ptr<GeoNode>;

class GeoPoint;
using GeoPointPtr = std::shared_ptr<GeoPoint>;

using std::string;


class LocationCatalog {

  private:
    std::map<string, GeoNodePtr> nodes;

  public:
    LocationCatalog() = default;
    void addNode(string nodeId);
    void updateNodeLocation(string nodeId, const GeoPointPtr& location);
    bool contains(string nodeId);
    GeoNodePtr getNode(string nodeId);
    [[nodiscard]] const std::map<string, GeoNodePtr>& getNodes() const;
    uint64_t size();
};

using LocationCatalogPtr = std::shared_ptr<LocationCatalog>;

}

#endif//NES_LOCATIONCATALOG_H
