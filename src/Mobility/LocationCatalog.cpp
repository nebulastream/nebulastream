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

namespace NES {

void LocationCatalog::addNode(string nodeId) {
    this->nodes.insert(std::pair<string, GeoNodePtr>(nodeId, std::make_shared<GeoNode>(nodeId)));
}

void LocationCatalog::updateNodeLocation(string nodeId, const GeoPointPtr& location) {
    std::_Rb_tree_iterator<std::pair<const string, GeoNodePtr>> it = this->nodes.find(nodeId);
    if (it != this->nodes.end()) {
        it->second->setCurrentLocation(location);
    }
}

bool LocationCatalog::contains(string nodeId) { return this->nodes.contains(nodeId); }

uint64_t LocationCatalog::size() { return this->nodes.size(); }

GeoNodePtr LocationCatalog::getNode(string nodeId) { return this->nodes.at(nodeId); }

const std::map<string, GeoNodePtr>& LocationCatalog::getNodes() const { return nodes; }

}
