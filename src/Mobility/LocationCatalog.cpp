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

void LocationCatalog::addSource(const string& nodeId) {
    std::unique_lock lock(catalogLock);
    this->sources.insert(std::pair<string, GeoSourcePtr>(nodeId, std::make_shared<GeoSource>(nodeId)));
}

void LocationCatalog::addSink(const string& nodeId, const double movingRangeArea) {
    std::unique_lock lock(catalogLock);
    this->sinks.insert(std::pair<string, GeoSinkPtr>(nodeId, std::make_shared<GeoSink>(nodeId, movingRangeArea)));
}

void LocationCatalog::enableSource(const string& nodeId) {
    std::unique_lock lock(catalogLock);
    if (this->sources.contains(nodeId)) {
        this->sources.at(nodeId)->setEnabled(true);
    }
}

void LocationCatalog::disableSource(const string& nodeId) {
    std::unique_lock lock(catalogLock);
    if (this->sources.contains(nodeId)) {
        this->sources.at(nodeId)->setEnabled(false);
    }

}

const std::map<string, GeoSinkPtr>& LocationCatalog::getSinks() const { return sinks; }

const std::map<string, GeoSourcePtr>& LocationCatalog::getSources() const { return sources; }

void LocationCatalog::updateNodeLocation(const string& nodeId, const GeoPointPtr& location) {
    std::unique_lock lock(catalogLock);
    if (sinks.contains(nodeId)) {
        std::_Rb_tree_iterator<std::pair<const string, GeoSinkPtr>> it = this->sinks.find(nodeId);
        if (it != this->sinks.end()) {
            it->second->setCurrentLocation(location);
        }
    }

    if (sources.contains(nodeId)) {
        std::_Rb_tree_iterator<std::pair<const string, GeoSourcePtr>> it = this->sources.find(nodeId);
        if (it != this->sources.end()) {
            it->second->setCurrentLocation(location);
        }
    }
}

bool LocationCatalog::contains(const string& nodeId) {
    std::unique_lock lock(catalogLock);
    return this->sinks.contains(nodeId) || this->sources.contains(nodeId);
}

uint64_t LocationCatalog::size() {
    std::unique_lock lock(catalogLock);
    return this->sinks.size() + this->sources.size();
}

}
