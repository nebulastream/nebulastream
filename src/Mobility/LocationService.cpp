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

#include <Mobility/LocationService.h>
#include <Util/Logger.hpp>
#include <chrono>
#include <thread>

namespace NES {

LocationService::LocationService(uint32_t updateInterval,
                                 uint32_t storageSize,
                                 bool dynamicDuplicatesFilterEnabled,
                                 uint32_t filterStorageSize)
    : running(false), updateInterval(updateInterval),
      dynamicDuplicatesFilterEnabled(dynamicDuplicatesFilterEnabled), filterStorageSize(filterStorageSize) {
    locationCatalog = std::make_shared<LocationCatalog>(storageSize);
}

void LocationService::addSink(const string& nodeId, const double movingRangeArea) {
    this->locationCatalog->addSink(nodeId, movingRangeArea);
}

void LocationService::addSource(const string& nodeId) { this->locationCatalog->addSource(nodeId, 0); }

void LocationService::addSource(const string& nodeId, double rangeArea) { this->locationCatalog->addSource(nodeId, rangeArea); }

void LocationService::updateNodeLocation(const string& nodeId, const GeoPointPtr& location) {
    this->locationCatalog->updateNodeLocation(nodeId, location);
}

const LocationCatalogPtr& LocationService::getLocationCatalog() const { return locationCatalog; }

void LocationService::initInstance(uint32_t updateInterval,
                                   uint32_t storageSize,
                                   bool dynamicDuplicatesFilterEnabled,
                                   uint32_t filterStorageSize) {
    instance = std::make_shared<LocationService>(updateInterval, storageSize, dynamicDuplicatesFilterEnabled, filterStorageSize);
}

LocationServicePtr LocationService::getInstance() {
    if (instance == nullptr) {
        instance = std::make_shared<LocationService>();
    }
    return instance;
}

void LocationService::cleanInstance() { instance = nullptr; }

void LocationService::updateSources() { locationCatalog->updateSources(); }

void LocationService::start() {
    this->running = true;
    NES_DEBUG("LocationService: starting with time interval -> " + std::to_string(updateInterval));

    while (running) {
        updateSources();
        std::this_thread::sleep_for(std::chrono::milliseconds(updateInterval));
    }
    NES_DEBUG("LocationService: stopped!");
}

void LocationService::stop() {
    NES_DEBUG("LocationService: stopping ...");
    this->running = false;
}

bool LocationService::isRunning() const { return running; }

bool LocationService::isDynamicDuplicatesFilterEnabled() const { return dynamicDuplicatesFilterEnabled; }

uint32_t LocationService::getFilterStorageSize() const { return filterStorageSize; }

}// namespace NES
