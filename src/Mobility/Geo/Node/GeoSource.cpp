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

#include <Mobility/Geo/Area/GeoAreaFactory.h>
#include <Mobility/Geo/Node/GeoSource.h>

namespace NES {

GeoSource::GeoSource(const std::string& id) : GeoNode(id), enabled(false) {}

GeoSource::GeoSource(const std::string& id, double rangeArea) : GeoNode(id, rangeArea),  enabled(false) {}

bool GeoSource::hasRange() const { return (range != nullptr); }

bool GeoSource::isEnabled() const { return enabled; }

void GeoSource::setEnabled(bool flag) { GeoSource::enabled = flag; }

void GeoSource::setCurrentLocation(const GeoPointPtr& currentLocation) {
    GeoNode::setCurrentLocation(currentLocation);

    if (rangeArea > 0) {
        range = GeoAreaFactory::createCircle(currentLocation, rangeArea);
    }
}

GeoSource::~GeoSource() = default;

}

