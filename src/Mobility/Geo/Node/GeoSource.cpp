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

#include <Mobility/Geo/Node/GeoSource.h>

namespace NES {

GeoSource::GeoSource(const std::string& id) : GeoNode(id), range(nullptr),  hasRange(false), enabled(false) {}

GeoSource::GeoSource(const std::string& id, NES::GeoAreaPtr  range) : GeoNode(id), range(std::move(range)),  hasRange(true), enabled(false) {}

const GeoAreaPtr& GeoSource::getRange() const { return range; }

bool GeoSource::isHasRange() const { return hasRange; }

bool GeoSource::isEnabled() const { return enabled; }

void GeoSource::setEnabled(bool flag) { GeoSource::enabled = flag; }

GeoSource::~GeoSource() = default;

}

