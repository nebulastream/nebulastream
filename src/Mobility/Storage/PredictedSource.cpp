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

#include <Mobility/Storage/PredictedSource.h>

namespace NES {

PredictedSource::PredictedSource(const GeoSourcePtr& source) : source(source), inRange(false) {}

const GeoSourcePtr& PredictedSource::getSource() const { return source; }

bool PredictedSource::isInRange() const { return inRange; }

void PredictedSource::setInRange(bool inRangeValue) { PredictedSource::inRange = inRangeValue; }

std::vector<GeoSourcePtr>& PredictedSource::getOverlaps() { return overlaps; }

}
