/*
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

#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

ReconfigurationMarkerPtr ReconfigurationMarker::create() { return std::make_shared<ReconfigurationMarker>(); }

std::optional<ReconfigurationMarkerEventPtr>
ReconfigurationMarker::getReconfigurationEvent(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion) const {
    if (reconfigurationEvents.contains(decomposedQueryIdWithVersion)) {
        return reconfigurationEvents.at(decomposedQueryIdWithVersion);
    }
    NES_DEBUG("{}.{} not found in the reconfiguration marker event",
              decomposedQueryIdWithVersion.id,
              decomposedQueryIdWithVersion.version);
    return std::nullopt;
}

void ReconfigurationMarker::addReconfigurationEvent(DecomposedQueryId decomposedQueryId,
                                                    DecomposedQueryPlanVersion decomposedQueryVersion,
                                                    const ReconfigurationMarkerEventPtr reconfigurationEvent) {
    reconfigurationEvents.insert({DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion), reconfigurationEvent});
}

void ReconfigurationMarker::addReconfigurationEvent(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion,
                                                    const ReconfigurationMarkerEventPtr reconfigurationEvent) {
    reconfigurationEvents.insert({decomposedQueryIdWithVersion, reconfigurationEvent});
}

const std::unordered_map<DecomposedQueryIdWithVersion, ReconfigurationMarkerEventPtr>&
ReconfigurationMarker::getAllReconfigurationMarkerEvents() const {
    return reconfigurationEvents;
}

std::vector<ReconfigurationMarkerEventPtr>
ReconfigurationMarker::getReconfigurationEventsForPlanId(DecomposedQueryId decomposedQueryId) const {
    std::vector<ReconfigurationMarkerEventPtr> result;
    for (auto [idAndVerion, event]: reconfigurationEvents) {
        if (idAndVerion.id == decomposedQueryId) {
            result.push_back(event);
        }
    }
    return result;
}
}// namespace NES
