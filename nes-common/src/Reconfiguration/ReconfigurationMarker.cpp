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

std::optional<ReconfigurationMarkerEventPtr> ReconfigurationMarker::getReconfigurationEvent(const std::string& key) {
    if (reconfigurationEvents.contains(key)) {
        return reconfigurationEvents.at(key);
    }
    NES_DEBUG("{} not found in the reconfiguration marker event", key);
    return std::nullopt;
}

void ReconfigurationMarker::addReconfigurationEvent(const std::string& key,
                                                    const ReconfigurationMarkerEventPtr& reconfigurationEvent) {
    reconfigurationEvents.insert({key, std::move(reconfigurationEvent)});
}
}// namespace NES
