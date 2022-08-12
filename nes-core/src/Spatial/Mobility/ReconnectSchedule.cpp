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
#include "Spatial/Mobility/ReconnectSchedule.hpp"
#include <utility>

namespace NES::Spatial::Mobility::Experimental {

ReconnectSchedule::ReconnectSchedule(uint64_t currentParentId,
                                     Index::Experimental::LocationPtr pathBeginning,
                                     Index::Experimental::LocationPtr pathEnd,
                                     Index::Experimental::LocationPtr lastIndexUpdatePosition,
                                     std::shared_ptr<std::vector<std::shared_ptr<ReconnectPoint>>> reconnectVector)
    : currentParentId(currentParentId), pathStart(std::move(pathBeginning)), pathEnd(std::move(pathEnd)),
      lastIndexUpdatePosition(lastIndexUpdatePosition), reconnectVector(std::move(reconnectVector)) {}

Index::Experimental::LocationPtr ReconnectSchedule::getPathStart() const { return pathStart; }

Index::Experimental::LocationPtr ReconnectSchedule::getPathEnd() const { return pathEnd; }

std::shared_ptr<std::vector<std::shared_ptr<ReconnectPoint>>> ReconnectSchedule::getReconnectVector() const {
    return reconnectVector;
}

ReconnectSchedule ReconnectSchedule::Empty() { return {0, {}, {}, {}, {}}; }
Index::Experimental::LocationPtr ReconnectSchedule::getLastIndexUpdatePosition() const { return lastIndexUpdatePosition; }

uint64_t ReconnectSchedule::getCurrentParentId() { return currentParentId; }
}// namespace NES::Spatial::Mobility::Experimental