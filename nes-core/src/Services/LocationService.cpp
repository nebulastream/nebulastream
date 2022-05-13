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

#include <Common/Location.hpp>
#include <Services/LocationService.hpp>
#include <Spatial/LocationIndex.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <cpprest/json.h>
#include <Spatial/ReconnectSchedule.hpp>
#include <utility>

namespace NES::Spatial::Index::Experimental {
LocationService::LocationService(TopologyPtr topology) : locationIndex(topology->getLocationIndex()), topology(topology){};

web::json::value LocationService::requestNodeLocationDataAsJson(uint64_t nodeId) {
    auto nodePtr = topology->findNodeWithId(nodeId);
    if (!nodePtr) {
        return web::json::value::null();
    }
    return convertNodeLocationInfoToJson(nodeId, nodePtr->getCoordinates());
}

web::json::value LocationService::requestReconnectScheduleAsJson(uint64_t nodeId) {
    auto nodePtr = topology->findNodeWithId(nodeId);
    if (!nodePtr || !nodePtr->isMobileNode()) {
        return web::json::value::null();
    }
    auto schedule = nodePtr->getReconnectSchedule();
    web::json::value scheduleJson;
    scheduleJson["pathStart"] = convertLocationToJson(schedule->getPathStart());
    scheduleJson["pathEnd"] = convertLocationToJson(schedule->getPathEnd());
    //todo: insert vector also
    return scheduleJson;

}

web::json::value LocationService::requestLocationDataFromAllMobileNodesAsJson() {
    auto nodeVector = locationIndex->getAllMobileNodeLocations();
    web::json::value locMapJson = web::json::value::array();
    size_t count = 0;
    for (const auto& [nodeId, location] : nodeVector) {
        web::json::value nodeInfo = convertNodeLocationInfoToJson(nodeId, location);
        locMapJson[count] = web::json::value(nodeInfo);
        ++count;
    }
    return locMapJson;
}


web::json::value LocationService::convertLocationToJson(LocationPtr loc) {
    web::json::value locJson;
    if (loc) {
        locJson[0] = loc->getLatitude();
        locJson[1] = loc->getLongitude();
    } else {
        locJson = web::json::value::null();
    }
    return locJson;
}

web::json::value LocationService::convertNodeLocationInfoToJson(uint64_t id, LocationPtr loc) {
    web::json::value nodeInfo;
    nodeInfo["id"] = web::json::value(id);
    web::json::value locJson = convertLocationToJson(std::move(loc));
    nodeInfo["location"] = web::json::value(locJson);
    return nodeInfo;
}
}// namespace NES::Spatial::Index::Experimental
