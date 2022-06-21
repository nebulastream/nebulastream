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
#include <Util/Experimental/WorkerSpatialType.hpp>

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
    if (!nodePtr || nodePtr->getSpatialType() != WorkerSpatialType::MOBILE_NODE) {
        return web::json::value::null();
    }
    auto schedule = nodePtr->getReconnectSchedule();
    web::json::value scheduleJson;
    scheduleJson["pathStart"] = convertLocationToJson(*schedule->getPathStart());
    scheduleJson["pathEnd"] = convertLocationToJson(*schedule->getPathEnd());
    scheduleJson["indexUpdatePosition"] = convertLocationToJson(*schedule->getLastIndexUpatePosition());

    auto reconnectArray = web::json::value::array();
    int i = 0;
    //todo: make nullcheck here
    if (schedule->getReconnectVector()) {
        for (auto elem : *(schedule->getReconnectVector())) {
            web::json::value elemJson;
            elemJson["id"] = std::get<0>(elem);
            //loc = std::get<1>(elem);
            elemJson["reconnectPoint"] = convertLocationToJson(std::get<1>(elem));
            elemJson["time"] = std::get<2>(elem);
            reconnectArray[i] = elemJson;
            i++;
        }
    }
    scheduleJson["reconnectPoints"] = reconnectArray;
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


web::json::value LocationService::convertLocationToJson(Location loc) {
    web::json::value locJson;
    if (loc.isValid()) {
        locJson[0] = loc.getLatitude();
        locJson[1] = loc.getLongitude();
    } else {
        locJson = web::json::value::null();
    }
    return locJson;
}

web::json::value LocationService::convertNodeLocationInfoToJson(uint64_t id, Location loc) {
    web::json::value nodeInfo;
    nodeInfo["id"] = web::json::value(id);
    web::json::value locJson = convertLocationToJson(loc);
    nodeInfo["location"] = web::json::value(locJson);
    return nodeInfo;
}
bool LocationService::updatePredictedReconnect(uint64_t deviceId, uint64_t reconnectNodeId, Location location, Timestamp time) {
   if (locationIndex->updatePredictedReconnect(deviceId, reconnectNodeId, location, time)) {
       return true;
   } else if (topology->findNodeWithId(reconnectNodeId)) {
       NES_DEBUG("node exists but is not a mobile node")
   }
   return false;
}
}// namespace NES::Spatial::Index::Experimental
