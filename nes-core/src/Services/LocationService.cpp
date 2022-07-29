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

#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Spatial/Mobility/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectSchedule.hpp>
#include <Services/LocationService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <cpprest/json.h>

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
    if (!nodePtr || nodePtr->getSpatialNodeType() != NodeType::MOBILE_NODE) {
        return web::json::value::null();
    }
    auto schedule = nodePtr->getReconnectSchedule();
    web::json::value scheduleJson;
    scheduleJson["pathStart"] = convertLocationToJson(*schedule->getPathStart());
    scheduleJson["pathEnd"] = convertLocationToJson(*schedule->getPathEnd());
    scheduleJson["indexUpdatePosition"] = convertLocationToJson(*schedule->getLastIndexUpdatePosition());

    auto reconnectArray = web::json::value::array();
    int i = 0;
    auto reconnectVectorPtr = schedule->getReconnectVector();
    if (reconnectVectorPtr) {
        auto reconnectVector = *reconnectVectorPtr;
        for (auto elem : reconnectVector) {
            web::json::value elemJson;
            elemJson["id"] = elem->reconnectPrediction.expectedNewParentId;
            elemJson["reconnectPoint"] = convertLocationToJson(elem->predictedReconnectLocation);
            elemJson["time"] = elem->reconnectPrediction.expectedTime;
            reconnectArray[i] = elemJson;
            i++;
        }
    }
    scheduleJson["reconnectPoints"] = reconnectArray;
    return scheduleJson;
}

web::json::value LocationService::requestLocationDataFromAllMobileNodesAsJson() {
    auto nodeVector = locationIndex->getAllMobileNodeLocations();
    web::json::value locMapJson = web::json::value::array();
    size_t count = 0;
    for (const auto& [nodeId, location] : nodeVector) {
        web::json::value nodeInfo = convertNodeLocationInfoToJson(nodeId, *location);
        locMapJson[count] = web::json::value(nodeInfo);
        ++count;
    }
    return locMapJson;
}


web::json::value LocationService::convertLocationToJson(Location location) {
    web::json::value locJson;
    if (location.isValid()) {
        locJson[0] = location.getLatitude();
        locJson[1] = location.getLongitude();
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
bool LocationService::updatePredictedReconnect(uint64_t mobileWorkerId, Mobility::Experimental::ReconnectPrediction prediction) {
   if (locationIndex->updatePredictedReconnect(mobileWorkerId, prediction)) {
       return true;
   } else if (topology->findNodeWithId(prediction.expectedNewParentId)) {
       NES_WARNING("node exists but is not a mobile node")
   }
   return false;
}
}// namespace NES::Spatial::Index::Experimental
