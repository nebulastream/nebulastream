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

#include <Services/LocationService.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Spatial/Mobility/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectSchedule.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <nlohmann/json.hpp>

namespace NES::Spatial::Index::Experimental {
LocationService::LocationService(TopologyPtr topology) : locationIndex(topology->getLocationIndex()), topology(topology){};

nlohmann::json LocationService::requestNodeLocationDataAsJson(uint64_t nodeId) {
    auto nodePtr = topology->findNodeWithId(nodeId);
    if (!nodePtr || !nodePtr->getCoordinates()) {
        return nullptr;
    }
    return convertNodeLocationInfoToJson(nodeId, *nodePtr->getCoordinates()->getLocation());
}

nlohmann::json LocationService::requestReconnectScheduleAsJson(uint64_t nodeId) {
    auto nodePtr = topology->findNodeWithId(nodeId);
    if (!nodePtr || nodePtr->getSpatialNodeType() != NodeType::MOBILE_NODE) {
        return nullptr;
    }
    auto schedule = nodePtr->getReconnectSchedule();
    nlohmann::json scheduleJson;
    auto startPtr = schedule->getPathStart();
    scheduleJson["pathStart"];
    if (startPtr) {
        scheduleJson["pathStart"] = convertLocationToJson(*startPtr);
    }
    auto endPtr = schedule->getPathEnd();
    scheduleJson["pathEnd"];
    if (endPtr) {
        scheduleJson["pathEnd"] = convertLocationToJson(*endPtr);
    }
    auto updatePostion = schedule->getLastIndexUpdatePosition();
    scheduleJson["indexUpdatePosition"];
    if (updatePostion) {
        scheduleJson["indexUpdatePosition"] = convertLocationToJson(*updatePostion);
    }

    auto reconnectArray = nlohmann::json::array();
    int i = 0;
    auto reconnectVectorPtr = schedule->getReconnectVector();
    if (reconnectVectorPtr) {
        auto reconnectVector = *reconnectVectorPtr;
        for (auto elem : reconnectVector) {
            nlohmann::json elemJson;
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

nlohmann::json LocationService::requestLocationDataFromAllMobileNodesAsJson() {
    auto nodeVector = locationIndex->getAllMobileNodeLocations();
    auto locMapJson = nlohmann::json::array();
    size_t count = 0;
    for (const auto& [nodeId, location] : nodeVector) {
        nlohmann::json nodeInfo = convertNodeLocationInfoToJson(nodeId, *location);
        locMapJson[count] = nodeInfo;
        ++count;
    }
    return locMapJson;
}

nlohmann::json LocationService::convertLocationToJson(Location location) {
    nlohmann::json locJson;
    if (location.isValid()) {
        locJson[0] = location.getLatitude();
        locJson[1] = location.getLongitude();
    }
    return locJson;
}

nlohmann::json LocationService::convertNodeLocationInfoToJson(uint64_t id, Location loc) {
    nlohmann::json nodeInfo;
    nodeInfo["id"] = id;
    nlohmann::json locJson = convertLocationToJson(loc);
    nodeInfo["location"] = locJson;
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
