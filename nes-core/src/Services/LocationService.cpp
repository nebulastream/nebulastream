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
#include <Spatial/DataTypes/GeoLocation.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <nlohmann/json.hpp>

namespace NES {

LocationService::LocationService(TopologyPtr topology, Spatial::Index::Experimental::LocationIndexPtr locationIndex)
    : locationIndex(locationIndex), topology(topology){};

nlohmann::json LocationService::requestNodeLocationDataAsJson(uint64_t nodeId) {
    if (!topology->findNodeWithId(nodeId)) {
        return nullptr;
    }
    auto geoLocation = locationIndex->getGeoLocationForNode(nodeId);
    /*
    if (!geoLocation.isValid()) {
        return convertNodeLocationInfoToJson(nodeId, std::move(geoLocation));
    }
     */
    return convertNodeLocationInfoToJson(nodeId, std::move(geoLocation));
}

nlohmann::json LocationService::requestReconnectScheduleAsJson(uint64_t) {
    /* auto nodePtr = topology->findNodeWithId(nodeId);
    if (!nodePtr || nodePtr->getSpatialNodeType() != Spatial::Index::Experimental::SpatialType::MOBILE_NODE) {
        return nullptr;
    }
    auto schedule = nodePtr->getReconnectSchedule();
    nlohmann::json scheduleJson;
    auto startPtr = schedule->getPathStart();

    scheduleJson["pathStart"] = convertLocationToJson(std::move(startPtr));

    auto endPtr = schedule->getPathEnd();
    scheduleJson["pathEnd"] = convertLocationToJson(std::move(endPtr));

    auto updatePostion = schedule->getLastIndexUpdatePosition();
    scheduleJson["indexUpdatePosition"] = convertLocationToJson(std::move(updatePostion));

    auto reconnectArray = nlohmann::json::array();
    int i = 0;
    auto reconnectVectorPtr = schedule->getReconnectVector();
    if (reconnectVectorPtr) {
        auto reconnectVector = *reconnectVectorPtr;
        for (auto elem : reconnectVector) {
            nlohmann::json elemJson;
            elemJson["id"] = elem->reconnectPrediction.expectedNewParentId;
            elemJson["reconnectPoint"] = convertLocationToJson(std::move(elem->pointGeoLocation));
            elemJson["time"] = elem->reconnectPrediction.expectedTime;
            reconnectArray[i] = elemJson;
            i++;
        }
    }
    scheduleJson["reconnectPoints"] = reconnectArray;
    return scheduleJson;*/
    NES_NOT_IMPLEMENTED();
}

nlohmann::json LocationService::requestLocationDataFromAllMobileNodesAsJson() {
    auto nodeVector = locationIndex->getAllMobileNodeLocations();
    auto locMapJson = nlohmann::json::array();
    size_t count = 0;
    for (auto& [nodeId, location] : nodeVector) {
        nlohmann::json nodeInfo = convertNodeLocationInfoToJson(nodeId, std::move(location));
        locMapJson[count] = nodeInfo;
        ++count;
    }
    return locMapJson;
}

nlohmann::json LocationService::convertLocationToJson(NES::Spatial::DataTypes::Experimental::GeoLocation&& location) {
    nlohmann::json locJson;
    if (location.isValid()) {
        locJson[0] = location.getLatitude();
        locJson[1] = location.getLongitude();
    }
    return locJson;
}

nlohmann::json LocationService::convertNodeLocationInfoToJson(uint64_t id,
                                                              NES::Spatial::DataTypes::Experimental::GeoLocation&& loc) {
    nlohmann::json nodeInfo;
    nodeInfo["id"] = id;
    nlohmann::json locJson = convertLocationToJson(std::move(loc));
    nodeInfo["location"] = locJson;
    return nodeInfo;
}

bool LocationService::updatePredictedReconnect(const std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>& addPredictions, const std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>& removePredictions ) {
    (void) addPredictions;
    (void) removePredictions;
    NES_NOT_IMPLEMENTED();//Will be implemented as part of #
}
}// namespace NES
