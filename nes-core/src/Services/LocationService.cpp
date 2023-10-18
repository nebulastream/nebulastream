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
#include<Util/Mobility/GeoLocation.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectPoint.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>
#include <utility>

namespace NES {

LocationService::LocationService(TopologyPtr topology, Spatial::Index::Experimental::LocationIndexPtr locationIndex)
    : locationIndex(std::move(locationIndex)), topology(std::move(topology)){};

nlohmann::json LocationService::requestNodeLocationDataAsJson(uint64_t nodeId) {

    if (!topology->findNodeWithId(nodeId)) {
        return nullptr;
    }

    auto geoLocation = locationIndex->getGeoLocationForNode(nodeId);
    if (geoLocation.has_value()) {
        return convertNodeLocationInfoToJson(nodeId, geoLocation.value());
    } else {
        nlohmann::json nodeInfo;
        nodeInfo["id"] = nodeId;
        return nodeInfo;
    }
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

nlohmann::json LocationService::requestLocationAndParentDataFromAllMobileNodes() {
    auto nodeVector = locationIndex->getAllNodeLocations();
    auto locationMapJson = nlohmann::json::array();
    auto mobileEdgesJson = nlohmann::json::array();
    uint32_t count = 0;
    uint32_t edgeCount = 0;
    for (const auto& [nodeId, location] : nodeVector) {
        auto topologyNode = topology->findNodeWithId(nodeId);
        if (topologyNode && topologyNode->getSpatialNodeType() == Spatial::Experimental::SpatialType::MOBILE_NODE) {
            nlohmann::json nodeInfo = convertNodeLocationInfoToJson(nodeId, location);
            locationMapJson[count] = nodeInfo;
            for (const auto& parent : topologyNode->getParents()) {
                const nlohmann::json edge{{"source", nodeId}, {"target", parent->as<TopologyNode>()->getId()}};
                /*
                edge["source"] = nodeId;
                edge["target"] = parent->as<TopologyNode>()->getId();
                 */
                mobileEdgesJson[edgeCount] = edge;
                ++edgeCount;
            }
            ++count;
        }
    }
    nlohmann::json response;
    response["nodes"] = locationMapJson;
    response["edges"] = mobileEdgesJson;
    return response;
}

nlohmann::json LocationService::convertLocationToJson(NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation) {
    nlohmann::json locJson;
    if (geoLocation.isValid()) {
        locJson["latitude"] = geoLocation.getLatitude();
        locJson["longitude"] = geoLocation.getLongitude();
    }
    return locJson;
}

nlohmann::json LocationService::convertNodeLocationInfoToJson(uint64_t id,
                                                              NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation) {
    nlohmann::json nodeInfo;
    nodeInfo["id"] = id;
    nlohmann::json locJson = convertLocationToJson(std::move(geoLocation));
    nodeInfo["location"] = locJson;
    return nodeInfo;
}

bool LocationService::updatePredictedReconnect(
    const std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>& addPredictions,
    const std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>& removePredictions) {
    (void) addPredictions;
    (void) removePredictions;
    NES_NOT_IMPLEMENTED();//Will be implemented as part of #
}
}// namespace NES
