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
#include <Spatial/Index/LocationIndex.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <unordered_map>
#ifdef S2DEF
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#endif

namespace NES::Spatial::Index::Experimental {

LocationIndex::LocationIndex() = default;

bool LocationIndex::initializeFieldNodeCoordinates(const TopologyNodePtr& node, Location geoLoc) {
    return setFieldNodeCoordinates(node, geoLoc);
}

bool LocationIndex::updateFieldNodeCoordinates(const TopologyNodePtr& node, Location geoLoc) {
#ifdef S2DEF
    if (removeNodeFromSpatialIndex(node)) {
        return setFieldNodeCoordinates(node, geoLoc);
    }
    return false;
#else
    return setFieldNodeCoordinates(node, geoLoc);
#endif
}

bool LocationIndex::setFieldNodeCoordinates(const TopologyNodePtr& node, Location geoLoc) {
    if (!geoLoc.isValid()) {
        NES_WARNING("trying to set node coordinates to invalid value")
        return false;
    }
#ifdef S2DEF
    double newLat = geoLoc.getLatitude();
    double newLng = geoLoc.getLongitude();
    S2Point newLoc(S2LatLng::FromDegrees(newLat, newLng));
    NES_DEBUG("updating location of Node to: " << newLat << ", " << newLng);
    std::unique_lock lock(locationIndexMutex);
    nodePointIndex.Add(newLoc, node);
#else
    NES_WARNING("Files were compiled without s2. Nothing inserted into spatial index");
    std::unique_lock lock(locationIndexMutex);
#endif
    node->setFixedCoordinates(geoLoc);
    lock.unlock();
    return true;
}

bool LocationIndex::removeNodeFromSpatialIndex(const TopologyNodePtr& node) {
    std::unique_lock lock(locationIndexMutex);
    if (node->getSpatialNodeType() == NodeType::MOBILE_NODE) {
        mobileNodes.erase(node->getId());
    }
#ifdef S2DEF
    auto geoLocation = node->getCoordinates()->getLocation();
    if (!geoLocation->isValid()) {
        NES_WARNING("trying to remove node from spatial index but the node does not have a location set");
        return false;
    }
    S2Point point(S2LatLng::FromDegrees(geoLocation->getLatitude(), geoLocation->getLongitude()));
    nodePointIndex.Remove(point, node);
    return true;
#else
    NES_WARNING("Files were compiled without s2. Nothing can be removed from the spatial index because it does not exist");
    NES_INFO("node id: " << node->getId());
    return {};
#endif
}

std::optional<TopologyNodePtr> LocationIndex::getClosestNodeTo(const Location& geoLoc, int radius) {
    std::unique_lock lock(locationIndexMutex);
#ifdef S2DEF
    S2ClosestPointQuery<TopologyNodePtr> query(&nodePointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));
    S2ClosestPointQuery<TopologyNodePtr>::PointTarget target(
        S2Point(S2LatLng::FromDegrees(geoLoc.getLatitude(), geoLoc.getLongitude())));
    S2ClosestPointQuery<TopologyNodePtr>::Result queryResult = query.FindClosestPoint(&target);
    if (queryResult.is_empty()) {
        return {};
    }
    return queryResult.data();
#else
    NES_WARNING("Files were compiled without s2. Nothing inserted into spatial index");
    NES_INFO("supplied values: " << geoLoc.getLatitude() << ", " << geoLoc.getLongitude() << " radius:" << radius);
    return {};
#endif
}

std::optional<TopologyNodePtr> LocationIndex::getClosestNodeTo(const TopologyNodePtr& nodePtr, int radius) {
#ifdef S2DEF
    auto geoLocation = nodePtr->getCoordinates()->getLocation();

    if (!geoLocation->isValid()) {
        NES_WARNING("Trying to get the closest node to a node that does not have a location");
        return {};
    }

    std::unique_lock lock(locationIndexMutex);
    S2ClosestPointQuery<TopologyNodePtr> query(&nodePointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));
    S2ClosestPointQuery<TopologyNodePtr>::PointTarget target(
        S2Point(S2LatLng::FromDegrees(geoLocation->getLatitude(), geoLocation->getLongitude())));
    auto queryResult = query.FindClosestPoint(&target);
    //if we cannot find any node within the radius return an empty optional
    if (queryResult.is_empty()) {
        return {};
    }
    //if the closest node is different from the input node, return it
    auto closest = queryResult.data();
    if (closest != nodePtr) {
        return closest;
    }
    //if the closest node is equal to our input node, we need to look for the second closest
    auto closestPoints = query.FindClosestPoints(&target);
    if (closestPoints.size() < 2) {
        return {};
    }
    return closestPoints[1].data();

#else
    NES_WARNING("Files were compiled without s2, cannot find closest nodes");
    NES_INFO(radius << nodePtr);
    return {};
#endif
}

std::vector<std::pair<TopologyNodePtr, Location>> LocationIndex::getNodesInRange(Location center, double radius) {
#ifdef S2DEF
    std::unique_lock lock(locationIndexMutex);
    S2ClosestPointQuery<TopologyNodePtr> query(&nodePointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));

    S2ClosestPointQuery<TopologyNodePtr>::PointTarget target(
        S2Point(S2LatLng::FromDegrees(center.getLatitude(), center.getLongitude())));
    auto result = query.FindClosestPoints(&target);
    std::vector<std::pair<TopologyNodePtr, Location>> closestNodeList;
    for (auto r : result) {
        auto latLng = S2LatLng(r.point());
        closestNodeList.emplace_back(r.data(), Location(latLng.lat().degrees(), latLng.lng().degrees()));
    }
    return closestNodeList;

#else
    NES_WARNING("Files were compiled without s2, cannot find closest nodes");
    NES_INFO("supplied values: " << center.getLatitude() << ", " << center.getLongitude() << "radius: " << radius);
    return {};
#endif
}

void LocationIndex::addMobileNode(TopologyNodePtr node) {
    std::unique_lock lock(locationIndexMutex);
    mobileNodes.insert({node->getId(), node});
}

std::vector<std::pair<uint64_t, LocationPtr>> LocationIndex::getAllMobileNodeLocations() {
    std::vector<std::pair<uint64_t, LocationPtr>> loccationVector;
    std::unique_lock lock(locationIndexMutex);
    loccationVector.reserve(mobileNodes.size());
    for (const auto& [nodeId, topologyNode] : mobileNodes) {
        auto location = topologyNode->getCoordinates()->getLocation();
        if (location->isValid()) {
            loccationVector.emplace_back(nodeId, location);
        }
    }
    return loccationVector;
}

size_t LocationIndex::getSizeOfPointIndex() {
    std::unique_lock lock(locationIndexMutex);
#ifdef S2DEF
    return nodePointIndex.num_points();
#else
    NES_WARNING("s2 lib not included");
    return {};
#endif
}

bool LocationIndex::updatePredictedReconnect(uint64_t mobileWorkerId, Mobility::Experimental::ReconnectPrediction prediction) {
    std::unique_lock lock(locationIndexMutex);
    if (mobileNodes.contains(mobileWorkerId)) {
        NES_DEBUG("LocationIndex: Updating reconnect prediciton for node " << mobileWorkerId)
        NES_DEBUG("New reconnect prediction: id=" << prediction.expectedNewParentId << " time=" << prediction.expectedTime)
        reconnectPredictionMap[mobileWorkerId] = prediction;
        return true;
    }
    NES_DEBUG("trying to update reconnect prediction but could not find a mobile node with id " << mobileWorkerId)
    return false;
}
std::optional<Mobility::Experimental::ReconnectPrediction> LocationIndex::getScheduledReconnect(uint64_t nodeId) {
    std::unique_lock lock(locationIndexMutex);
    if (reconnectPredictionMap.contains(nodeId)) {
        return reconnectPredictionMap[nodeId];
    }
    return {};
}
}// namespace NES::Spatial::Index::Experimental
