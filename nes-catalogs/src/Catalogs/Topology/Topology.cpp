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

#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Mobility/SpatialTypeUtility.hpp>
#include <algorithm>
#include <deque>
#include <utility>

namespace NES {

Topology::Topology() : rootWorkerId(INVALID_WORKER_NODE_ID) {
    locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
}

TopologyPtr Topology::create() { return std::shared_ptr<Topology>(new Topology()); }

WorkerId Topology::getRootTopologyNodeId() { return rootWorkerId; }

void Topology::setRootTopologyNodeId(WorkerId workerId) { rootWorkerId = workerId; }

bool Topology::registerTopologyNode(WorkerId workerId,
                                    const std::string& address,
                                    const int64_t grpcPort,
                                    const int64_t dataPort,
                                    const uint16_t numberOfSlots,
                                    std::map<std::string, std::any> workerProperties) {

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    if (!lockedWorkerIdToTopologyNodeMap->contains(workerId)) {
        TopologyNodePtr newTopologyNode =
            TopologyNode::create(workerId, address, grpcPort, dataPort, numberOfSlots, workerProperties);
        NES_INFO("Adding New Node {} to the catalog of nodes.", newTopologyNode->toString());
        (*lockedWorkerIdToTopologyNodeMap)[workerId] = newTopologyNode;
        return true;
    }
    NES_WARNING("Topology node with id {} already exists. Failed to register the new topology node.", workerId);
    return false;
}

bool Topology::addTopologyNodeAsChild(WorkerId parentWorkerId, WorkerId childWorkerId) {

    if (parentWorkerId == childWorkerId) {
        NES_ERROR("Can not add link to itself");
        return false;
    }

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    if (!lockedWorkerIdToTopologyNodeMap->contains(parentWorkerId)) {
        NES_WARNING("No parent topology node with id {} registered.", parentWorkerId);
        return false;
    }
    if (!lockedWorkerIdToTopologyNodeMap->contains(childWorkerId)) {
        NES_WARNING("No child topology node with id {} registered.", childWorkerId);
        return false;
    }

    auto lockedParent = (*lockedWorkerIdToTopologyNodeMap)[parentWorkerId].rlock();
    auto children = (*lockedParent)->getChildren();
    for (const auto& child : children) {
        if (child->as<TopologyNode>()->getId() == childWorkerId) {
            NES_ERROR("TopologyManagerService::AddParent: nodes {} and {} already exists", childWorkerId, parentWorkerId);
            return false;
        }
    }
    auto lockedChild = (*lockedWorkerIdToTopologyNodeMap)[childWorkerId].rlock();
    NES_INFO("Adding Node {} as child to the node {}", (*lockedChild)->toString(), (*lockedParent)->toString());
    return (*lockedParent)->addChild((*lockedChild));
}

bool Topology::removeTopologyNode(WorkerId topologyNodeId) {

    NES_INFO("Removing Node {}", topologyNodeId);
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    if (!lockedWorkerIdToTopologyNodeMap->contains(topologyNodeId)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", topologyNodeId);
        return false;
    }

    if (rootWorkerId == topologyNodeId) {
        NES_WARNING("Removing the root node.");
    }

    auto lockedTopologyNode = (*lockedWorkerIdToTopologyNodeMap)[topologyNodeId].wlock();

    (*lockedTopologyNode)->removeAllParent();
    (*lockedTopologyNode)->removeChildren();
    lockedWorkerIdToTopologyNodeMap->erase(topologyNodeId);
    NES_DEBUG("Successfully removed the node.");
    return true;
}

TopologyNodePtr Topology::getCopyOfTopologyNodeWithId(WorkerId workerId) {
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    NES_INFO("Finding a physical node with id {}", workerId);
    if (lockedWorkerIdToTopologyNodeMap->contains(workerId)) {
        NES_DEBUG("Found a physical node with id {}", workerId);
        return (*(*lockedWorkerIdToTopologyNodeMap)[workerId].wlock())->copy();
    }
    NES_WARNING("Unable to find a physical node with id {}", workerId);
    return nullptr;
}

bool Topology::nodeWithWorkerIdExists(WorkerId workerId) {
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    return lockedWorkerIdToTopologyNodeMap->contains(workerId);
}

bool Topology::removeTopologyNodeAsChild(WorkerId parentWorkerId, WorkerId childWorkerId) {
    NES_INFO("Removing node {} as child to the node {}", childWorkerId, parentWorkerId);
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    if (!lockedWorkerIdToTopologyNodeMap->contains(parentWorkerId)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", parentWorkerId);
        return false;
    }

    if (!lockedWorkerIdToTopologyNodeMap->contains(childWorkerId)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", childWorkerId);
        return false;
    }

    auto lockedParentTopologyNode = (*lockedWorkerIdToTopologyNodeMap)[parentWorkerId].wlock();
    auto lockedChildTopologyNode = (*lockedWorkerIdToTopologyNodeMap)[childWorkerId].wlock();
    return (*lockedParentTopologyNode)->remove((*lockedChildTopologyNode));
}

bool Topology::occupySlots(WorkerId workerId, uint16_t amountToOccupy) {
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    NES_INFO("Reduce {} resources from node with id {}", amountToOccupy, workerId);
    if (lockedWorkerIdToTopologyNodeMap->contains(workerId)) {
        return (*(*lockedWorkerIdToTopologyNodeMap)[workerId].wlock())->occupySlots(amountToOccupy);
    }
    NES_WARNING("Unable to find node with id {}", workerId);
    return false;
}

bool Topology::releaseSlots(WorkerId workerId, uint16_t amountToRelease) {
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    NES_INFO("Increase {} resources from node with id {}", amountToRelease, workerId);
    if (lockedWorkerIdToTopologyNodeMap->contains(workerId)) {
        return (*(*lockedWorkerIdToTopologyNodeMap)[workerId].wlock())->releaseSlots(amountToRelease);
    }
    NES_WARNING("Unable to find node with id {}", workerId);
    return false;
}

TopologyNodeWLock Topology::lockTopologyNode(WorkerId workerId) {

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    if (lockedWorkerIdToTopologyNodeMap->contains(workerId)) {
        return std::make_unique<folly::Synchronized<TopologyNodePtr>::WLockedPtr>(
            (*lockedWorkerIdToTopologyNodeMap)[workerId].wlock());
    }
    NES_WARNING("Unable to locate topology node with id {}", workerId);
    return nullptr;
}

std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>>
Topology::getTopologyNodeIdsInRange(Spatial::DataTypes::Experimental::GeoLocation center, double radius) {
    return locationIndex->getNodeIdsInRange(center, radius);
}

NES::Spatial::Index::Experimental::LocationIndexPtr Topology::getLocationIndex() { return locationIndex; }

nlohmann::json Topology::toJson() {

    nlohmann::json topologyJson{};

    if (rootWorkerId == INVALID_WORKER_NODE_ID) {
        NES_WARNING("No root node found");
        return topologyJson;
    }

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    auto root = (*lockedWorkerIdToTopologyNodeMap)[rootWorkerId].rlock();
    std::deque<TopologyNodePtr> parentToAdd{(*root)};
    std::deque<TopologyNodePtr> childToAdd;

    std::vector<nlohmann::json> nodes = {};
    std::vector<nlohmann::json> edges = {};

    while (!parentToAdd.empty()) {
        // Current topology node to add to the JSON
        TopologyNodePtr currentNode = parentToAdd.front();
        nlohmann::json currentNodeJsonValue{};

        parentToAdd.pop_front();
        // Add properties for current topology node
        currentNodeJsonValue["id"] = currentNode->getId();
        currentNodeJsonValue["available_resources"] = currentNode->getAvailableResources();
        currentNodeJsonValue["ip_address"] = currentNode->getIpAddress();
        if (currentNode->getSpatialNodeType() != NES::Spatial::Experimental::SpatialType::MOBILE_NODE) {
            auto geoLocation = locationIndex->getGeoLocationForNode(currentNode->getId());
            auto locationInfo = nlohmann::json{};
            if (geoLocation.has_value() && geoLocation.value().isValid()) {
                locationInfo["latitude"] = geoLocation.value().getLatitude();
                locationInfo["longitude"] = geoLocation.value().getLongitude();
            }
            currentNodeJsonValue["location"] = locationInfo;
        }
        currentNodeJsonValue["nodeType"] = Spatial::Util::SpatialTypeUtility::toString(currentNode->getSpatialNodeType());

        auto children = currentNode->getChildren();
        for (const auto& child : children) {
            // Add edge information for current topology node
            nlohmann::json currentEdgeJsonValue{};
            currentEdgeJsonValue["source"] = child->as<TopologyNode>()->getId();
            currentEdgeJsonValue["target"] = currentNode->getId();
            edges.push_back(currentEdgeJsonValue);

            childToAdd.push_back(child->as<TopologyNode>());
        }

        if (parentToAdd.empty()) {
            parentToAdd.insert(parentToAdd.end(), childToAdd.begin(), childToAdd.end());
            childToAdd.clear();
        }

        nodes.push_back(currentNodeJsonValue);
    }
    NES_INFO("TopologyController: no more topology node to add");

    // add `nodes` and `edges` JSON array to the final JSON result
    topologyJson["nodes"] = nodes;
    topologyJson["edges"] = edges;
    return topologyJson;
}

std::string Topology::toString() {

    if (rootWorkerId == INVALID_WORKER_NODE_ID) {
        NES_WARNING("No root node found");
        return "";
    }

    std::stringstream topologyInfo;
    topologyInfo << std::endl;

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    auto root = (*lockedWorkerIdToTopologyNodeMap)[rootWorkerId].rlock();

    // store pair of TopologyNodePtr and its depth in when printed
    std::deque<std::pair<TopologyNodePtr, uint64_t>> parentToPrint{std::make_pair((*root), 0)};

    // indent offset
    int indent = 2;

    // perform dfs traverse
    while (!parentToPrint.empty()) {
        std::pair<TopologyNodePtr, uint64_t> nodeToPrint = parentToPrint.front();
        parentToPrint.pop_front();
        for (std::size_t i = 0; i < indent * nodeToPrint.second; i++) {
            if (i % indent == 0) {
                topologyInfo << '|';
            } else {
                if (i >= indent * nodeToPrint.second - 1) {
                    topologyInfo << std::string(indent, '-');
                } else {
                    topologyInfo << std::string(indent, ' ');
                }
            }
        }
        topologyInfo << nodeToPrint.first->toString() << std::endl;

        for (const auto& child : nodeToPrint.first->getChildren()) {
            parentToPrint.emplace_front(child->as<TopologyNode>(), nodeToPrint.second + 1);
        }
    }
    return topologyInfo.str();
}

void Topology::print() { NES_DEBUG("Topology print:{}", toString()); }

bool Topology::addGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    if (!lockedWorkerIdToTopologyNodeMap->contains(workerId)) {
        NES_ERROR("Unable to find node with id {}", workerId);
        return false;
    }

    auto lockedTopologyNode = (*lockedWorkerIdToTopologyNodeMap)[workerId].wlock();
    lockedWorkerIdToTopologyNodeMap.unlock();

    if (geoLocation.isValid()
        && (*lockedTopologyNode)->getSpatialNodeType() == Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: {}, {}", geoLocation.getLatitude(), geoLocation.getLongitude());
        locationIndex->initializeFieldNodeCoordinates(workerId, std::move(geoLocation));
    } else {
        NES_DEBUG("added node is a non field node");
        if ((*lockedTopologyNode)->getSpatialNodeType() == Spatial::Experimental::SpatialType::MOBILE_NODE) {
            locationIndex->addMobileNode(workerId, std::move(geoLocation));
            NES_DEBUG("added node is a mobile node");
        } else {
            NES_DEBUG("added node is a non mobile node");
        }
    }
    return true;
}

bool Topology::updateGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    if (!lockedWorkerIdToTopologyNodeMap->contains(workerId)) {
        NES_ERROR("Unable to find node with id {}", workerId);
        return false;
    }

    auto lockedTopologyNode = (*lockedWorkerIdToTopologyNodeMap)[workerId].wlock();
    lockedWorkerIdToTopologyNodeMap.unlock();

    if (geoLocation.isValid()
        && (*lockedTopologyNode)->getSpatialNodeType() == Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: {}, {}", geoLocation.getLatitude(), geoLocation.getLongitude());
        locationIndex->updateFieldNodeCoordinates(workerId, std::move(geoLocation));
    } else {
        NES_DEBUG("added node is a non field node");
        if ((*lockedTopologyNode)->getSpatialNodeType() == Spatial::Experimental::SpatialType::MOBILE_NODE) {
            locationIndex->addMobileNode(workerId, std::move(geoLocation));
            NES_DEBUG("added node is a mobile node");
        } else {
            NES_DEBUG("added node is a non mobile node");
        }
    }
    return true;
}

bool Topology::removeGeoLocation(WorkerId workerId) {
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    if (!lockedWorkerIdToTopologyNodeMap->contains(workerId)) {
        NES_ERROR("Unable to find node with id {}", workerId);
        return false;
    }
    lockedWorkerIdToTopologyNodeMap.unlock();
    return locationIndex->removeNodeFromSpatialIndex(workerId);
}

std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation> Topology::getGeoLocationForNode(WorkerId nodeId) {
    return locationIndex->getGeoLocationForNode(nodeId);
}

nlohmann::json Topology::requestNodeLocationDataAsJson(WorkerId workerId) {

    if (!nodeWithWorkerIdExists(workerId)) {
        return nullptr;
    }

    auto geoLocation = locationIndex->getGeoLocationForNode(workerId);
    if (geoLocation.has_value()) {
        return convertNodeLocationInfoToJson(workerId, geoLocation.value());
    } else {
        nlohmann::json nodeInfo;
        nodeInfo["id"] = workerId;
        return nodeInfo;
    }
}

nlohmann::json Topology::requestLocationAndParentDataFromAllMobileNodes() {
    auto nodeVector = locationIndex->getAllNodeLocations();
    auto locationMapJson = nlohmann::json::array();
    auto mobileEdgesJson = nlohmann::json::array();
    uint32_t count = 0;
    uint32_t edgeCount = 0;
    for (const auto& [nodeId, location] : nodeVector) {
        auto topologyNode = getCopyOfTopologyNodeWithId(nodeId);
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

nlohmann::json Topology::convertLocationToJson(NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation) {
    nlohmann::json locJson;
    if (geoLocation.isValid()) {
        locJson["latitude"] = geoLocation.getLatitude();
        locJson["longitude"] = geoLocation.getLongitude();
    }
    return locJson;
}

nlohmann::json Topology::convertNodeLocationInfoToJson(WorkerId workerId,
                                                       NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation) {
    nlohmann::json nodeInfo;
    nodeInfo["id"] = workerId;
    nlohmann::json locJson = convertLocationToJson(std::move(geoLocation));
    nodeInfo["location"] = locJson;
    return nodeInfo;
}

}// namespace NES