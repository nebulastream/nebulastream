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

#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Services/AbstractHealthCheckService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialTypeUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SpatialUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

TopologyManagerService::TopologyManagerService(TopologyPtr topology,
                                               NES::Spatial::Index::Experimental::LocationIndexPtr locationIndex)
    : topology(std::move(topology)), locationIndex(std::move(locationIndex)) {
    NES_DEBUG("TopologyManagerService()");
}

void TopologyManagerService::setHealthService(HealthCheckServicePtr healthCheckService) {
    this->healthCheckService = healthCheckService;
}

uint64_t TopologyManagerService::registerWorker(const std::string& address,
                                                const int64_t grpcPort,
                                                const int64_t dataPort,
                                                const uint64_t numberOfSlots,
                                                std::map<std::string, std::any> workerProperties,
                                                const uint64_t memoryCapacity,
                                                const uint64_t mtbfValue,
                                                const uint64_t launchTime,
                                                const uint64_t epochValue,
                                                const uint64_t ingestionRate,
                                                const uint64_t networkCapacity) {
    NES_TRACE("TopologyManagerService: Register Node address=" << address << " numberOfSlots=" << numberOfSlots);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    NES_DEBUG("TopologyManagerService::registerWorker: topology before insert");
    NES_DEBUG(topology->toString());

    if (topology->nodeExistsWithIpAndPort(address, grpcPort)) {
        NES_ERROR("TopologyManagerService::registerWorker: node with address " << address << " and grpc port " << grpcPort
                                                                               << " already exists");
        return INVALID_TOPOLOGY_NODE_ID;
    }

    NES_DEBUG("TopologyManagerService::registerWorker: register node");
    //get unique id for the new node
    uint64_t id = getNextTopologyNodeId();
    TopologyNodePtr newTopologyNode = TopologyNode::create(id,
                                                           address,
                                                           grpcPort,
                                                           dataPort,
                                                           numberOfSlots,
                                                           workerProperties,
                                                           memoryCapacity,
                                                           mtbfValue,
                                                           launchTime,
                                                           epochValue,
                                                           ingestionRate,
                                                           networkCapacity);

    if (!newTopologyNode) {
        NES_ERROR("TopologyManagerService::RegisterNode : node not created");
        return INVALID_TOPOLOGY_NODE_ID;
    }

    const TopologyNodePtr rootNode = topology->getRoot();

    if (!rootNode) {
        NES_DEBUG("TopologyManagerService::registerWorker: tree is empty so this becomes new root");
        topology->setAsRoot(newTopologyNode);
    } else {
        NES_DEBUG("TopologyManagerService::registerWorker: add link to the root node " << rootNode->toString());
        topology->addNewTopologyNodeAsChild(rootNode, newTopologyNode);
    }

//    if (healthCheckService) {
//        //add node to health check
//        healthCheckService->addNodeToHealthCheck(newTopologyNode);
//    }

    NES_DEBUG("TopologyManagerService::registerWorker: topology after insert = ");
    topology->print();
    return id;
}

bool TopologyManagerService::unregisterNode(uint64_t nodeId) {
    NES_DEBUG("TopologyManagerService::UnregisterNode: try to disconnect sensor with id " << nodeId);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);

    if (!physicalNode) {
        NES_ERROR("CoordinatorActor: node with id not found " << nodeId);
        return false;
    }

    if (healthCheckService) {
        //remove node to health check
        healthCheckService->removeNodeFromHealthCheck(physicalNode);
    }

    //todo: remove mobile nodes here too?
    auto spatialType = physicalNode->getSpatialNodeType();
    if (spatialType == NES::Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        removeGeoLocation(nodeId);
    }

    NES_DEBUG("TopologyManagerService::UnregisterNode: found sensor, try to delete it in toplogy");
    //remove from topology
    bool successTopology = topology->removePhysicalNode(physicalNode);
    NES_DEBUG("TopologyManagerService::UnregisterNode: success in topology is " << successTopology);

    return successTopology;
}

bool TopologyManagerService::addParent(uint64_t childId, uint64_t parentId) {
    NES_DEBUG("TopologyManagerService::addParent: childId=" << childId << " parentId=" << parentId);

    TopologyNodePtr childPhysicalNode = topology->findNodeWithId(childId);
    if (!childPhysicalNode) {
        NES_ERROR("TopologyManagerService::AddParent: source node " << childId << " does not exists");
        return false;
    }
    NES_DEBUG("TopologyManagerService::AddParent: source node " << childId << " exists");

    TopologyNodePtr parentPhysicalNode = topology->findNodeWithId(parentId);
    if (!parentPhysicalNode) {
        NES_ERROR("TopologyManagerService::AddParent: sensorParent node " << parentId << " does not exists");
        return false;
    }
    NES_DEBUG("TopologyManagerService::AddParent: sensorParent node " << parentId << " exists");
    LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

    childPhysicalNode->addLinkProperty(parentPhysicalNode, linkProperty);
    parentPhysicalNode->addLinkProperty(childPhysicalNode, linkProperty);
    childPhysicalNode->addNodeProperty("slots", 100);
    parentPhysicalNode->addNodeProperty("slots", 100);
    auto children = parentPhysicalNode->getChildren();
    for (const auto& child : children) {
        if (child->as<TopologyNode>()->getId() == childId) {
            NES_ERROR("TopologyManagerService::AddParent: nodes " << childId << " and " << parentId << " already exists");
            return false;
        }
    }
    bool added = topology->addNewTopologyNodeAsChild(parentPhysicalNode, childPhysicalNode);
    if (added) {
        NES_DEBUG("TopologyManagerService::AddParent: created link successfully new topology is=");
        topology->print();
        return true;
    }
    NES_ERROR("TopologyManagerService::AddParent: created NOT successfully added");
    return false;
}

bool TopologyManagerService::removeParent(uint64_t childId, uint64_t parentId) {
    NES_DEBUG("TopologyManagerService::removeParent: childId=" << childId << " parentId=" << parentId);

    TopologyNodePtr childNode = topology->findNodeWithId(childId);
    if (!childNode) {
        NES_ERROR("TopologyManagerService::removeParent: source node " << childId << " does not exists");
        return false;
    }
    NES_DEBUG("TopologyManagerService::removeParent: source node " << childId << " exists");

    TopologyNodePtr parentNode = topology->findNodeWithId(parentId);
    if (!parentNode) {
        NES_ERROR("TopologyManagerService::removeParent: sensorParent node " << childId << " does not exists");
        return false;
    }

    NES_DEBUG("TopologyManagerService::AddParent: sensorParent node " << parentId << " exists");

    std::vector<NodePtr> children = parentNode->getChildren();
    auto found = std::find_if(children.begin(), children.end(), [&childId](const NodePtr& node) {
        return node->as<TopologyNode>()->getId() == childId;
    });

    if (found == children.end()) {
        NES_ERROR("TopologyManagerService::removeParent: nodes " << childId << " and " << parentId << " are not connected");
        return false;
    }

    for (auto& child : children) {
        if (child->as<TopologyNode>()->getId() == childId) {
        }
    }

    NES_DEBUG("TopologyManagerService::removeParent: nodes connected");

    bool success = topology->removeNodeAsChild(parentNode, childNode);
    if (!success) {
        NES_ERROR("TopologyManagerService::removeParent: edge between  " << childId << " and " << parentId
                                                                         << " could not be removed");
        return false;
    }
    NES_DEBUG("TopologyManagerService::removeParent: successful");
    return true;
}

TopologyNodePtr TopologyManagerService::findNodeWithId(uint64_t nodeId) { return topology->findNodeWithId(nodeId); }

uint64_t TopologyManagerService::getNextTopologyNodeId() { return ++topologyNodeIdCounter; }

//TODO #2498 add functions here, that do not only search in a circular area, but make sure, that there are nodes found in every possible direction of future movement
std::vector<std::pair<uint64_t, Spatial::DataTypes::Experimental::GeoLocation>>
TopologyManagerService::getNodesIdsInRange(Spatial::DataTypes::Experimental::GeoLocation center, double radius) {
    return locationIndex->getNodeIdsInRange(center, radius);
}

TopologyNodePtr TopologyManagerService::getRootNode() { return topology->getRoot(); }

bool TopologyManagerService::removePhysicalNode(const TopologyNodePtr& nodeToRemove) {
    return topology->removePhysicalNode(nodeToRemove);
}

nlohmann::json TopologyManagerService::getTopologyAsJson() {
    NES_INFO("TopologyController: getting topology as JSON");

    nlohmann::json topologyJson{};
    auto root = topology->getRoot();
    std::deque<TopologyNodePtr> parentToAdd{std::move(root)};
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
            auto geoLocation = getGeoLocationForNode(currentNode->getId());
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

//All of these methods can be moved to Location service
bool TopologyManagerService::addGeoLocation(TopologyNodeId topologyNodeId,
                                            NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {

    auto topologyNode = topology->findNodeWithId(topologyNodeId);
    if (!topologyNode) {
        NES_ERROR("Unable to find node with id " << topologyNodeId);
        return false;
    }

    if (geoLocation.isValid() && topologyNode->getSpatialNodeType() == Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: " << geoLocation.getLatitude() << ", " << geoLocation.getLongitude());
        locationIndex->initializeFieldNodeCoordinates(topologyNodeId, std::move(geoLocation));
    } else {
        NES_DEBUG("added node is a non field node");
        if (topologyNode->getSpatialNodeType() == Spatial::Experimental::SpatialType::MOBILE_NODE) {
            locationIndex->addMobileNode(topologyNode->getId(), std::move(geoLocation));
            NES_DEBUG("added node is a mobile node");
        } else {
            NES_DEBUG("added node is a non mobile node");
        }
    }
    return true;
}

bool TopologyManagerService::updateGeoLocation(TopologyNodeId topologyNodeId,
                                               NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {

    auto topologyNode = topology->findNodeWithId(topologyNodeId);
    if (!topologyNode) {
        NES_ERROR("Unable to find node with id " << topologyNodeId);
        return false;
    }

    if (geoLocation.isValid() && topologyNode->getSpatialNodeType() == Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: " << geoLocation.getLatitude() << ", " << geoLocation.getLongitude());
        locationIndex->updateFieldNodeCoordinates(topologyNodeId, std::move(geoLocation));
    } else {
        NES_DEBUG("added node is a non field node");
        if (topologyNode->getSpatialNodeType() == Spatial::Experimental::SpatialType::MOBILE_NODE) {
            locationIndex->addMobileNode(topologyNode->getId(), std::move(geoLocation));
            NES_DEBUG("added node is a mobile node");
        } else {
            NES_DEBUG("added node is a non mobile node");
        }
    }
    return true;
}

bool TopologyManagerService::removeGeoLocation(TopologyNodeId topologyNodeId) {
    auto topologyNode = topology->findNodeWithId(topologyNodeId);
    if (!topologyNode) {
        NES_ERROR("Unable to find node with id " << topologyNodeId);
        return false;
    }
    return locationIndex->removeNodeFromSpatialIndex(topologyNodeId);
}

std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>
TopologyManagerService::getGeoLocationForNode(TopologyNodeId nodeId) {
    return locationIndex->getGeoLocationForNode(nodeId);
}
}// namespace NES
