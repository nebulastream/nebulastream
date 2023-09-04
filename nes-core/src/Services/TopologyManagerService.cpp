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
#include <Util/Core.hpp>
#include <Util/Experimental/SpatialTypeUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SpatialUtils.hpp>
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

TopologyNodeId TopologyManagerService::registerWorker(TopologyNodeId workerId,
                                                      const std::string& address,
                                                      const int64_t grpcPort,
                                                      const int64_t dataPort,
                                                      const uint16_t numberOfSlots,
                                                      std::map<std::string, std::any> workerProperties) {
    NES_TRACE("TopologyManagerService: Register Node address={} numberOfSlots={}", address, numberOfSlots);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    NES_DEBUG("TopologyManagerService::registerWorker: topology before insert");
    NES_DEBUG("", topology->toString());

    TopologyNodeId id;

    // if worker is started with a workerId
    if (workerId != INVALID_TOPOLOGY_NODE_ID) {
        // check if an active worker with workerId already exists
        if (topology->nodeWithWorkerIdExists(workerId)) {
            NES_WARNING("TopologyManagerService::registerWorker: node with worker id {} already exists and is running. A new "
                        "worker id will be assigned.",
                        workerId);
            id = getNextTopologyNodeId();
        }
        // check if an inactive worker with workerId already exists
        else if (wasAnnouncedFailedWorker(workerId)) {
            // node is reregistering (was inactive and became active again)
            NES_TRACE("TopologyManagerService::registerWorker: node with worker id {} is reregistering", workerId);
            id = workerId;
            TopologyNodePtr workerWithOldConfig = healthCheckService->getWorkerByWorkerId(id);
            if (workerWithOldConfig) {
                healthCheckService->removeNodeFromHealthCheck(workerWithOldConfig);
            }
            removeAnnouncedFailedWorker(workerId);
        } else {
            // there is no active worker with workerId and there is no inactive worker with workerId, therefore
            // simply assign next available workerId
            id = getNextTopologyNodeId();
        }
    }

    if (workerId == INVALID_TOPOLOGY_NODE_ID) {
        // worker does not have a workerId yet => assign next available workerId
        id = getNextTopologyNodeId();
    }

    NES_DEBUG("TopologyManagerService::registerWorker: register node");

    TopologyNodePtr newTopologyNode = TopologyNode::create(id, address, grpcPort, dataPort, numberOfSlots, workerProperties);

    if (!newTopologyNode) {
        NES_ERROR("TopologyManagerService::RegisterNode : node not created");
        return INVALID_TOPOLOGY_NODE_ID;
    }

    const TopologyNodePtr rootNode = topology->getRoot();

    if (!rootNode) {
        NES_DEBUG("TopologyManagerService::registerWorker: tree is empty so this becomes new root");
        topology->setAsRoot(newTopologyNode);
    } else {
        NES_DEBUG("TopologyManagerService::registerWorker: add link to the root node {}", rootNode->toString());
        topology->addNewTopologyNodeAsChild(rootNode, newTopologyNode);
    }

    if (healthCheckService) {
        //add node to health check
        healthCheckService->addNodeToHealthCheck(newTopologyNode);
    }

    NES_DEBUG("TopologyManagerService::registerWorker: topology after insert = ");
    topology->print();
    return id;
}

bool TopologyManagerService::unregisterNode(uint64_t nodeId) {
    NES_DEBUG("TopologyManagerService::UnregisterNode: try to disconnect sensor with id  {}", nodeId);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);

    if (!physicalNode) {
        NES_ERROR("CoordinatorActor: node with id not found  {}", nodeId);
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
    NES_DEBUG("TopologyManagerService::UnregisterNode: success in topology is  {}", successTopology);

    return successTopology;
}

bool TopologyManagerService::addParent(uint64_t childId, uint64_t parentId) {
    NES_DEBUG("TopologyManagerService::addParent: childId= {}  parentId= {}", childId, parentId);

    if (childId == parentId) {
        NES_ERROR("TopologyManagerService::AddParent: cannot add link to itself");
        return false;
    }

    TopologyNodePtr childPhysicalNode = topology->findNodeWithId(childId);
    if (!childPhysicalNode) {
        NES_ERROR("TopologyManagerService::AddParent: source node {} does not exists", childId);
        return false;
    }
    NES_DEBUG("TopologyManagerService::AddParent: source node {} exists", childId);

    TopologyNodePtr parentPhysicalNode = topology->findNodeWithId(parentId);
    if (!parentPhysicalNode) {
        NES_ERROR("TopologyManagerService::AddParent: sensorParent node {} does not exists", parentId);
        return false;
    }
    NES_DEBUG("TopologyManagerService::AddParent: sensorParent node  {}  exists", parentId);

    auto children = parentPhysicalNode->getChildren();
    for (const auto& child : children) {
        if (child->as<TopologyNode>()->getId() == childId) {
            NES_ERROR("TopologyManagerService::AddParent: nodes {} and {} already exists", childId, parentId);
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
    NES_DEBUG("TopologyManagerService::removeParent: childId= {}  parentId= {}", childId, parentId);

    TopologyNodePtr childNode = topology->findNodeWithId(childId);
    if (!childNode) {
        NES_ERROR("TopologyManagerService::removeParent: source node {} does not exists", childId);
        return false;
    }
    NES_DEBUG("TopologyManagerService::removeParent: source node  {}  exists", childId);

    TopologyNodePtr parentNode = topology->findNodeWithId(parentId);
    if (!parentNode) {
        NES_ERROR("TopologyManagerService::removeParent: sensorParent node {} does not exists", childId);
        return false;
    }

    NES_DEBUG("TopologyManagerService::AddParent: sensorParent node  {}  exists", parentId);

    std::vector<NodePtr> children = parentNode->getChildren();
    auto found = std::find_if(children.begin(), children.end(), [&childId](const NodePtr& node) {
        return node->as<TopologyNode>()->getId() == childId;
    });

    if (found == children.end()) {
        NES_ERROR("TopologyManagerService::removeParent: nodes {} and {} are not connected", childId, parentId);
        return false;
    }

    for (auto& child : children) {
        if (child->as<TopologyNode>()->getId() == childId) {
        }
    }

    NES_DEBUG("TopologyManagerService::removeParent: nodes connected");

    bool success = topology->removeNodeAsChild(parentNode, childNode);
    if (!success) {
        NES_ERROR("TopologyManagerService::removeParent: edge between {} and {} could not be removed", childId, parentId);
        return false;
    }
    NES_DEBUG("TopologyManagerService::removeParent: successful");
    return true;
}

TopologyNodePtr TopologyManagerService::findNodeWithId(uint64_t nodeId) { return topology->findNodeWithId(nodeId); }

TopologyNodeId TopologyManagerService::getNextTopologyNodeId() { return ++topologyNodeIdCounter; }

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
        NES_ERROR("Unable to find node with id {}", topologyNodeId);
        return false;
    }

    if (geoLocation.isValid() && topologyNode->getSpatialNodeType() == Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: {}, {}", geoLocation.getLatitude(), geoLocation.getLongitude());
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
        NES_ERROR("Unable to find node with id {}", topologyNodeId);
        return false;
    }

    if (geoLocation.isValid() && topologyNode->getSpatialNodeType() == Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: {}, {}", geoLocation.getLatitude(), geoLocation.getLongitude());
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
        NES_ERROR("Unable to find node with id {}", topologyNodeId);
        return false;
    }
    return locationIndex->removeNodeFromSpatialIndex(topologyNodeId);
}

std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>
TopologyManagerService::getGeoLocationForNode(TopologyNodeId nodeId) {
    return locationIndex->getGeoLocationForNode(nodeId);
}

bool TopologyManagerService::wasAnnouncedFailedWorker(TopologyNodeId workerId) {
    return announcedFailedWorkers.find(workerId) != announcedFailedWorkers.end();
}

void TopologyManagerService::addAnnouncedFailedWorker(TopologyNodeId workerId) {
    announcedFailedWorkers.insert(workerId);
}

void TopologyManagerService::removeAnnouncedFailedWorker(TopologyNodeId workerId) {
    if (announcedFailedWorkers.find(workerId) != announcedFailedWorkers.end()) {
        announcedFailedWorkers.erase(workerId);
    } else {
        NES_DEBUG("TopologyManagerService::removeAnnouncedFailedWorker could not remove workerId={} from announcedFailedWorkers set because it is not present", workerId);
    }
}

void TopologyManagerService::addZoneLeader(TopologyNodeId zoneLeaderWorkerId) {
    zoneLeaders.insert(zoneLeaderWorkerId);
}

bool TopologyManagerService::isZoneLeader(TopologyNodeId workerId) {
    return zoneLeaders.find(workerId) != zoneLeaders.end();
}

void TopologyManagerService::removeZoneLeader(TopologyNodeId zoneLeaderWorkerId) {
    if (isZoneLeader(zoneLeaderWorkerId)) {
        zoneLeaders.erase(zoneLeaderWorkerId);
    }
}

void TopologyManagerService::addEntryInLeaderToZoneMap(TopologyNodeId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation centerZone) {
    leaderToZoneMap.insert(workerId, centerZone);
}

NES::Spatial::DataTypes::Experimental::GeoLocation TopologyManagerService::findZoneByLeader(TopologyNodeId workerId){
    return leaderToZoneMap.find(workerId);
}

void TopologyManagerService::splitTopologyIntoZones() {
    NES_DEBUG("TopologyManagerService::splitting topology into geographical zones");

    // === ZONE 1 ===
    NES::Spatial::DataTypes::Experimental::GeoLocation centerZone1(13.001, 13.001);
    auto nodesInZone1 = getNodesIdsInRange(centerZone1, 10);
    std::vector<TopologyNodePtr> workersInZone1;
    for (auto nodeInZone1 : nodesInZone1) {
        NES_DEBUG("Found node in zone1, workerId {}", nodeInZone1.first);
        auto worker = findNodeWithId(nodeInZone1.first);
        workersInZone1.push_back(worker);
    }
    auto zone1Leader = electLeaderInZone(workersInZone1);
    NES_DEBUG("Elected leader in zone 1 with workerId {}", zone1Leader->getId());
    addEntryInLeaderToZoneMap(zone1Leader->getId(), centerZone1);
    healthCheckService->addWorkerAsZoneLeader(zone1Leader->getId());
    addZoneLeader(zone1Leader->getId());

//    // === ZONE 2 ===
//    NES::Spatial::DataTypes::Experimental::GeoLocation centerZone2(33.001, 33.001);
//    auto nodesInZone2 = getNodesIdsInRange(centerZone2, 10);
//    std::vector<TopologyNodePtr> workersInZone2;
//    for (auto nodeInZone2 : nodesInZone2) {
//        NES_DEBUG("Found node in zone2, workerId {}", nodeInZone2.first);
//        auto worker = findNodeWithId(nodeInZone2.first);
//        workersInZone2.push_back(worker);
//    }
//    auto zone2Leader = electLeaderInZone(workersInZone2);
//    NES_DEBUG("Elected leader in zone 2 with workerId {}", zone2Leader->getId());
//    addEntryInLeaderToZoneMap(zone2Leader->getId(), centerZone2);
//    healthCheckService->addWorkerAsZoneLeader(zone2Leader->getId());
//    addZoneLeader(zone2Leader->getId());


//    // === ZONE 3 ===
//    NES::Spatial::DataTypes::Experimental::GeoLocation centerZone3(53.001, 53.001);
//    auto nodesInZone3 = getNodesIdsInRange(centerZone3, 10);
//    std::vector<TopologyNodePtr> workersInZone3;
//    for (auto nodeInZone3 : nodesInZone3) {
//        NES_DEBUG("Found node in zone3, workerId {}", nodeInZone3.first);
//        auto worker = findNodeWithId(nodeInZone3.first);
//        workersInZone3.push_back(worker);
//    }
//    auto zone3Leader = electLeaderInZone(workersInZone3);
//    NES_DEBUG("Elected leader in zone 3 with workerId {}", zone3Leader->getId());
//    addEntryInLeaderToZoneMap(zone3Leader->getId(), centerZone3);
//    healthCheckService->addWorkerAsZoneLeader(zone3Leader->getId());
//    addZoneLeader(zone3Leader->getId());
//
//    // === ZONE 4 ===
//    NES::Spatial::DataTypes::Experimental::GeoLocation centerZone4(73.001, 73.001);
//    auto nodesInZone4 = getNodesIdsInRange(centerZone4, 10);
//    std::vector<TopologyNodePtr> workersInZone4;
//    for (auto nodeInZone4 : nodesInZone4) {
//        NES_DEBUG("Found node in zone 4, workerId {}", nodeInZone4.first);
//        auto worker = findNodeWithId(nodeInZone4.first);
//        workersInZone4.push_back(worker);
//    }
//    auto zone4Leader = electLeaderInZone(workersInZone4);
//    NES_DEBUG("Elected leader in zone 4 with workerId {}", zone4Leader->getId());
//    addEntryInLeaderToZoneMap(zone4Leader->getId(), centerZone4);
//    healthCheckService->addWorkerAsZoneLeader(zone4Leader->getId());
//    addZoneLeader(zone4Leader->getId());

    // start coordinator health check
    healthCheckService->startHealthCheck();
}

struct less_than_key
{
    inline bool operator() (const TopologyNodePtr& struct1, const TopologyNodePtr& struct2)
    {
        return (struct1->getAvailableResources() > struct2->getAvailableResources());
    }
};

TopologyNodePtr TopologyManagerService::electLeaderInZone(std::vector<TopologyNodePtr> workersInZone) {
    std::sort(workersInZone.begin(), workersInZone.end(), less_than_key());
    return workersInZone.at(0);
}

void TopologyManagerService::reelectLeaderInZone(TopologyNodeId oldLeaderWorkerId) {
    NES_DEBUG("TopologyManagerService::electing a new leader for zone with old leader workerId {}", oldLeaderWorkerId);
    auto zone = findZoneByLeader(oldLeaderWorkerId);
    auto workerIds = getNodesIdsInRange(zone, 10);

    std::vector<TopologyNodePtr> workers;
    for (auto workerId : workerIds) {
        if (workerId.first != oldLeaderWorkerId) {
            workers.push_back(findNodeWithId(workerId.first));
        }
    }

    removeZoneLeader(oldLeaderWorkerId);
    addAnnouncedFailedWorker(oldLeaderWorkerId);

    TopologyNodePtr newLeader = electLeaderInZone(workers);
    healthCheckService->removeWorkerAsZoneLeader(oldLeaderWorkerId);
    healthCheckService->addWorkerAsZoneLeader(newLeader->getId());
    addZoneLeader(newLeader->getId());
    addEntryInLeaderToZoneMap(newLeader->getId(), zone);
}

}// namespace NES
