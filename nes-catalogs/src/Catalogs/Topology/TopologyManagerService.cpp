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
#include <Catalogs/Topology/AbstractHealthCheckService.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyManagerService.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/SpatialUtils.hpp>
#include <utility>

namespace NES {

TopologyManagerService::TopologyManagerService(const TopologyPtr& topology) : topology(topology) {
    NES_DEBUG("TopologyManagerService()");
}

void TopologyManagerService::setHealthService(const HealthCheckServicePtr& healthCheckService) {
    this->healthCheckService = healthCheckService;
}

WorkerId TopologyManagerService::registerWorker(WorkerId workerId,
                                                const std::string& address,
                                                const int64_t grpcPort,
                                                const int64_t dataPort,
                                                const uint16_t numberOfSlots,
                                                std::map<std::string, std::any> workerProperties) {
    NES_TRACE("TopologyManagerService: Register Node address={} numberOfSlots={}", address, numberOfSlots);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    NES_DEBUG("TopologyManagerService::registerWorker: topology before insert");
    NES_DEBUG("", topology->toString());

    WorkerId id;

    // if worker is started with a workerId
    if (workerId != INVALID_WORKER_NODE_ID) {
        // check if an active worker with workerId already exists
        if (topology->nodeWithWorkerIdExists(workerId)) {
            NES_WARNING("TopologyManagerService::registerWorker: node with worker id {} already exists and is running. A new "
                        "worker id will be assigned.",
                        workerId);
            id = getNextWorkerId();
        }
        // check if an inactive worker with workerId already exists
        else if (healthCheckService && healthCheckService->isWorkerInactive(workerId)) {
            // node is reregistering (was inactive and became active again)
            NES_TRACE("TopologyManagerService::registerWorker: node with worker id {} is reregistering", workerId);
            id = workerId;
            TopologyNodePtr workerWithOldConfig = healthCheckService->getWorkerByWorkerId(id);
            if (workerWithOldConfig) {
                healthCheckService->removeNodeFromHealthCheck(workerWithOldConfig);
            }
        } else {
            // there is no active worker with workerId and there is no inactive worker with workerId, therefore
            // simply assign next available workerId
            id = getNextWorkerId();
        }
    }

    if (workerId == INVALID_WORKER_NODE_ID) {
        // worker does not have a workerId yet => assign next available workerId
        id = getNextWorkerId();
    }

    NES_DEBUG("TopologyManagerService::registerWorker: register node");

    TopologyNodePtr newTopologyNode = TopologyNode::create(id, address, grpcPort, dataPort, numberOfSlots, workerProperties);

    if (!newTopologyNode) {
        NES_ERROR("TopologyManagerService::RegisterNode : node not created");
        return INVALID_WORKER_NODE_ID;
    }

    topology->registerTopologyNode(std::move(newTopologyNode));

    auto rootTopologyNodeId = topology->getRootTopologyNodeId();

    if (rootTopologyNodeId == INVALID_WORKER_NODE_ID) {
        NES_DEBUG("TopologyManagerService::registerWorker: tree is empty so this becomes new root");
        topology->setRootTopologyNodeId(newTopologyNode);
    } else {
        NES_DEBUG("TopologyManagerService::registerWorker: add link to the root node {}", rootTopologyNodeId);
        topology->addTopologyNodeAsChild(rootTopologyNodeId, newTopologyNode->getId());
    }

    if (healthCheckService) {
        //add node to health check
        healthCheckService->addNodeToHealthCheck(newTopologyNode);
    }

    NES_DEBUG("TopologyManagerService::registerWorker: topology after insert = ");
    topology->print();
    return id;
}

bool TopologyManagerService::unregisterNode(WorkerId nodeId) {

    NES_DEBUG("TopologyManagerService::UnregisterNode: try to disconnect sensor with id  {}", nodeId);
    TopologyNodePtr physicalNode = topology->findWorkerWithId(nodeId);
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
    bool successTopology = topology->removeTopologyNode(physicalNode);
    NES_DEBUG("TopologyManagerService::UnregisterNode: success in topology is  {}", successTopology);

    return successTopology;
}

bool TopologyManagerService::addParent(uint64_t childId, uint64_t parentId) {
    NES_DEBUG("TopologyManagerService::addParent: childId= {}  parentId= {}", childId, parentId);

    if (childId == parentId) {
        NES_ERROR("TopologyManagerService::AddParent: cannot add link to itself");
        return false;
    }

    NES_DEBUG("TopologyManagerService::AddParent: sensorParent node  {}  exists", parentId);

    bool added = topology->addTopologyNodeAsChild(parentId, childId);
    if (added) {
        NES_DEBUG("TopologyManagerService::AddParent: created link successfully new topology is=");
        topology->print();
        return true;
    }
    NES_ERROR("TopologyManagerService::AddParent: created NOT successfully added");
    return false;
}

bool TopologyManagerService::removeAsParent(uint64_t childId, uint64_t parentId) {
    NES_DEBUG("TopologyManagerService::removeAsParent: childId= {}  parentId= {}", childId, parentId);

    TopologyNodePtr childNode = topology->findWorkerWithId(childId);
    if (!childNode) {
        NES_ERROR("TopologyManagerService::removeAsParent: source node {} does not exists", childId);
        return false;
    }
    NES_DEBUG("TopologyManagerService::removeAsParent: source node  {}  exists", childId);

    TopologyNodePtr parentNode = topology->findWorkerWithId(parentId);
    if (!parentNode) {
        NES_ERROR("TopologyManagerService::removeAsParent: sensorParent node {} does not exists", childId);
        return false;
    }

    NES_DEBUG("TopologyManagerService::AddParent: sensorParent node  {}  exists", parentId);

    std::vector<NodePtr> children = parentNode->getChildren();
    auto found = std::find_if(children.begin(), children.end(), [&childId](const NodePtr& node) {
        return node->as<TopologyNode>()->getId() == childId;
    });

    if (found == children.end()) {
        NES_ERROR("TopologyManagerService::removeAsParent: nodes {} and {} are not connected", childId, parentId);
        return false;
    }

    for (auto& child : children) {
        if (child->as<TopologyNode>()->getId() == childId) {
        }
    }

    NES_DEBUG("TopologyManagerService::removeAsParent: nodes connected");

    bool success = topology->removeTopologyNodeAsChild(parentNode, childNode);
    if (!success) {
        NES_ERROR("TopologyManagerService::removeAsParent: edge between {} and {} could not be removed", childId, parentId);
        return false;
    }
    NES_DEBUG("TopologyManagerService::removeAsParent: successful");
    return true;
}

TopologyNodePtr TopologyManagerService::findNodeWithId(uint64_t nodeId) { return topology->findWorkerWithId(nodeId); }

WorkerId TopologyManagerService::getNextWorkerId() { return ++topologyNodeIdCounter; }

//TODO #2498 add functions here, that do not only search in a circular area, but make sure, that there are nodes found in every possible direction of future movement
std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>>
TopologyManagerService::getTopologyNodeIdsInRange(Spatial::DataTypes::Experimental::GeoLocation center, double radius) {
    return topology->getTopologyNodeIdsInRange(center, radius);
}

WorkerId TopologyManagerService::getRootTopologyNodeId() { return topology->getRootTopologyNodeId(); }

bool TopologyManagerService::removePhysicalNode(const TopologyNodePtr& nodeToRemove) {
    return topology->removeTopologyNode(nodeToRemove);
}

nlohmann::json TopologyManagerService::getTopologyAsJson() {
    NES_INFO("TopologyController: getting topology as JSON");
    topology->toJson();
}

//All of these methods can be moved to Location service
bool TopologyManagerService::addGeoLocation(WorkerId topologyNodeId,
                                            NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {

    auto topologyNode = topology->findWorkerWithId(topologyNodeId);
    if (!topology->nodeWithWorkerIdExists(topologyNodeId)) {
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

bool TopologyManagerService::updateGeoLocation(WorkerId topologyNodeId,
                                               NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {

    auto topologyNode = topology->findWorkerWithId(topologyNodeId);
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

bool TopologyManagerService::removeGeoLocation(WorkerId topologyNodeId) {
    auto topologyNode = topology->findWorkerWithId(topologyNodeId);
    if (!topologyNode) {
        NES_ERROR("Unable to find node with id {}", topologyNodeId);
        return false;
    }
    return locationIndex->removeNodeFromSpatialIndex(topologyNodeId);
}

std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation> TopologyManagerService::getGeoLocationForNode(WorkerId nodeId) {
    return locationIndex->getGeoLocationForNode(nodeId);
}
}// namespace NES
