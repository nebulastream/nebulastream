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
            // node is re-registering (was inactive and became active again)
            NES_TRACE("TopologyManagerService::registerWorker: node with worker id {} is reregistering", workerId);
            id = workerId;
            healthCheckService->removeNodeFromHealthCheck(id);
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

    if (!topology->registerTopologyNode(workerId, address, grpcPort, dataPort, numberOfSlots, workerProperties)) {
        NES_ERROR("TopologyManagerService::RegisterNode : node not created");
        return INVALID_WORKER_NODE_ID;
    }

    auto rootTopologyNodeId = topology->getRootTopologyNodeId();

    if (rootTopologyNodeId == INVALID_WORKER_NODE_ID) {
        NES_DEBUG("TopologyManagerService::registerWorker: tree is empty so this becomes new root");
        topology->setRootTopologyNodeId(workerId);
    } else {
        NES_DEBUG("TopologyManagerService::registerWorker: add link to the root node {}", rootTopologyNodeId);
        topology->addTopologyNodeAsChild(rootTopologyNodeId, workerId);
    }

    if (healthCheckService) {
        std::string grpcAddress = address + ":" + std::to_string(grpcPort);
        //add node to health check
        healthCheckService->addNodeToHealthCheck(workerId, grpcAddress);
    }

    NES_DEBUG("TopologyManagerService::registerWorker: topology after insert = ");
    topology->print();
    return id;
}

bool TopologyManagerService::unregisterNode(WorkerId workerId) {

    NES_DEBUG("TopologyManagerService::UnregisterNode: try to disconnect sensor with id  {}", workerId);
    TopologyNodePtr physicalNode = topology->getCopyOfTopologyNodeWithId(workerId);
    if (!topology->nodeWithWorkerIdExists(workerId)) {
        NES_ERROR("Topology node with id not found  {}", workerId);
        return false;
    }

    if (healthCheckService) {
        //remove node to health check
        healthCheckService->removeNodeFromHealthCheck(workerId);
    }

    //todo: remove mobile nodes here too?
    auto spatialType = physicalNode->getSpatialNodeType();
    if (spatialType == NES::Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        topology->removeGeoLocation(workerId);
    }

    NES_DEBUG("TopologyManagerService::UnregisterNode: found sensor, try to delete it in toplogy");
    //remove from topology
    bool successTopology = topology->removeTopologyNode(workerId);
    NES_DEBUG("TopologyManagerService::UnregisterNode: success in topology is  {}", successTopology);

    return successTopology;
}

bool TopologyManagerService::addParent(WorkerId childId, WorkerId parentId) {
    NES_DEBUG("TopologyManagerService::addParent: childId= {}  parentId= {}", childId, parentId);

    bool added = topology->addTopologyNodeAsChild(parentId, childId);
    if (added) {
        NES_DEBUG("TopologyManagerService::AddParent: created link successfully new topology is=");
        topology->print();
        return true;
    }
    NES_ERROR("TopologyManagerService::AddParent: created NOT successfully added");
    return false;
}

bool TopologyManagerService::removeAsParent(WorkerId childId, WorkerId parentId) {
    bool success = topology->removeTopologyNodeAsChild(parentId, childId);
    if (!success) {
        NES_ERROR("TopologyManagerService::removeAsParent: edge between {} and {} could not be removed", childId, parentId);
        return false;
    }
    NES_DEBUG("TopologyManagerService::removeAsParent: successful");
    return true;
}

TopologyNodePtr TopologyManagerService::findNodeWithId(uint64_t nodeId) { return topology->getCopyOfTopologyNodeWithId(nodeId); }

WorkerId TopologyManagerService::getNextWorkerId() { return ++topologyNodeIdCounter; }

//TODO #2498 add functions here, that do not only search in a circular area, but make sure, that there are nodes found in every possible direction of future movement
std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>>
TopologyManagerService::getTopologyNodeIdsInRange(Spatial::DataTypes::Experimental::GeoLocation center, double radius) {
    return topology->getTopologyNodeIdsInRange(center, radius);
}

WorkerId TopologyManagerService::getRootTopologyNodeId() { return topology->getRootTopologyNodeId(); }

bool TopologyManagerService::removeTopologyNode(WorkerId nodeToRemove) { return topology->removeTopologyNode(nodeToRemove); }

nlohmann::json TopologyManagerService::getTopologyAsJson() {
    NES_INFO("TopologyController: getting topology as JSON");
    return topology->toJson();
}

bool TopologyManagerService::addGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {
    return topology->addGeoLocation(workerId, std::move(geoLocation));
}

bool TopologyManagerService::updateGeoLocation(WorkerId workerId,
                                               NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {
    return topology->updateGeoLocation(workerId, std::move(geoLocation));
}

bool TopologyManagerService::removeGeoLocation(WorkerId workerId) { return topology->removeGeoLocation(workerId); }

std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>
TopologyManagerService::getGeoLocationForNode(WorkerId workerId) {
    return topology->getGeoLocationForNode(workerId);
}
}// namespace NES
