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
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Services/AbstractHealthCheckService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

TopologyManagerService::TopologyManagerService(TopologyPtr topology) : topology(std::move(topology)) {
    NES_DEBUG("TopologyManagerService()");
}
void TopologyManagerService::setHealthService(HealthCheckServicePtr healthCheckService) {
    this->healthCheckService = healthCheckService;
}

uint64_t TopologyManagerService::registerNode(const std::string& address,
                                              int64_t grpcPort,
                                              int64_t dataPort,
                                              uint16_t numberOfSlots,
                                              Spatial::Index::Experimental::Location fixedCoordinates,
                                              Spatial::Index::Experimental::NodeType spatialType,
                                              bool isTfInstalled) {
    NES_TRACE("TopologyManagerService: Register Node address=" << address << " numberOfSlots=" << numberOfSlots);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    NES_DEBUG("TopologyManagerService::registerNode: topology before insert");
    topology->print();

    if (topology->nodeExistsWithIpAndPort(address, grpcPort)) {
        NES_ERROR("TopologyManagerService::registerNode: node with address " << address << " and grpc port " << grpcPort
                                                                             << " already exists");
        return false;
    }

    NES_DEBUG("TopologyManagerService::registerNode: register node");
    //get unique id for the new node
    uint64_t id = getNextTopologyNodeId();
    TopologyNodePtr newTopologyNode = TopologyNode::create(id, address, grpcPort, dataPort, numberOfSlots);
    newTopologyNode->setSpatialNodeType(spatialType);
    newTopologyNode->addNodeProperty("tf_installed", isTfInstalled);

    if (!newTopologyNode) {
        NES_ERROR("TopologyManagerService::RegisterNode : node not created");
        return 0;
    }

    const TopologyNodePtr rootNode = topology->getRoot();

    if (!rootNode) {
        NES_DEBUG("TopologyManagerService::registerNode: tree is empty so this becomes new root");
        topology->setAsRoot(newTopologyNode);
    } else {
        NES_DEBUG("TopologyManagerService::registerNode: add link to the root node " << rootNode->toString());
        topology->addNewTopologyNodeAsChild(rootNode, newTopologyNode);
    }

    if (fixedCoordinates.isValid()
        && newTopologyNode->getSpatialNodeType() == Spatial::Index::Experimental::NodeType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: " << fixedCoordinates.getLatitude() << ", "
                                                            << fixedCoordinates.getLongitude());
        topology->getLocationIndex()->initializeFieldNodeCoordinates(newTopologyNode, fixedCoordinates);
    } else {
        NES_DEBUG("added node is a non field node");
        if (newTopologyNode->getSpatialNodeType() == Spatial::Index::Experimental::NodeType::MOBILE_NODE) {
            topology->getLocationIndex()->addMobileNode(newTopologyNode);
            NES_DEBUG("added node is a mobile node");
        } else {
            NES_DEBUG("added node is a non mobile node");
        }
    }

    if (healthCheckService) {
        //add node to health check
        healthCheckService->addNodeToHealthCheck(newTopologyNode);
    }

    NES_DEBUG("TopologyManagerService::registerNode: topology after insert = ");
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

    topology->getLocationIndex()->removeNodeFromSpatialIndex(physicalNode);

    NES_DEBUG("TopologyManagerService::UnregisterNode: found sensor, try to delete it in toplogy");
    //remove from topology
    bool successTopology = topology->removePhysicalNode(physicalNode);
    NES_DEBUG("TopologyManagerService::UnregisterNode: success in topology is " << successTopology);

    return successTopology;
}

bool TopologyManagerService::addParent(uint64_t childId, uint64_t parentId) {
    NES_DEBUG("TopologyManagerService::addParent: childId=" << childId << " parentId=" << parentId);

    if (childId == parentId) {
        NES_ERROR("TopologyManagerService::AddParent: cannot add link to itself");
        return false;
    }

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

    for (const auto& child : parentPhysicalNode->getChildren()) {
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

//TODO #2498 add functions here, that do not only search in a circular area, but make sure, that there are nodes found in every possible direction of furture movement

std::vector<std::pair<TopologyNodePtr, Spatial::Index::Experimental::Location>>
TopologyManagerService::getNodesInRange(Spatial::Index::Experimental::Location center, double radius) {
    return topology->getLocationIndex()->getNodesInRange(center, radius);
}

std::vector<std::pair<uint64_t, Spatial::Index::Experimental::Location>>
TopologyManagerService::getNodesIdsInRange(Spatial::Index::Experimental::Location center, double radius) {
    auto list = getNodesInRange(center, radius);
    std::vector<std::pair<uint64_t, Spatial::Index::Experimental::Location>> nodeIDsInRange{};
    nodeIDsInRange.reserve(list.size());
    for (auto elem : list) {
        nodeIDsInRange.emplace_back(std::pair(elem.first->getId(), elem.second));
    }
    return nodeIDsInRange;
}

TopologyNodePtr TopologyManagerService::getRootNode() { return topology->getRoot(); }

bool TopologyManagerService::removePhysicalNode(const TopologyNodePtr& nodeToRemove) {
    return topology->removePhysicalNode(nodeToRemove);
}
}// namespace NES
