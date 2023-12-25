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
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.rlock();
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

std::vector<TopologyNodePtr> Topology::findPathBetween(const std::vector<WorkerId>& sourceTopologyNodeIds,
                                                       const std::vector<WorkerId>& destinationTopologyNodeIds) {

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();

    NES_INFO("Topology: Finding path between set of start and destination nodes");
    std::vector<TopologyNodePtr> startNodesOfGraph;
    for (const auto& sourceNodeId : sourceTopologyNodeIds) {
        NES_TRACE("Topology: Finding all paths between the source node {} and a set of destination nodes", sourceNodeId);

        auto sourceTopologyNode = (*(*lockedWorkerIdToTopologyNodeMap)[sourceNodeId].rlock());
        std::map<WorkerId, TopologyNodePtr> mapOfUniqueNodes;
        TopologyNodePtr startNodeOfGraph = find(sourceTopologyNode, destinationTopologyNodeIds, mapOfUniqueNodes);
        NES_TRACE("Topology: Validate if all destination nodes reachable");
        for (const auto& destinationNode : destinationTopologyNodeIds) {
            if (mapOfUniqueNodes.find(destinationNode->getId()) == mapOfUniqueNodes.end()) {
                NES_ERROR("Topology: Unable to find path between source node {} and destination node{}",
                          sourceNodeId->toString(),
                          destinationNode->toString());
                return {};
            }
        }
        NES_TRACE("Topology: Push the start node of the graph into a collection of start nodes");
        startNodesOfGraph.push_back(startNodeOfGraph);
    }
    NES_TRACE("Topology: Merge all found sub-graphs together to create a single sub graph and return the set of start nodes of "
              "the merged graph.");
    return mergeSubGraphs(startNodesOfGraph);
}

std::optional<TopologyNodePtr> Topology::findAllPathBetween(const TopologyNodePtr& sourceNode,
                                                            const TopologyNodePtr& destinationNode) {

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();

    NES_DEBUG("Topology: Finding path between {} and {}", sourceNode->toString(), destinationNode->toString());

    std::optional<TopologyNodePtr> result;
    std::vector<TopologyNodePtr> searchedNodes{destinationNode};
    std::map<uint64_t, TopologyNodePtr> mapOfUniqueNodes;
    TopologyNodePtr found = find(sourceNode, searchedNodes, mapOfUniqueNodes);
    if (found) {
        NES_DEBUG("Topology: Found path between {} and {}", sourceNode->toString(), destinationNode->toString());
        return found;
    }
    NES_WARNING("Topology: Unable to find path between {} and {}", sourceNode->toString(), destinationNode->toString());
    return result;
}

std::vector<TopologyNodePtr> Topology::mergeSubGraphs(const std::vector<TopologyNodePtr>& startNodes) {
    NES_INFO("Topology: Merge {} sub-graphs to create a single sub-graph", startNodes.size());

    NES_DEBUG("Topology: Compute a map storing number of times a node occurred in different sub-graphs");
    std::map<WorkerId, uint32_t> nodeCountMap;
    for (const auto& startNode : startNodes) {
        NES_TRACE("Topology: Fetch all ancestor nodes of the given start node");
        const std::vector<NodePtr> family = startNode->getAndFlattenAllAncestors();
        NES_TRACE(
            "Topology: Iterate over the family members and add the information in the node count map about the node occurrence");
        for (const auto& member : family) {
            WorkerId workerId = member->as<TopologyNode>()->getId();
            if (nodeCountMap.find(workerId) != nodeCountMap.end()) {
                NES_TRACE("Topology: Family member already present increment the occurrence count");
                uint32_t count = nodeCountMap[workerId];
                nodeCountMap[workerId] = count + 1;
            } else {
                NES_TRACE("Topology: Add family member into the node count map");
                nodeCountMap[workerId] = 1;
            }
        }
    }

    NES_DEBUG("Topology: Iterate over each sub-graph and compute a single merged sub-graph");
    std::vector<TopologyNodePtr> result;
    std::map<WorkerId, TopologyNodePtr> mergedGraphNodeMap;
    for (const auto& startNode : startNodes) {
        NES_DEBUG(
            "Topology: Check if the node already present in the new merged graph and add a copy of the node if not present");
        if (mergedGraphNodeMap.find(startNode->getId()) == mergedGraphNodeMap.end()) {
            TopologyNodePtr copyOfStartNode = startNode->copy();
            NES_DEBUG("Topology: Add the start node to the list of start nodes for the new merged graph");
            result.push_back(copyOfStartNode);
            mergedGraphNodeMap[startNode->getId()] = copyOfStartNode;
        }
        NES_DEBUG("Topology: Iterate over the ancestry of the start node and add the eligible nodes to new merged graph");
        TopologyNodePtr childNode = startNode;
        while (childNode) {
            NES_TRACE("Topology: Get all parents of the child node to select the next parent to traverse.");
            std::vector<NodePtr> parents = childNode->getParents();
            TopologyNodePtr selectedParent;
            if (parents.size() > 1) {
                NES_TRACE("Topology: Found more than one parent for the node");
                NES_TRACE("Topology: Iterate over all parents and select the parent node that has the max cost value.");
                double maxCost = 0;
                for (auto& parent : parents) {

                    NES_TRACE("Topology: Get all ancestor of the node and aggregate their occurrence counts.");
                    std::vector<NodePtr> family = parent->getAndFlattenAllAncestors();
                    double occurrenceCount = 0;
                    for (auto& member : family) {
                        occurrenceCount = occurrenceCount + nodeCountMap[member->as<TopologyNode>()->getId()];
                    }

                    NES_TRACE("Topology: Compute cost by multiplying aggregate occurrence count with base multiplier and "
                              "dividing the result by the number of nodes in the path.");
                    double cost = (occurrenceCount * BASE_MULTIPLIER) / family.size();

                    if (cost > maxCost) {
                        NES_TRACE("Topology: The cost is more than max cost found till now.");
                        if (selectedParent) {
                            NES_TRACE("Topology: Remove the previously selected parent as parent to the current child node.");
                            childNode->removeParent(selectedParent);
                        }
                        maxCost = cost;
                        NES_TRACE("Topology: Mark this parent as next selected parent.");
                        selectedParent = parent->as<TopologyNode>();
                    } else {
                        NES_TRACE("Topology: The cost is less than max cost found till now.");
                        if (selectedParent) {
                            NES_TRACE("Topology: Remove this parent as parent to the current child node.");
                            childNode->removeParent(parent);
                        }
                    }
                }
            } else if (parents.size() == 1) {
                NES_TRACE("Topology: Found only one parent for the current child node");
                NES_TRACE("Topology: Set the parent as next parent to traverse");
                selectedParent = parents[0]->as<TopologyNode>();
            }

            if (selectedParent) {
                NES_TRACE("Topology: Found a new next parent to traverse");
                if (mergedGraphNodeMap.find(selectedParent->getId()) != mergedGraphNodeMap.end()) {
                    NES_TRACE("Topology: New next parent is already present in the new merged graph.");
                    TopologyNodePtr equivalentParentNode = mergedGraphNodeMap[selectedParent->getId()];
                    TopologyNodePtr equivalentChildNode = mergedGraphNodeMap[childNode->getId()];
                    NES_TRACE("Topology: Add the existing node, with id same as new next parent, as parent to the existing node "
                              "with id same as current child node");
                    equivalentChildNode->addParent(equivalentParentNode);
                } else {
                    NES_TRACE("Topology: New next parent is not present in the new merged graph.");
                    NES_TRACE(
                        "Topology: Add copy of new next parent as parent to the existing child node in the new merged graph.");
                    TopologyNodePtr copyOfSelectedParent = selectedParent->copy();
                    TopologyNodePtr equivalentChildNode = mergedGraphNodeMap[childNode->getId()];
                    equivalentChildNode->addParent(copyOfSelectedParent);
                    mergedGraphNodeMap[selectedParent->getId()] = copyOfSelectedParent;
                }
            }
            NES_TRACE("Topology: Assign new selected parent as next child node to traverse.");
            childNode = selectedParent;
        }
    }

    return result;
}

TopologyNodePtr Topology::find(TopologyNodePtr testNode,
                               std::vector<TopologyNodePtr> searchedNodes,
                               std::map<WorkerId, TopologyNodePtr>& uniqueNodes) {

    NES_TRACE("Topology: check if test node is one of the searched node");
    auto found = std::find_if(searchedNodes.begin(), searchedNodes.end(), [&](const TopologyNodePtr& searchedNode) {
        return searchedNode->getId() == testNode->getId();
    });

    if (found != searchedNodes.end()) {
        NES_DEBUG("Topology: found the destination node");
        if (uniqueNodes.find(testNode->getId()) == uniqueNodes.end()) {
            NES_TRACE("Topology: Insert the information about the test node in the unique node map");
            const TopologyNodePtr copyOfTestNode = testNode->copy();
            uniqueNodes[testNode->getId()] = copyOfTestNode;
        }
        NES_TRACE("Topology: Insert the information about the test node in the unique node map");
        return uniqueNodes[testNode->getId()];
    }

    std::vector<NodePtr> parents = testNode->getParents();
    std::vector<NodePtr> updatedParents;
    //filters out all parents that are marked for maintenance, as these should be ignored during path finding
    for (auto& parent : parents) {
        if (!parent->as<TopologyNode>()->isUnderMaintenance()) {
            updatedParents.push_back(parent);
        }
    }

    if (updatedParents.empty()) {
        NES_WARNING("Topology: reached end of the tree but destination node not found.");
        return nullptr;
    }

    TopologyNodePtr foundNode = nullptr;
    for (auto& parent : updatedParents) {
        TopologyNodePtr foundInParent = find(parent->as<TopologyNode>(), searchedNodes, uniqueNodes);
        if (foundInParent) {
            NES_TRACE("Topology: found the destination node as the parent of the physical node.");
            if (!foundNode) {
                if (uniqueNodes.find(testNode->getId()) == uniqueNodes.end()) {
                    const TopologyNodePtr copyOfTestNode = testNode->copy();
                    uniqueNodes[testNode->getId()] = copyOfTestNode;
                }
                foundNode = uniqueNodes[testNode->getId()];
            }
            NES_TRACE("Topology: Adding found node as parent to the copy of testNode.");
            foundNode->addParent(foundInParent);
        }
    }
    return foundNode;
}

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

std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>>
Topology::getTopologyNodeIdsInRange(Spatial::DataTypes::Experimental::GeoLocation center, double radius) {
    return locationIndex->getNodeIdsInRange(center, radius);
}

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