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

#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <algorithm>
#include <deque>
#include <utility>
#include "Common/GeographicalLocation.hpp"
#ifdef S2DEF
#include <s2/s2latlng.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#endif

namespace NES {

Topology::Topology() : rootNode(nullptr) {}

TopologyPtr Topology::create() { return std::shared_ptr<Topology>(new Topology()); }

bool Topology::addNewTopologyNodeAsChild(const TopologyNodePtr& parent, const TopologyNodePtr& newNode) {
    std::unique_lock lock(topologyLock);
    uint64_t newNodeId = newNode->getId();
    if (indexOnNodeIds.find(newNodeId) == indexOnNodeIds.end()) {
        NES_INFO("Topology: Adding New Node " << newNode->toString() << " to the catalog of nodes.");
        indexOnNodeIds[newNodeId] = newNode;
    }
    NES_INFO("Topology: Adding Node " << newNode->toString() << " as child to the node " << parent->toString());
    return parent->addChild(newNode);
}

bool Topology::removePhysicalNode(const TopologyNodePtr& nodeToRemove) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Removing Node " << nodeToRemove->toString());

    uint64_t idOfNodeToRemove = nodeToRemove->getId();
    if (indexOnNodeIds.find(idOfNodeToRemove) == indexOnNodeIds.end()) {
        NES_WARNING("Topology: The physical node " << nodeToRemove << " doesn't exists in the system.");
        return false;
    }

    if (!rootNode) {
        NES_WARNING("Topology: No root node exists in the topology");
        return false;
    }

    if (rootNode->getId() == idOfNodeToRemove) {
        NES_WARNING("Topology: Attempt to remove the root node. Removing root node is not allowed.");
        return false;
    }

    bool success = rootNode->remove(nodeToRemove);
    if (success) {
        if (nodeToRemove->hasLocation()) {
            removeNodeFromSpatialIndex(nodeToRemove);
        }
        indexOnNodeIds.erase(idOfNodeToRemove);
        NES_DEBUG("Topology: Successfully removed the node.");
        return true;
    }
    NES_WARNING("Topology: Unable to remove the node.");
    return false;
}

bool Topology::setPhysicalNodePosition(const TopologyNodePtr& node, GeographicalLocation geoLoc, bool init) {
    double newLat = geoLoc.getLatitude();
    double newLng = geoLoc.getLongitude();
#ifdef S2DEF
    S2Point newLoc(S2LatLng::FromDegrees(newLat, newLng));
    NES_DEBUG("updating location of Node to: " << newLat << ", " << newLng);

    //if this function was not called during the creation of the node (init != false), we need to delete the old entry
    if (!init) {
        auto oldCoordOpt = node->getCoordinates();
        //a non field node cannot be given a position after its creation
        if (!oldCoordOpt.has_value() && !init) {
            NES_WARNING("Trying to set the Position of a non field node");
            return false;
        }

        auto oldCoord = oldCoordOpt.value();
        S2Point oldLoc(S2LatLng::FromDegrees(oldCoord.getLatitude(), oldCoord.getLongitude()));
        nodePointIndex.Remove(oldLoc, node);
    }

    nodePointIndex.Add(newLoc, node);
#else
    NES_WARNING("Files were compiled without s2. Nothing inserted into spatial index");
    NES_INFO("init = " << init);
#endif
    node->setCoordinates(geoLoc);
    return true;
}

bool Topology::removeNodeFromSpatialIndex(const TopologyNodePtr& node) {
#ifdef S2DEF
    auto geoLocOpt = node->getCoordinates();
    if(!geoLocOpt.has_value()) {
        NES_WARNING("trying to remove node from spatial index but the node does not have a location set");
        return false;
    }
    auto geoLoc = geoLocOpt.value();
    S2Point point(S2LatLng::FromDegrees(geoLoc.getLatitude(), geoLoc.getLongitude()));
    nodePointIndex.Remove(point, node);
    return true;
#else
    NES_WARNING("Files were compiled without s2. Nothing can be removed from the spatial index because it does not exist");
    NES_INFO("node id: " << node->getId());
    return {};
#endif
}

std::optional<TopologyNodePtr> Topology::getClosestNodeTo(const GeographicalLocation& geoLoc, int radius) {
#ifdef S2DEF
    S2ClosestPointQuery<TopologyNodePtr> query(&nodePointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));
    S2ClosestPointQuery<TopologyNodePtr>::PointTarget target(S2Point(S2LatLng::FromDegrees(geoLoc.getLatitude(), geoLoc.getLongitude())));
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

std::optional<TopologyNodePtr> Topology::getClosestNodeTo(const TopologyNodePtr& nodePtr, int radius) {
#ifdef S2DEF
    auto GeoLocOpt = nodePtr->getCoordinates();

    if (!GeoLocOpt.has_value()) {
        NES_WARNING("Trying to get the closest node to a node that does not have a location");
        return {};
    }

    auto geoLoc = GeoLocOpt.value();

    S2ClosestPointQuery<TopologyNodePtr> query(&nodePointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));
    S2ClosestPointQuery<TopologyNodePtr>::PointTarget target(S2Point(S2LatLng::FromDegrees(geoLoc.getLatitude(), geoLoc.getLongitude())));
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

std::vector<std::pair<TopologyNodePtr, GeographicalLocation>> Topology::getNodesInRange(GeographicalLocation center, double radius) {
#ifdef S2DEF
    S2ClosestPointQuery<TopologyNodePtr> query(&nodePointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));

    S2ClosestPointQuery<TopologyNodePtr>::PointTarget target(S2Point(S2LatLng::FromDegrees(center.getLatitude(), center.getLongitude())));
    auto result = query.FindClosestPoints(&target);
    std::vector<std::pair<TopologyNodePtr, GeographicalLocation>> closestNodeList;
    for ( auto r : result) {
        auto latLng = S2LatLng(r.point());
        closestNodeList.emplace_back(r.data(), GeographicalLocation(latLng.lat().degrees(), latLng.lng().degrees()));
    }
    return closestNodeList;

#else
    NES_WARNING("Files were compiled without s2, cannot find closest nodes");
    NES_INFO("supplied values: " << center.getLatitude() << ", " << center.getLongitude() << "radius: " << radius);
    return {};
#endif
}

size_t Topology::getSizeOfPointIndex() {
#ifdef S2DEF
    return nodePointIndex.num_points();
#else
    NES_WARNING("s2 lib not included");
    return {};
#endif
}

std::vector<TopologyNodePtr> Topology::findPathBetween(const std::vector<TopologyNodePtr>& sourceNodes,
                                                       const std::vector<TopologyNodePtr>& destinationNodes) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding path between set of start and destination nodes");
    std::vector<TopologyNodePtr> startNodesOfGraph;
    for (const auto& sourceNode : sourceNodes) {
        NES_TRACE("Topology: Finding all paths between the source node " << sourceNode << " and a set of destination nodes");
        std::map<uint64_t, TopologyNodePtr> mapOfUniqueNodes;
        TopologyNodePtr startNodeOfGraph = find(sourceNode, destinationNodes, mapOfUniqueNodes);
        NES_TRACE("Topology: Validate if all destination nodes reachable");
        for (const auto& destinationNode : destinationNodes) {
            if (mapOfUniqueNodes.find(destinationNode->getId()) == mapOfUniqueNodes.end()) {
                NES_ERROR("Topology: Unable to find path between source node "
                          << sourceNode->toString() << " and destination node " << destinationNode->toString());
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

std::vector<TopologyNodePtr> Topology::mergeSubGraphs(const std::vector<TopologyNodePtr>& startNodes) {
    NES_INFO("Topology: Merge " << startNodes.size() << " sub-graphs to create a single sub-graph");

    NES_DEBUG("Topology: Compute a map storing number of times a node occurred in different sub-graphs");
    std::map<uint64_t, uint32_t> nodeCountMap;
    for (const auto& startNode : startNodes) {
        NES_TRACE("Topology: Fetch all ancestor nodes of the given start node");
        const std::vector<NodePtr> family = startNode->getAndFlattenAllAncestors();
        NES_TRACE(
            "Topology: Iterate over the family members and add the information in the node count map about the node occurrence");
        for (const auto& member : family) {
            uint64_t nodeId = member->as<TopologyNode>()->getId();
            if (nodeCountMap.find(nodeId) != nodeCountMap.end()) {
                NES_TRACE("Topology: Family member already present increment the occurrence count");
                uint32_t count = nodeCountMap[nodeId];
                nodeCountMap[nodeId] = count + 1;
            } else {
                NES_TRACE("Topology: Add family member into the node count map");
                nodeCountMap[nodeId] = 1;
            }
        }
    }

    NES_DEBUG("Topology: Iterate over each sub-graph and compute a single merged sub-graph");
    std::vector<TopologyNodePtr> result;
    std::map<uint64_t, TopologyNodePtr> mergedGraphNodeMap;
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

std::optional<TopologyNodePtr> Topology::findAllPathBetween(const TopologyNodePtr& startNode,
                                                            const TopologyNodePtr& destinationNode) {
    std::unique_lock lock(topologyLock);
    NES_DEBUG("Topology: Finding path between " << startNode->toString() << " and " << destinationNode->toString());

    std::optional<TopologyNodePtr> result;
    std::vector<TopologyNodePtr> searchedNodes{destinationNode};
    std::map<uint64_t, TopologyNodePtr> mapOfUniqueNodes;
    TopologyNodePtr found = find(startNode, searchedNodes, mapOfUniqueNodes);
    if (found) {
        NES_DEBUG("Topology: Found path between " << startNode->toString() << " and " << destinationNode->toString());
        return found;
    }
    NES_WARNING("Topology: Unable to find path between " << startNode->toString() << " and " << destinationNode->toString());
    return result;
}

TopologyNodePtr Topology::find(TopologyNodePtr testNode,
                               std::vector<TopologyNodePtr> searchedNodes,
                               std::map<uint64_t, TopologyNodePtr>& uniqueNodes) {

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
        if (!parent->as<TopologyNode>()->getMaintenanceFlag()) {
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
                //TODO: SZ I don't understand how we can end up here
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

std::string Topology::toString() {
    std::unique_lock lock(topologyLock);

    if (!rootNode) {
        NES_WARNING("Topology: No root node found");
        return "";
    }

    std::stringstream topologyInfo;
    topologyInfo << std::endl;

    // store pair of TopologyNodePtr and its depth in when printed
    std::deque<std::pair<TopologyNodePtr, uint64_t>> parentToPrint{std::make_pair(rootNode, 0)};

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

void Topology::print() { NES_DEBUG("Topology:" << toString()); }

bool Topology::nodeExistsWithIpAndPort(const std::string& ipAddress, uint32_t grpcPort) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding if a physical node with ip " << ipAddress << " and port " << grpcPort << " exists.");
    if (!rootNode) {
        NES_WARNING("Topology: Root node not found.");
        return false;
    }
    NES_TRACE("Topology: Traversing the topology using BFS.");
    BreadthFirstNodeIterator bfsIterator(rootNode);
    for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
        auto physicalNode = (*itr)->as<TopologyNode>();
        if (physicalNode->getIpAddress() == ipAddress && physicalNode->getGrpcPort() == grpcPort) {
            NES_TRACE("Topology: Found a physical node " << physicalNode->toString() << " with ip " << ipAddress << " and port "
                                                         << grpcPort);
            return true;
        }
    }
    return false;
}

TopologyNodePtr Topology::getRoot() {
    std::unique_lock lock(topologyLock);
    return rootNode;
}

TopologyNodePtr Topology::findNodeWithId(uint64_t nodeId) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding a physical node with id " << nodeId);
    if (indexOnNodeIds.find(nodeId) != indexOnNodeIds.end()) {
        NES_DEBUG("Topology: Found a physical node with id " << nodeId);
        return indexOnNodeIds[nodeId];
    }
    NES_WARNING("Topology: Unable to find a physical node with id " << nodeId);
    return nullptr;
}

void Topology::setAsRoot(const TopologyNodePtr& physicalNode) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Setting physical node " << physicalNode->toString() << " as root to the topology.");
    indexOnNodeIds[physicalNode->getId()] = physicalNode;
    rootNode = physicalNode;
}

bool Topology::removeNodeAsChild(const TopologyNodePtr& parentNode, const TopologyNodePtr& childNode) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Removing node " << childNode->toString() << " as child to the node " << parentNode->toString());
    return parentNode->remove(childNode);
}

bool Topology::reduceResources(uint64_t nodeId, uint16_t amountToReduce) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Reduce " << amountToReduce << " resources from node with id " << nodeId);
    if (indexOnNodeIds.find(nodeId) == indexOnNodeIds.end()) {
        NES_WARNING("Topology: Unable to find node with id " << nodeId);
        return false;
    }
    indexOnNodeIds[nodeId]->reduceResources(amountToReduce);
    return true;
}

bool Topology::increaseResources(uint64_t nodeId, uint16_t amountToIncrease) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Increase " << amountToIncrease << " resources from node with id " << nodeId);
    if (indexOnNodeIds.find(nodeId) == indexOnNodeIds.end()) {
        NES_WARNING("Topology: Unable to find node with id " << nodeId);
        return false;
    }
    indexOnNodeIds[nodeId]->increaseResources(amountToIncrease);
    return true;
}

TopologyNodePtr Topology::findCommonAncestor(std::vector<TopologyNodePtr> topologyNodes) {

    NES_DEBUG("Topology: find common node for a set of topology nodes.");

    if (topologyNodes.empty()) {
        NES_ERROR("Topology: Input topology node list was empty.");
        return nullptr;
    }

    //Check if one of the input node is a root node of the topology
    auto found = std::find_if(topologyNodes.begin(), topologyNodes.end(), [&](const TopologyNodePtr& topologyNode) {
        return rootNode->getId() == topologyNode->getId();
    });

    // If a root node found in the input nodes then return the root topology node
    if (found != topologyNodes.end()) {
        return *found;
    }

    NES_DEBUG("Topology: Selecting a start node to identify the common ancestor.");
    TopologyNodePtr startNode = topologyNodes[0];
    bool foundAncestor = false;
    TopologyNodePtr resultAncestor;
    NES_TRACE("Topology: Adding selected node to the deque for further processing.");
    std::deque<NodePtr> nodesToProcess{startNode};
    while (!nodesToProcess.empty()) {
        TopologyNodePtr candidateNode = nodesToProcess.front()->as<TopologyNode>();
        nodesToProcess.pop_front();
        NES_TRACE(
            "Topology: Check if the children topology node of the node under consideration contains all input topology nodes.");
        std::vector<NodePtr> children = candidateNode->getAndFlattenAllChildren(false);
        for (auto& nodeToLook : topologyNodes) {
            auto found = std::find_if(children.begin(), children.end(), [&](const NodePtr& child) {
                return nodeToLook->getId() == child->as<TopologyNode>()->getId();
            });

            if (found == children.end()) {
                NES_TRACE("Topology: Unable to find the input topology node as child of the node under consideration.");
                foundAncestor = false;
                break;
            }
            foundAncestor = true;
        }

        if (foundAncestor) {
            NES_TRACE("Topology: The node under consideration contains all input node as its children.");
            return candidateNode;
        }

        NES_TRACE("Topology: Add parent of the the node under consideration to the deque for further processing.");
        for (const auto& parent : candidateNode->getParents()) {
            if (!parent->as<TopologyNode>()->getMaintenanceFlag())
                nodesToProcess.push_back(parent);
        }
    }

    NES_ERROR("Topology: Unable to find a common ancestor topology node for the input topology nodes.");
    return nullptr;
}

TopologyNodePtr Topology::findCommonChild(std::vector<TopologyNodePtr> topologyNodes) {
    NES_INFO("Topology: find common child node for a set of parent topology nodes.");

    if (topologyNodes.empty()) {
        NES_WARNING("Topology: Input topology node list was empty.");
        return nullptr;
    }

    NES_DEBUG("Topology: Selecting a start node to identify the common child.");
    TopologyNodePtr startNode = topologyNodes[0];
    bool foundAncestor = false;
    TopologyNodePtr resultAncestor;
    NES_TRACE("Topology: Adding selected node to the deque for further processing.");
    std::deque<NodePtr> nodesToProcess{startNode};
    while (!nodesToProcess.empty()) {
        TopologyNodePtr candidateNode = nodesToProcess.front()->as<TopologyNode>();
        nodesToProcess.pop_front();
        NES_TRACE(
            "Topology: Check if the parent topology node of the node under consideration contains all input topology nodes.");
        std::vector<NodePtr> parents = candidateNode->getAndFlattenAllAncestors();
        for (auto& nodeToLook : topologyNodes) {
            auto found = std::find_if(parents.begin(), parents.end(), [&](const NodePtr& parent) {
                return nodeToLook->getId() == parent->as<TopologyNode>()->getId();
            });

            if (found == parents.end()) {
                NES_TRACE("Topology: Unable to find the input topology node as parent of the node under consideration.");
                foundAncestor = false;
                break;
            }
            foundAncestor = true;
        }

        if (foundAncestor) {
            NES_TRACE("Topology: The node under consideration contains all input node as its parent.");
            return candidateNode;
        }

        NES_TRACE("Topology: Add children of the the node under consideration to the deque for further processing.");
        for (const auto& child : candidateNode->getChildren()) {
            if (!child->as<TopologyNode>()->getMaintenanceFlag()) {
                nodesToProcess.push_back(child);
            }
        }
    }
    NES_WARNING("Topology: Unable to find a common child topology node for the input topology nodes.");
    return nullptr;
}

TopologyNodePtr Topology::findCommonNodeBetween(std::vector<TopologyNodePtr> childNodes,
                                                std::vector<TopologyNodePtr> parenNodes) {
    NES_DEBUG("Topology: Find a common ancestor node for the input children nodes.");
    TopologyNodePtr commonAncestorForChildren = findCommonAncestor(std::move(childNodes));
    if (!commonAncestorForChildren) {
        NES_WARNING("Topology: Unable to find a common ancestor node for the input child node.");
        return nullptr;
    }

    NES_DEBUG("Topology: Find a common child node for the input parent nodes.");
    TopologyNodePtr commonChildForParents = findCommonChild(std::move(parenNodes));
    if (!commonChildForParents) {
        NES_WARNING("Topology: Unable to find a common child node for the input parent nodes.");
        return nullptr;
    }

    if (commonChildForParents->getId() == commonAncestorForChildren->getId()) {
        NES_DEBUG("Topology: Both common child and ancestor are same node. Returning as result.");
        return commonChildForParents;
    }
    if (commonChildForParents->containAsChild(commonAncestorForChildren)) {
        NES_DEBUG("Topology: Returning the common children of the parent topology nodes");
        return commonChildForParents;
    } else if (!commonChildForParents->containAsParent(commonAncestorForChildren)) {
        NES_WARNING("Topology: Common child is not connected to the common ancestor.");
        return nullptr;
    }
    NES_DEBUG("Topology: Returning common ancestor as result.");
    return commonAncestorForChildren;
}

std::vector<TopologyNodePtr> Topology::findNodesBetween(const TopologyNodePtr& sourceNode,
                                                        const TopologyNodePtr& destinationNode) {

    NES_DEBUG("Topology: Find topology nodes between source and destination nodes.");
    if (sourceNode->getId() == destinationNode->getId()) {
        NES_DEBUG("Topology: Both source and destination are same node.");
        return {sourceNode};
    }
    if (!sourceNode->containAsParent(destinationNode)) {
        NES_WARNING("Topology: source node is not connected to the destination node.");
        return {};
    }

    std::vector<TopologyNodePtr> nodesBetween;
    NES_DEBUG("Topology: iterate over parent of the source node and find path between its parent and destination nodes.");
    for (const auto& sourceParent : sourceNode->getParents()) {
        std::vector<TopologyNodePtr> foundBetweenNodes = findNodesBetween(sourceParent->as<TopologyNode>(), destinationNode);
        if (!foundBetweenNodes.empty()) {
            NES_TRACE("Topology: found a path between source nodes parent and destination nodes.");
            nodesBetween.push_back(sourceNode);
            nodesBetween.insert(nodesBetween.end(), foundBetweenNodes.begin(), foundBetweenNodes.end());
            return nodesBetween;
        }
    }
    NES_DEBUG("Topology: return the found path between source and destination nodes.");
    return nodesBetween;
}

std::vector<TopologyNodePtr> Topology::findNodesBetween(std::vector<TopologyNodePtr> sourceNodes,
                                                        std::vector<TopologyNodePtr> destinationNodes) {
    NES_DEBUG("Topology: Find a common ancestor node for the input children nodes.");
    TopologyNodePtr commonAncestorForChildren = findCommonAncestor(std::move(sourceNodes));
    if (!commonAncestorForChildren) {
        NES_WARNING("Topology: Unable to find a common ancestor node for the input child node.");
        return {};
    }

    NES_DEBUG("Topology: Find a common child node for the input parent nodes.");
    TopologyNodePtr commonChildForParents = findCommonChild(std::move(destinationNodes));
    if (!commonChildForParents) {
        NES_WARNING("Topology: Unable to find a common child node for the input parent nodes.");
        return {};
    }

    return findNodesBetween(commonAncestorForChildren, commonChildForParents);
}

TopologyNodePtr Topology::findTopologyNodeInSubgraphById(uint64_t id, const std::vector<TopologyNodePtr>& sourceNodes) {
    //First look in all source nodes
    for (const auto& sourceNode : sourceNodes) {
        if (sourceNode->getId() == id) {
            return sourceNode;
        }
    }

    //Perform DFS from the root node to find TopologyNode with matching identifier
    auto rootNodes = sourceNodes[0]->getAllRootNodes();
    //TODO: When we support more than 1 sink location please change the logic
    if (rootNodes.size() > 1) {
        NES_NOT_IMPLEMENTED();
    }
    auto topologyIterator = NES::DepthFirstNodeIterator(rootNodes[0]).begin();
    while (topologyIterator != NES::DepthFirstNodeIterator::end()) {
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
        if (currentTopologyNode->getId() == id) {
            return currentTopologyNode;
        }
        ++topologyIterator;
    }
    return nullptr;
}

}// namespace NES
