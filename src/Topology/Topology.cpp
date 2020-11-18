/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <deque>

namespace NES {

Topology::Topology() : rootNode(nullptr), topologyLock() { NES_DEBUG("Topology()"); }

Topology::~Topology() { NES_DEBUG("~Topology()"); }

TopologyPtr Topology::create() { return std::shared_ptr<Topology>(new Topology()); }

bool Topology::addNewPhysicalNodeAsChild(TopologyNodePtr parent, TopologyNodePtr newNode) {
    std::unique_lock lock(topologyLock);
    uint64_t newNodeId = newNode->getId();
    if (indexOnNodeIds.find(newNodeId) == indexOnNodeIds.end()) {
        NES_INFO("Topology: Adding New Node " << newNode->toString() << " to the catalog of nodes.");
        indexOnNodeIds[newNodeId] = newNode;
    }
    NES_INFO("Topology: Adding Node " << newNode->toString() << " as child to the node " << parent->toString());
    return parent->addChild(newNode);
}

bool Topology::removePhysicalNode(TopologyNodePtr nodeToRemove) {
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
        indexOnNodeIds.erase(idOfNodeToRemove);
        NES_DEBUG("Topology: Successfully removed the node.");
        return true;
    }
    NES_WARNING("Topology: Unable to remove the node.");
    return false;
}

std::vector<TopologyNodePtr> Topology::findPathBetween(std::vector<TopologyNodePtr> sourceNodes,
                                                       std::vector<TopologyNodePtr> destinationNodes) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding path between set of start and destination nodes");
    std::vector<TopologyNodePtr> startNodesOfGraph;
    for (auto& sourceNode : sourceNodes) {
        NES_TRACE("Topology: Finding all paths between the source node " << sourceNode << " and a set of destination nodes");
        std::map<uint64_t, TopologyNodePtr> mapOfUniqueNodes;
        TopologyNodePtr startNodeOfGraph = find(sourceNode, destinationNodes, mapOfUniqueNodes);
        NES_TRACE("Topology: Validate if all destination nodes reachable");
        for (auto& destinationNode : destinationNodes) {
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

std::vector<TopologyNodePtr> Topology::mergeSubGraphs(std::vector<TopologyNodePtr> startNodes) {
    NES_INFO("Topology: Merge " << startNodes.size() << " sub-graphs to create a single sub-graph");

    NES_DEBUG("Topology: Compute a map storing number of times a node occurred in different sub-graphs");
    std::map<uint64_t, uint32_t> nodeCountMap;
    for (auto& startNode : startNodes) {
        NES_TRACE("Topology: Fetch all ancestor nodes of the given start node");
        const std::vector<NodePtr> family = startNode->getAndFlattenAllAncestors();
        NES_TRACE(
            "Topology: Iterate over the family members and add the information in the node count map about the node occurrence");
        for (auto& member : family) {
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
    for (auto& startNode : startNodes) {
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

std::optional<TopologyNodePtr> Topology::findAllPathBetween(TopologyNodePtr startNode, TopologyNodePtr destinationNode) {
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

TopologyNodePtr Topology::find(TopologyNodePtr testNode, std::vector<TopologyNodePtr> searchedNodes,
                               std::map<uint64_t, TopologyNodePtr>& uniqueNodes) {

    NES_TRACE("Topology: check if test node is one of the searched node");
    auto found = std::find_if(searchedNodes.begin(), searchedNodes.end(), [&](TopologyNodePtr searchedNode) {
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
    if (parents.empty()) {
        NES_WARNING("Topology: reached end of the tree but destination node not found.");
        return nullptr;
    }

    TopologyNodePtr foundNode = nullptr;
    for (auto& parent : parents) {
        TopologyNodePtr found = find(parent->as<TopologyNode>(), searchedNodes, uniqueNodes);
        if (found) {
            NES_TRACE("Topology: found the destination node as the parent of the physical node.");
            if (!foundNode) {
                if (uniqueNodes.find(testNode->getId()) == uniqueNodes.end()) {
                    const TopologyNodePtr copyOfTestNode = testNode->copy();
                    uniqueNodes[testNode->getId()] = copyOfTestNode;
                }
                foundNode = uniqueNodes[testNode->getId()];
            }
            NES_TRACE("Topology: Adding found node as parent to the copy of testNode.");
            foundNode->addParent(found);
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
    std::deque<std::pair<TopologyNodePtr, size_t>> parentToPrint{std::make_pair(rootNode, 0)};
    std::deque<std::pair<TopologyNodePtr, size_t>> childToPrint;

    // indent multiplier
    int indent = 2;

    // perform dfs traverse
    while (!parentToPrint.empty()) {
        std::pair<TopologyNodePtr, size_t> nodeToPrint = parentToPrint.front();
        parentToPrint.pop_front();
        for (int i = 0; i < indent * nodeToPrint.second; i++) {
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

        for (auto& child : nodeToPrint.first->getChildren()) {
            parentToPrint.emplace_front(child->as<TopologyNode>(), nodeToPrint.second + 1);
        }
    }
    return topologyInfo.str();
}

void Topology::print() { NES_DEBUG("Topology:" << toString()); }

bool Topology::nodeExistsWithIpAndPort(std::string ipAddress, uint32_t grpcPort) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding if a physical node with ip " << ipAddress << " and port " << grpcPort << " exists.");
    if (!rootNode) {
        NES_WARNING("Topology: Root node not found.");
        return false;
    }
    NES_TRACE("Topology: Traversing the topology using BFS.");
    BreadthFirstNodeIterator bfsIterator(rootNode);
    for (auto itr = bfsIterator.begin(); itr != bfsIterator.end(); ++itr) {
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

void Topology::setAsRoot(TopologyNodePtr physicalNode) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Setting physical node " << physicalNode->toString() << " as root to the topology.");
    indexOnNodeIds[physicalNode->getId()] = physicalNode;
    rootNode = physicalNode;
}

bool Topology::removeNodeAsChild(TopologyNodePtr parentNode, TopologyNodePtr childNode) {
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

    NES_INFO("Topology: find common node for a set of topology nodes.");

    if (topologyNodes.empty()) {
        NES_WARNING("Topology: Input topology node list was empty.");
        return nullptr;
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
        std::vector<NodePtr> children = candidateNode->getAndFlattenAllChildren();
        for (auto& nodeToLook : topologyNodes) {
            auto found = std::find_if(children.begin(), children.end(), [&](NodePtr child) {
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
        for (auto& parent : candidateNode->getParents()) {
            nodesToProcess.push_back(parent);
        }
    }

    NES_WARNING("Topology: Unable to find a common ancestor topology node for the input topology nodes.");
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
            auto found = std::find_if(parents.begin(), parents.end(), [&](NodePtr parent) {
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
        for (auto& child : candidateNode->getChildren()) {
            nodesToProcess.push_back(child);
        }
    }
    NES_WARNING("Topology: Unable to find a common child topology node for the input topology nodes.");
    return nullptr;
}

TopologyNodePtr Topology::findCommonNodeBetween(std::vector<TopologyNodePtr> childNodes,
                                                std::vector<TopologyNodePtr> parenNodes) {
    NES_DEBUG("Topology: Find a common ancestor node for the input children nodes.");
    TopologyNodePtr commonAncestor = findCommonAncestor(childNodes);
    if (!commonAncestor) {
        NES_WARNING("Topology: Unable to fina a common ancestor node for the input child node.");
        return nullptr;
    }

    NES_DEBUG("Topology: Find a common child node for the input parent nodes.");
    TopologyNodePtr commonChild = findCommonChild(parenNodes);
    if (!commonChild) {
        NES_WARNING("Topology: Unable to find a common child node for the input parent nodes.");
        return nullptr;
    }

    if (commonChild->getId() == commonAncestor->getId()) {
        NES_DEBUG("Topology: Both common child and ancestor are same node. Returning as result.");
        return commonChild;
    } else if (!commonChild->containAsParent(commonAncestor)) {
        NES_WARNING("Topology: Common child is not connected to the common ancestor.");
        return nullptr;
    }
    NES_DEBUG("Topology: Returning common ancestor as result.");
    return commonAncestor;
}

std::vector<TopologyNodePtr> Topology::findTopologyNodesBetween(TopologyNodePtr sourceNode, TopologyNodePtr destinationNode) {

    NES_DEBUG("Topology: Find topology nodes between source and destination nodes.");
    if (sourceNode->getId() == destinationNode->getId()) {
        NES_DEBUG("Topology: Both source and destination are same node.");
        return {sourceNode};
    } else if (!sourceNode->containAsParent(destinationNode)) {
        NES_WARNING("Topology: source node is not connected to the destination node.");
        return {};
    }

    std::vector<TopologyNodePtr> nodesBetween;
    NES_DEBUG("Topology: iterate over parent of the source node and find path between its parent and destination nodes.");
    for (auto& sourceParent : sourceNode->getParents()) {
        std::vector<TopologyNodePtr> foundBetweenNodes =
            findTopologyNodesBetween(sourceParent->as<TopologyNode>(), destinationNode);
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

}// namespace NES
