#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <deque>

namespace NES {

Topology::Topology() : rootNode(nullptr), topologyLock() {
    NES_DEBUG("Topology()");
}

Topology::~Topology() {
    NES_DEBUG("~Topology()");
}

TopologyPtr Topology::create() {
    return std::shared_ptr<Topology>(new Topology());
}

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

std::vector<TopologyNodePtr> Topology::findPathBetween(std::vector<TopologyNodePtr> startNodes, std::vector<TopologyNodePtr> destinationNodes) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding path between set of start and destination nodes");
    std::vector<TopologyNodePtr> startNodesOfGraph;
    for (auto& startNode : startNodes) {
        std::map<uint64_t, TopologyNodePtr> mapOfUniqueNodes;
        TopologyNodePtr startNodeOfGraph = find(startNode, destinationNodes, mapOfUniqueNodes);
        for (auto& destinationNode : destinationNodes) {
            if (mapOfUniqueNodes.find(destinationNode->getId()) == mapOfUniqueNodes.end()) {
                NES_ERROR("Topology: Unable to find path between source node " << startNode->toString() << " and destination node " << destinationNode->toString());
                return {};
            }
        }
        startNodesOfGraph.push_back(startNodeOfGraph);
    }
    return startNodesOfGraph;
}

std::vector<TopologyNodePtr> mergeGraphs(std::vector<TopologyNodePtr> startNodes) {
    std::map<uint64_t, uint32_t> nodeCountMap;

    std::deque<TopologyNodePtr> nodesToTraverse{startNodes.begin(), startNodes.end()};
    while (!nodesToTraverse.empty()) {
        TopologyNodePtr node = nodesToTraverse.front();
        nodesToTraverse.pop_front();
        const std::vector<NodePtr> family = node->getAndFlattenAllParent();
        for (auto& member : family) {
            uint64_t nodeId = member->as<TopologyNode>()->getId();
            if (nodeCountMap.find(nodeId) != nodeCountMap.end()) {
                uint32_t count = nodeCountMap[nodeId];
                nodeCountMap[nodeId] = count + 1;
            } else {
                nodeCountMap[nodeId] = 1;
            }
        }
    }

    for (auto& startNode : startNodes) {
        while (startNode) {
            std::vector<NodePtr> parents = startNode->getParents();
            if (parents.size() > 1) {
                uint32_t maxWeight = 0;
                TopologyNodePtr selectedParent;
                for (auto& parent : parents) {
                    std::vector<NodePtr> family = parent->getAndFlattenAllParent();
                    uint32_t weight = 0;
                    for (auto& member : family) {
                        weight = weight + nodeCountMap[member->as<TopologyNode>()->getId()];
                    }
                    if (weight > maxWeight) {
                        if (selectedParent) {
                            startNode->removeChild(selectedParent);
                        }
                        maxWeight = weight;
                        selectedParent = parent->as<TopologyNode>();
                    } else {
                        if (selectedParent) {
                            startNode->removeChild(selectedParent);
                        }
                    }
                }
                startNode = selectedParent;
            } else if (parents.size() == 1) {
                startNode = parents[0]->as<TopologyNode>();
            } else {
                startNode = nullptr;
            }
        }
    }

    return startNodes;
}

std::optional<TopologyNodePtr> Topology::findAllPathBetween(TopologyNodePtr startNode, TopologyNodePtr destinationNode) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding path between " << startNode->toString() << " and " << destinationNode->toString());

    std::optional<TopologyNodePtr> result;
    std::vector<TopologyNodePtr> searchedNodes{destinationNode};
    std::map<uint64_t, TopologyNodePtr> mapOfUniqueNodes;
    TopologyNodePtr found = find(startNode, searchedNodes, mapOfUniqueNodes);
    if (found) {
        NES_INFO("Topology: Found path between " << startNode->toString() << " and " << destinationNode->toString());
        return found;
    }
    NES_WARNING("Topology: Unable to find path between " << startNode->toString() << " and " << destinationNode->toString());
    return result;
}

TopologyNodePtr Topology::find(TopologyNodePtr testNode, std::vector<TopologyNodePtr> searchedNodes, std::map<uint64_t, TopologyNodePtr>& uniqueNodes) {

    auto found = std::find_if(searchedNodes.begin(), searchedNodes.end(), [&](TopologyNodePtr searchedNode) {
        return searchedNode->getId() == testNode->getId();
    });

    std::optional<TopologyNodePtr> result;

    if (found != searchedNodes.end()) {
        NES_INFO("Topology: found the destination node");
        if (uniqueNodes.find(testNode->getId()) == uniqueNodes.end()) {
            const TopologyNodePtr copyOfTestNode = testNode->copy();
            uniqueNodes[testNode->getId()] = copyOfTestNode;
        }
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
    //FIXME: as part of #954

    if (!rootNode) {
        NES_WARNING("Topology: No root node found");
        return "";
    }

    std::stringstream topologyInfo;
    topologyInfo << std::endl;
    std::deque<TopologyNodePtr> parentToPrint{rootNode};
    std::deque<TopologyNodePtr> childToPrint;

    while (!parentToPrint.empty()) {
        TopologyNodePtr nodeToPrint = parentToPrint.front();
        parentToPrint.pop_front();
        topologyInfo << nodeToPrint->getId() << "\t";

        for (auto& child : nodeToPrint->getChildren()) {
            childToPrint.push_back(child->as<TopologyNode>());
        }

        if (parentToPrint.empty()) {
            topologyInfo << std::endl;
            parentToPrint.insert(parentToPrint.end(), childToPrint.begin(), childToPrint.end());
            childToPrint.clear();
        }
    }
    return topologyInfo.str();
}

void Topology::print() {
    NES_DEBUG("Topology:" << toString());
}

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
            NES_TRACE("Topology: Found a physical node " << physicalNode->toString() << " with ip " << ipAddress << " and port " << grpcPort);
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

}// namespace NES
