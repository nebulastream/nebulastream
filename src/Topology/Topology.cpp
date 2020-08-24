#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <deque>

namespace NES {

Topology::Topology() : rootNode(nullptr), mutex() {}

TopologyPtr Topology::create() {
    return std::shared_ptr<Topology>(new Topology());
}

bool Topology::addNewPhysicalNodeAsChild(TopologyNodePtr parent, TopologyNodePtr newNode) {
    std::unique_lock lock(mutex);
    uint64_t newNodeId = newNode->getId();
    if (indexOnNodeIds.find(newNodeId) == indexOnNodeIds.end()) {
        NES_INFO("Topology: Adding New Node " << newNode->toString() << " to the catalog of nodes.");
        indexOnNodeIds[newNodeId] = newNode;
    }
    NES_INFO("Topology: Adding Node " << newNode->toString() << " as child to the node " << parent->toString());
    return parent->addChild(newNode);
}

bool Topology::removePhysicalNode(TopologyNodePtr nodeToRemove) {
    std::unique_lock lock(mutex);
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

std::optional<TopologyNodePtr> Topology::findPathBetween(TopologyNodePtr startNode, TopologyNodePtr destinationNode) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Finding path between " << startNode->toString() << " and " << destinationNode->toString());

    std::vector<TopologyNodePtr> nodesInPath;
    bool found = find(startNode, destinationNode, nodesInPath);
    if (found) {
        NES_INFO("Topology: Found path between " << startNode->toString() << " and " << destinationNode->toString());
        TopologyNodePtr previousNode;
        for (auto itr = nodesInPath.begin(); itr != nodesInPath.end(); itr++) {
            if (previousNode) {
                previousNode->addChild(*itr);
            }
            previousNode = *itr;
        }
        return previousNode;
    }
    NES_WARNING("Topology: Unable to find path between " << startNode->toString() << " and " << destinationNode->toString());
    return {};
}

bool Topology::find(TopologyNodePtr physicalNode, TopologyNodePtr destinationNode, std::vector<TopologyNodePtr>& nodesInPath) {

    if (physicalNode->getId() == destinationNode->getId()) {
        NES_INFO("Topology: found the destination node with " << physicalNode->toString() << " == " << destinationNode->toString());
        nodesInPath.push_back(physicalNode->copy());
        return true;
    }

    std::vector<NodePtr> parents = physicalNode->getParents();
    if (parents.empty()) {
        NES_WARNING("Topology: reached end of the tree but destination node was not found.");
        return false;
    }

    for (auto& parent : parents) {
        if (find(parent->as<TopologyNode>(), destinationNode, nodesInPath)) {
            NES_TRACE("Topology: found the destination node as the parent of the physical node.");
            nodesInPath.push_back(physicalNode->copy());
            return true;
        }
    }
    return false;
}

std::string Topology::toString() {
    std::unique_lock lock(mutex);
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
    std::unique_lock lock(mutex);
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
    std::unique_lock lock(mutex);
    return rootNode;
}

TopologyNodePtr Topology::findNodeWithId(uint64_t nodeId) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Finding a physical node with id " << nodeId);
    if (indexOnNodeIds.find(nodeId) != indexOnNodeIds.end()) {
        NES_DEBUG("Topology: Found a physical node with id " << nodeId);
        return indexOnNodeIds[nodeId];
    }
    NES_WARNING("Topology: Unable to find a physical node with id " << nodeId);
    return nullptr;
}

void Topology::setAsRoot(TopologyNodePtr physicalNode) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Setting physical node " << physicalNode->toString() << " as root to the topology.");
    indexOnNodeIds[physicalNode->getId()] = physicalNode;
    rootNode = physicalNode;
}

bool Topology::removeNodeAsChild(TopologyNodePtr parentNode, TopologyNodePtr childNode) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Removing node " << childNode->toString() << " as child to the node " << parentNode->toString());
    return parentNode->remove(childNode);
}

bool Topology::reduceResources(uint64_t nodeId, uint16_t amountToReduce) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Reduce " << amountToReduce << " resources from node with id " << nodeId);
    if (indexOnNodeIds.find(nodeId) == indexOnNodeIds.end()) {
        NES_WARNING("Topology: Unable to find node with id " << nodeId);
        return false;
    }
    indexOnNodeIds[nodeId]->reduceResources(amountToReduce);
    return true;
}

bool Topology::increaseResources(uint64_t nodeId, uint16_t amountToIncrease) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Increase " << amountToIncrease << " resources from node with id " << nodeId);
    if (indexOnNodeIds.find(nodeId) == indexOnNodeIds.end()) {
        NES_WARNING("Topology: Unable to find node with id " << nodeId);
        return false;
    }
    indexOnNodeIds[nodeId]->increaseResources(amountToIncrease);
    return true;
}

}// namespace NES
