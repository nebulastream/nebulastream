#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Topology/PhysicalNode.hpp>
#include <Topology/Topology.hpp>
#include <deque>

namespace NES {

Topology::Topology() : rootNode(nullptr), mutex() {}

TopologyPtr Topology::create() {
    return std::shared_ptr<Topology>(new Topology());
}

bool Topology::addNewPhysicalNodeAsChild(PhysicalNodePtr parent, PhysicalNodePtr newNode) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Adding Node " << newNode->toString() << " as child to the node " << parent->toString());
    return parent->addChild(newNode);
}

bool Topology::removePhysicalNode(PhysicalNodePtr nodeToRemove) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Removing Node " << nodeToRemove->toString());

    if (!rootNode) {
        NES_WARNING("Topology: No root not exists in the topology");
        return false;
    }

    if (rootNode->getId() == nodeToRemove->getId()) {
        NES_WARNING("Topology: Attempt to remove the root node. Removing root node is not allowed.");
        return false;
    }
    return rootNode->remove(nodeToRemove);
}

PhysicalNodePtr Topology::findPathBetween(PhysicalNodePtr startNode, PhysicalNodePtr destinationNode) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Finding path between " << startNode->toString() << " and " << destinationNode->toString());

    std::vector<PhysicalNodePtr> nodesInPath;
    bool found = find(startNode, destinationNode, nodesInPath);
    if (found) {
        NES_INFO("Topology: Found path between " << startNode->toString() << " and " << destinationNode->toString());
        PhysicalNodePtr previousNode;
        for(auto itr = nodesInPath.begin(); itr != nodesInPath.end(); itr++){
            if(previousNode){
                previousNode->addChild(*itr);
            }
            previousNode = *itr;
        }
        return previousNode;
    }
    NES_WARNING("Topology: Unable to find path between " << startNode->toString() << " and " << destinationNode->toString());
    return nullptr;
}

bool Topology::find(PhysicalNodePtr physicalNode, PhysicalNodePtr destinationNode, std::vector<PhysicalNodePtr>& nodesInPath) {

    if (physicalNode->getId() == destinationNode->getId()) {
        NES_INFO("Topology: found the destination node");
        nodesInPath.push_back(physicalNode->copy());
        return true;
    }

    std::vector<NodePtr> parents = physicalNode->getParents();
    if (parents.empty()) {
        NES_WARNING("Topology: reached end of the tree but destination node not found.");
        return false;
    }

    for (auto& parent : parents) {
        if (find(parent->as<PhysicalNode>(), destinationNode, nodesInPath)) {
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
    std::deque<PhysicalNodePtr> parentToPrint{rootNode};
    std::deque<PhysicalNodePtr> childToPrint;

    while (!parentToPrint.empty()) {
        PhysicalNodePtr nodeToPrint = parentToPrint.front();
        parentToPrint.pop_front();
        topologyInfo << nodeToPrint->getId() << "\t";

        for (auto& child : nodeToPrint->getChildren()) {
            childToPrint.push_back(child->as<PhysicalNode>());
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
    BreadthFirstNodeIterator bfsIterator(rootNode);
    for (auto itr = bfsIterator.begin(); itr != bfsIterator.end(); ++itr) {
        auto physicalNode = (*itr)->as<PhysicalNode>();
        if (physicalNode->getIpAddress() == ipAddress && physicalNode->getGrpcPort() == grpcPort) {
            return true;
        }
    }
    return false;
}

PhysicalNodePtr Topology::getRoot() {
    std::unique_lock lock(mutex);
    return rootNode;
}

PhysicalNodePtr Topology::findNodeWithId(uint64_t nodeId) {
    std::unique_lock lock(mutex);
    NES_INFO("Topology: Removing a physical node with id " << nodeId);
    BreadthFirstNodeIterator bfsIterator(rootNode);
    for (auto itr = bfsIterator.begin(); itr != bfsIterator.end(); ++itr) {
        auto physicalNode = (*itr)->as<PhysicalNode>();
        if (physicalNode->getId() == nodeId) {
            return physicalNode;
        }
    }
    return nullptr;
}

void Topology::setAsRoot(PhysicalNodePtr physicalNode) {
    std::unique_lock lock(mutex);
    rootNode = physicalNode;
}

bool Topology::removeNodeAsChild(PhysicalNodePtr parentNode, PhysicalNodePtr childNode) {
    std::unique_lock lock(mutex);
    return parentNode->remove(childNode);
}

}// namespace NES
