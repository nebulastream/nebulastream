#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Topology/PhysicalNode.hpp>
#include <Topology/Topology.hpp>
#include <deque>

namespace NES {

Topology::Topology() : rootNode(nullptr) {}

TopologyPtr Topology::create() {
    return std::shared_ptr<Topology>();
}

bool Topology::addNewPhysicalNodeAsChild(PhysicalNodePtr parent, PhysicalNodePtr newNode) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_INFO("Topology: Adding Node " << newNode->toString() << " as child to the node " << parent->toString());
    return parent->addChild(newNode);
}

bool Topology::removePhysicalNode(PhysicalNodePtr nodeToRemove) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_INFO("Topology: Removing Node " << nodeToRemove->toString());
    return rootNode->remove(nodeToRemove);
}

PhysicalNodePtr Topology::findPathBetween(PhysicalNodePtr startNode, PhysicalNodePtr destinationNode) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_INFO("Topology: Finding path between " << startNode->toString() << " and " << destinationNode->toString());

    std::vector<PhysicalNodePtr> nodesInPath;
    bool found = find(startNode, destinationNode, nodesInPath);
    if (found) {
        for (auto& node : nodesInPath) {
            return node;
        }
    }
    return nullptr;
}

bool Topology::find(PhysicalNodePtr physicalNode, PhysicalNodePtr destinationNode, std::vector<PhysicalNodePtr>& nodesInPath) {

    std::vector<NodePtr> parents = physicalNode->getParents();
    if (parents.empty()) {
        return false;
    }

    if (physicalNode->getId() == destinationNode->getId()) {
        nodesInPath.push_back(physicalNode->copy());
        return true;
    }

    for (auto& parent : parents) {
        if (find(physicalNode, destinationNode, nodesInPath)) {
            nodesInPath.push_back(parent->as<PhysicalNode>()->copy());
            return true;
        }
    }
    return false;
}

void Topology::print() {
    std::unique_lock<std::mutex> lock(mutex);
    //FIXME: as part of #954
    std::stringstream topologyInfo;
    std::deque<PhysicalNodePtr> nodesToPrint{rootNode};

    while (!nodesToPrint.empty()) {
        topologyInfo << rootNode->getId() << "\t";
        for (auto& child : rootNode->getChildren()) {
            nodesToPrint.push_back(child->as<PhysicalNode>());
        }
    }
    NES_DEBUG("Topology:" << topologyInfo.str());
}

bool Topology::nodeExistsWithIpAndPort(std::string ipAddress, uint32_t grpcPort) {
    std::unique_lock<std::mutex> lock(mutex);
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
    std::unique_lock<std::mutex> lock(mutex);
    return rootNode;
}

PhysicalNodePtr Topology::findNodeWithId(uint64_t nodeId) {
    std::unique_lock<std::mutex> lock(mutex);
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
    std::unique_lock<std::mutex> lock(mutex);
    rootNode = physicalNode;
}

bool Topology::removeNodeAsChild(PhysicalNodePtr parentNode, PhysicalNodePtr childNode) {
    std::unique_lock<std::mutex> lock(mutex);
    return parentNode->remove(childNode);
}

}// namespace NES
