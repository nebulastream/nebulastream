#include <Topology/PhysicalNode.hpp>
#include <Topology/Topology.hpp>

namespace NES {

Topology::Topology(PhysicalNodePtr rootNode) : rootNode(rootNode) {}

TopologyPtr Topology::create(PhysicalNodePtr rootNode) {
    return std::shared_ptr<Topology>(new Topology(rootNode));
}

bool Topology::addNewPhysicalNodeAsChild(PhysicalNodePtr parent, PhysicalNodePtr newNode) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_INFO("Topology: Adding Node " << newNode->toString() << " as child to the node " << parent->toString());
    return parent->addChild(newNode);
}

bool Topology::addNewPhysicalNodeAsParent(PhysicalNodePtr child, PhysicalNodePtr newNode) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_INFO("Topology: Adding Node " << newNode->toString() << " as parent to the node " << child->toString());
    return child->addParent(newNode);
}

bool Topology::removePhysicalNode(PhysicalNodePtr nodeToRemove) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_INFO("Topology: Removing Node " << nodeToRemove->toString());
    return rootNode->remove(nodeToRemove);
}

}// namespace NES
