#include <Topology/TopologyNode.hpp>

namespace NES {

TopologyNode::TopologyNode(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources)
    : id(id), ipAddress(ipAddress), grpcPort(grpcPort), dataPort(dataPort), resources(resources), usedResources(0) {}

TopologyNodePtr TopologyNode::create(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources) {
    return std::make_shared<TopologyNode>(id, ipAddress, grpcPort, dataPort, resources);
}

uint64_t TopologyNode::getId() {
    return id;
}

uint32_t TopologyNode::getGrpcPort() const {
    return grpcPort;
}

uint32_t TopologyNode::getDataPort() const {
    return dataPort;
}

void TopologyNode::setNodeStats(NodeStats nodeStats) {
    this->nodeStats = nodeStats;
}

NodeStats TopologyNode::getNodeStats() {
    return nodeStats;
}

uint16_t TopologyNode::getAvailableResources() {
    return resources - usedResources;
}

void TopologyNode::increaseResources(uint16_t freedCapacity) {
    NES_ASSERT(freedCapacity <= resources, "PhysicalNode: amount of resources to free can't be more than actual resources");
    NES_ASSERT(freedCapacity <= usedResources, "PhysicalNode: amount of resources to free can't be more than actual consumed resources");
    usedResources = usedResources - freedCapacity;
}

void TopologyNode::reduceResources(uint16_t usedCapacity) {
    NES_ASSERT(usedCapacity <= resources, "PhysicalNode: amount of resources to be used can't be more than actual resources");
    NES_ASSERT(usedCapacity <= (resources - usedResources), "PhysicalNode: amount of resources to be used can't be more than available resources");
    usedResources = usedResources + usedCapacity;
}

TopologyNodePtr TopologyNode::copy() {
    TopologyNodePtr copy = std::make_shared<TopologyNode>(TopologyNode(id, ipAddress, grpcPort, dataPort, resources));
    copy->reduceResources(usedResources);
    return copy;
}

const std::string TopologyNode::getIpAddress() const {
    return ipAddress;
}

const std::string TopologyNode::toString() const {
    std::stringstream ss;
    ss << "PhysicalNode[id=" + std::to_string(id) + ", ip=" + ipAddress + ", resourceCapacity=" + std::to_string(resources)
            + ", usedResource=" + std::to_string(usedResources) + "]";
    return ss.str();
}

bool TopologyNode::containAsParent(NodePtr node) {
    std::vector<NodePtr> ancestors = this->getAndFlattenAllAncestors();
    auto found = std::find_if(ancestors.begin(), ancestors.end(), [node](NodePtr familyMember) {
        return familyMember->as<TopologyNode>()->getId() == node->as<TopologyNode>()->getId();
    });
    return found != ancestors.end();
}

bool TopologyNode::containAsChild(NodePtr node) {
    std::vector<NodePtr> children = this->getAndFlattenAllChildren();
    auto found = std::find_if(children.begin(), children.end(), [node](NodePtr familyMember) {
        return familyMember->as<TopologyNode>()->getId() == node->as<TopologyNode>()->getId();
    });
    return found != children.end();
}

}// namespace NES
