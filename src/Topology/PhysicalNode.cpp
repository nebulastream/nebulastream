#include <Topology/PhysicalNode.hpp>

namespace NES {

PhysicalNode::PhysicalNode(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resource)
    : id(id), ipAddress(ipAddress), grpcPort(grpcPort), dataPort(dataPort), resource(resource), usedResource(0) {}

PhysicalNodePtr PhysicalNode::create(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resource) {
    return std::shared_ptr<PhysicalNode>(new PhysicalNode(id, ipAddress, grpcPort, dataPort, resource));
}

uint64_t PhysicalNode::getId() {
    return id;
}

uint32_t PhysicalNode::getGrpcPort() const {
    return grpcPort;
}

uint32_t PhysicalNode::getDataPort() const {
    return dataPort;
}

void PhysicalNode::setNodeStats(NodeStats nodeStats) {
    PhysicalNode::nodeStats = nodeStats;
}

NodeStats PhysicalNode::getNodeStats() {
    return nodeStats;
}

uint16_t PhysicalNode::getAvailableResource() {
    return resource - usedResource;
}

void PhysicalNode::increaseResource(uint16_t freedCapacity) {
    NES_ASSERT(freedCapacity > resource, "PhysicalNode: amount of resources to free can't be more than actual resources");
    NES_ASSERT(freedCapacity > (resource - usedResource), "PhysicalNode: amount of resources to free can't be more than actual consumed resources");
    usedResource = usedResource - freedCapacity;
}

void PhysicalNode::reduceResource(uint16_t usedCapacity) {
    NES_ASSERT(usedCapacity > resource, "PhysicalNode: amount of resources to be used can't be more than actual resources");
    NES_ASSERT(usedCapacity > (resource - usedResource), "PhysicalNode: amount of resources to be used can't be more than available resources");
    usedResource = usedResource + usedCapacity;
}

PhysicalNodePtr PhysicalNode::copy() {
    PhysicalNodePtr copy = std::shared_ptr<PhysicalNode>(new PhysicalNode(id, ipAddress, grpcPort, dataPort, resource));
    copy->reduceResource(usedResource);
    return copy;
}

const std::string& PhysicalNode::getIpAddress() const {
    return ipAddress;
}

const std::string PhysicalNode::toString() const {
    std::stringstream ss;
    ss << "PhysicalNode[id=" + std::to_string(id) + ", ip = " + ipAddress + ", grpcPort = ,"
            + std::to_string(grpcPort) + ", dataPort= " + std::to_string(dataPort) + ", resources = " + std::to_string(resource)
            + ", usedResource=" + std::to_string(usedResource) + "]";
    return ss.str();
}

}// namespace NES
