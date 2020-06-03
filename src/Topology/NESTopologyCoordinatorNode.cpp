#include <Topology/NESTopologyCoordinatorNode.hpp>

namespace NES {

NESTopologyCoordinatorNode::NESTopologyCoordinatorNode(size_t nodeId,
                                                       std::string ipAddress) {
    this->id = nodeId;
    this->ipAddress = std::move(ipAddress);
    isASink = false;
    cpuCapacity = 0;
    remainingCPUCapacity = 0;
}

size_t NESTopologyCoordinatorNode::getCpuCapacity() {
    return cpuCapacity;
}

void NESTopologyCoordinatorNode::setCpuCapacity(CPUCapacity cpuCapacity) {
    this->cpuCapacity = cpuCapacity.toInt();
    this->remainingCPUCapacity = this->cpuCapacity;
}

void NESTopologyCoordinatorNode::reduceCpuCapacity(size_t usedCapacity) {
    assert(usedCapacity <= remainingCPUCapacity);
    this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
}

void NESTopologyCoordinatorNode::increaseCpuCapacity(size_t freedCapacity) {
    this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
}

size_t NESTopologyCoordinatorNode::getRemainingCpuCapacity() {
    return remainingCPUCapacity;
}

void NESTopologyCoordinatorNode::setSinkNode(bool isASink) {
    this->isASink = isASink;
}

bool NESTopologyCoordinatorNode::getIsASinkNode() {
    return this->isASink;
}

NESNodeType NESTopologyCoordinatorNode::getEntryType() {
    return Coordinator;
}

std::string NESTopologyCoordinatorNode::getEntryTypeString() {
    if (isASink) {
        return "sink";
    }
    return "Coordinator";
}

void NESTopologyCoordinatorNode::setQuery(QueryPtr pQuery) {
    this->query = pQuery;
}
}// namespace NES
