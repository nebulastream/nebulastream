#include <Topology/NESTopologyWorkerNode.hpp>

namespace NES {

size_t NESTopologyWorkerNode::getCpuCapacity() {
    return cpuCapacity;
}

void NESTopologyWorkerNode::setCpuCapacity(CPUCapacity cpuCapacity) {
    this->cpuCapacity = cpuCapacity.toInt();
    this->remainingCPUCapacity = this->cpuCapacity;
}

void NESTopologyWorkerNode::reduceCpuCapacity(size_t usedCapacity) {
    assert(usedCapacity <= remainingCPUCapacity);
    this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
}

void NESTopologyWorkerNode::increaseCpuCapacity(size_t freedCapacity) {
    this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
}

size_t NESTopologyWorkerNode::getRemainingCpuCapacity() {
    return remainingCPUCapacity;
}

bool NESTopologyWorkerNode::getIsASinkNode() {
    return this->isASink;
}

NESNodeType NESTopologyWorkerNode::getEntryType() {
    return Worker;
}

std::string NESTopologyWorkerNode::getEntryTypeString() {
    if (isASink) {
        return "sink";
    }
    return "Worker";
}

void NESTopologyWorkerNode::setQuery(QueryPtr pQuery) {
    this->queryPtr = pQuery;
}

}// namespace NES
