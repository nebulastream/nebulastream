#include <Topology/NESTopologySensorNode.hpp>
#include <assert.h>
namespace NES {

size_t NESTopologySensorNode::getCpuCapacity() {
    return cpuCapacity;
}

void NESTopologySensorNode::setCpuCapacity(CPUCapacity cpuCapacity) {
    this->cpuCapacity = cpuCapacity.toInt();
    this->remainingCPUCapacity = this->cpuCapacity;
}

void NESTopologySensorNode::reduceCpuCapacity(size_t usedCapacity) {
    assert(usedCapacity <= remainingCPUCapacity);
    this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
}

void NESTopologySensorNode::increaseCpuCapacity(size_t freedCapacity) {
    this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
}

size_t NESTopologySensorNode::getRemainingCpuCapacity() {
    return remainingCPUCapacity;
}

NESNodeType NESTopologySensorNode::getEntryType() {
    return Sensor;
}

std::string NESTopologySensorNode::getEntryTypeString() {
    return "Sensor(" + physicalStreamName + ")";
}

std::string NESTopologySensorNode::getPhysicalStreamName() {
    return physicalStreamName;
}

void NESTopologySensorNode::setPhysicalStreamName(std::string name) {
    this->physicalStreamName = name;
}

}// namespace NES
