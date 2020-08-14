#include <Topology/NESTopologySensorNode.hpp>
#include <assert.h>
namespace NES {

NESTopologySensorNode::NESTopologySensorNode(size_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint8_t cpuCapacity)
    : NESTopologyEntry(id, ipAddress, grpcPort, dataPort, cpuCapacity), physicalStreamName("default_physical") {}

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
