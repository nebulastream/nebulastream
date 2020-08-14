#include <NodeStats.pb.h>
#include <Topology/NESTopologyEntry.hpp>
#include <Util/Logger.hpp>
namespace NES {

NESTopologyEntry::NESTopologyEntry(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint8_t cpuCapacity)
    : id(id), ipAddress(ipAddress), grpcPort(grpcPort), dataPort(dataPort), cpuCapacity(cpuCapacity), remainingCPUCapacity(0) {}

void NESTopologyEntry::setNodeStats(NodeStats nodeStats) {
    NES_TRACE("setNodeStats=" << nodeStats.DebugString());
    this->nodeStats = nodeStats;
}

std::string NESTopologyEntry::toString() {
    return "id=" + std::to_string(getId()) + " type=" + getEntryTypeString();
}

NodeStats NESTopologyEntry::getNodeStats() {
    return nodeStats;
}

size_t NESTopologyEntry::getId() {
    return id;
}

uint8_t NESTopologyEntry::getCpuCapacity() {
    return cpuCapacity;
}

void NESTopologyEntry::reduceCpuCapacity(size_t usedCapacity) {
    assert(usedCapacity <= remainingCPUCapacity);
    this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
}

void NESTopologyEntry::increaseCpuCapacity(size_t freedCapacity) {
    this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
}

uint8_t NESTopologyEntry::getRemainingCpuCapacity() {
    return remainingCPUCapacity;
}

const std::string& NESTopologyEntry::getIpAddress() const {
    return ipAddress;
}

uint32_t NESTopologyEntry::getGrpcPort() const {
    return grpcPort;
}

uint32_t NESTopologyEntry::getDataPort() const {
    return dataPort;
}

}// namespace NES
