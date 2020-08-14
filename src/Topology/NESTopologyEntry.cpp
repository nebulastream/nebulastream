#include <NodeStats.pb.h>
#include <Topology/NESTopologyEntry.hpp>
#include <Util/Logger.hpp>
namespace NES {

NESTopologyEntry::NESTopologyEntry(uint64_t id, std::string ipAddress, int32_t grpcPort, int32_t dataPort, int8_t cpuCapacity)
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

uint64_t NESTopologyEntry::getId() {
    return id;
}

int8_t NESTopologyEntry::getCpuCapacity() {
    return cpuCapacity;
}

void NESTopologyEntry::reduceCpuCapacity(int8_t usedCapacity) {
    assert(usedCapacity <= remainingCPUCapacity);
    this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
}

void NESTopologyEntry::increaseCpuCapacity(int8_t freedCapacity) {
    this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
}

int8_t NESTopologyEntry::getRemainingCpuCapacity() {
    return remainingCPUCapacity;
}

const std::string& NESTopologyEntry::getIpAddress() const {
    return ipAddress;
}

int32_t NESTopologyEntry::getGrpcPort() const {
    return grpcPort;
}

int32_t NESTopologyEntry::getDataPort() const {
    return dataPort;
}

}// namespace NES
