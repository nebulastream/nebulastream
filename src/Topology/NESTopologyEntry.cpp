#include <Topology/NESTopologyEntry.hpp>
#include <Util/Logger.hpp>
#include <NodeStats.pb.h>
namespace NES {

void NESTopologyEntry::setNodeProperty(NodeStats nodeStats) {
    NES_DEBUG("setNodeProperty=" << nodeStats.DebugString());
    this->nodeStats = nodeStats;
}

/**
 * @brief method to get the node properties
 * @return serialized json of the node properties object
 */
std::string NESTopologyEntry::getNodeProperty() {
    return this->nodeStats.DebugString();
}

std::string NESTopologyEntry::toString() {
    return "id=" + std::to_string(getId()) + " type=" + getEntryTypeString();
}

NodeStats NESTopologyEntry::getNodeProperties() {
    return nodeStats;
}

void NESTopologyEntry::setId(size_t id) {
    this->id = id;
}

size_t NESTopologyEntry::getId() {
    return id;
}

void NESTopologyEntry::setIp(std::string ip) {
    this->ipAddress = ip;
}

std::string NESTopologyEntry::getIp() {
    return this->ipAddress;
}

uint16_t NESTopologyEntry::getPublishPort() {
    return publish_port;
}

void NESTopologyEntry::setPublishPort(uint16_t publishPort) {
    publish_port = publishPort;
}

uint16_t NESTopologyEntry::getReceivePort() {
    return receive_port;
}

uint16_t NESTopologyEntry::getNextFreeReceivePort() {
    receive_port = (receive_port + 12123123 + time(0) * 321 * rand() % 65535) + 1024;
    return receive_port;
}

void NESTopologyEntry::setReceivePort(uint16_t receivePort) {
    receive_port = receivePort;
}

}// namespace NES
