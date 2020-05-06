#include <Topology/NESTopologyEntry.hpp>

namespace NES {

void NESTopologyEntry::setNodeProperty(std::string nodeProperties) {
    if (nodeProperties != "")
        this->nodeProperties = std::make_shared<NodeProperties>(nodeProperties);
    else
        this->nodeProperties = std::make_shared<NodeProperties>();
}

/**
 * @brief method to get the node properties
 * @return serialized json of the node properties object
 */
std::string NESTopologyEntry::getNodeProperty() {
    return this->nodeProperties->dump();
}

std::string NESTopologyEntry::toString() {
    return "id=" + std::to_string(getId()) + " type=" + getEntryTypeString();
}

NodePropertiesPtr NESTopologyEntry::getNodeProperties() {
    return nodeProperties;
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
    receive_port++;
    return receive_port;
}

void NESTopologyEntry::setReceivePort(uint16_t receivePort) {
    receive_port = receivePort;
}

}// namespace NES
