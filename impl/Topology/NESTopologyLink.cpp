#include <Topology/NESTopologyLink.hpp>
#include <Util/Logger.hpp>
namespace NES {

size_t NESTopologyLink::getId() {
    return linkId;
}

NESTopologyEntryPtr NESTopologyLink::getSourceNode() {
    return sourceNode;
}

size_t NESTopologyLink::getSourceNodeId() {
    return sourceNode->getId();
}

NESTopologyEntryPtr NESTopologyLink::getDestNode() {
    return destNode;
}

size_t NESTopologyLink::getDestNodeId() {
    return destNode->getId();
}

LinkType NESTopologyLink::getLinkType() {
    if (sourceNode->getEntryType() == Worker
        && destNode->getEntryType() == Worker) {
        return NodeToNode;
    } else if (sourceNode->getEntryType() == Sensor && destNode->getEntryType() == Worker) {
        return SensorToNode;
    } else if (sourceNode->getEntryType() == Worker
               && destNode->getEntryType() == Sensor) {
        return NodeToSensor;
    }
    NES_FATAL_ERROR("Unrecognized LinkType!");
}

std::string NESTopologyLink::getLinkTypeString() {
    switch (getLinkType()) {
        case NodeToNode: return "NodeToNode";
        case SensorToNode: return "SensorToNode";
        case NodeToSensor: return "NodeToSensor";
    }
    NES_FATAL_ERROR("String for LinkType not found!");
}

void NESTopologyLink::updateLinkLatency(size_t linkLatency) {
    NESTopologyLink::linkLatency = linkLatency;
}

void NESTopologyLink::updateLinkCapacity(size_t linkCapacity) {
    NESTopologyLink::linkCapacity = linkCapacity;
}

size_t NESTopologyLink::getLinkLatency() const {
    return linkLatency;
}

size_t NESTopologyLink::getLinkCapacity() const {
    return linkCapacity;
}

}// namespace NES
