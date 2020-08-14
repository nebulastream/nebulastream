#include <Topology/NESTopologyCoordinatorNode.hpp>
#include <assert.h>

namespace NES {

NESTopologyCoordinatorNode::NESTopologyCoordinatorNode(size_t nodeId, std::string ipAddress, int8_t cpuCapacity)
    : NESTopologyEntry(nodeId, ipAddress, 0, 0, cpuCapacity), isASink(false) {}

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

}// namespace NES
