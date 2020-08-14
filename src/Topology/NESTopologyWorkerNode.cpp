#include <Topology/NESTopologyWorkerNode.hpp>
#include <assert.h>

namespace NES {

NESTopologyWorkerNode::NESTopologyWorkerNode(size_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint8_t cpuCapacity)
    : NESTopologyEntry(id, ipAddress, grpcPort, dataPort, cpuCapacity), isASink(false) {}

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

}// namespace NES
