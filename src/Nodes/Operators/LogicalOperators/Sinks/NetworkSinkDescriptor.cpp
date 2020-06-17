#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>

namespace NES {
namespace Network {

NetworkSinkDescriptor::NetworkSinkDescriptor(NodeLocation nodeLocation, NesPartition nesPartition,
                                             std::chrono::seconds waitTime, uint32_t retryTimes) : nodeLocation(nodeLocation), nesPartition(nesPartition), waitTime(waitTime), retryTimes(retryTimes) {
}

SinkDescriptorPtr NetworkSinkDescriptor::create(NodeLocation nodeLocation, NesPartition nesPartition,
                                                std::chrono::seconds waitTime, uint32_t retryTimes) {
    return std::make_shared<NetworkSinkDescriptor>(NetworkSinkDescriptor(nodeLocation, nesPartition,
                                                                         waitTime, retryTimes));
}

bool NetworkSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<NetworkSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<NetworkSinkDescriptor>();
    return (nesPartition == otherSinkDescriptor->nesPartition)
        && (nodeLocation == otherSinkDescriptor->nodeLocation) && (waitTime == otherSinkDescriptor->waitTime)
        && (retryTimes == otherSinkDescriptor->retryTimes);
}

std::string NetworkSinkDescriptor::toString() {
    return "NetworkSinkDescriptor()";
}

NodeLocation NetworkSinkDescriptor::getNodeLocation() const {
    return nodeLocation;
}

NesPartition NetworkSinkDescriptor::getNesPartition() const {
    return nesPartition;
}

std::chrono::seconds NetworkSinkDescriptor::getWaitTime() const {
    return waitTime;
}

uint8_t NetworkSinkDescriptor::getRetryTimes() const {
    return retryTimes;
}

}// namespace Network
}// namespace NES