#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>

namespace NES {
namespace Network {

NetworkSinkDescriptor::NetworkSinkDescriptor(SchemaPtr schema, NodeLocation nodeLocation, NesPartition nesPartition,
                                             std::chrono::seconds waitTime, uint8_t retryTimes) :
    schema(schema), nodeLocation(nodeLocation), nesPartition(nesPartition), waitTime(waitTime), retryTimes(retryTimes) {
}

SinkDescriptorPtr NetworkSinkDescriptor::create(SchemaPtr schema, NodeLocation nodeLocation, NesPartition nesPartition,
                                                std::chrono::seconds waitTime, uint8_t retryTimes) {
    return std::make_shared<NetworkSinkDescriptor>(NetworkSinkDescriptor(schema, nodeLocation, nesPartition,
                                                                         waitTime, retryTimes));
}

bool NetworkSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<NetworkSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<NetworkSinkDescriptor>();
    return (schema == otherSinkDescriptor->schema) && (nesPartition == otherSinkDescriptor->nesPartition)
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

SchemaPtr NetworkSinkDescriptor::getSchema() const {
    return schema;
}

}
}