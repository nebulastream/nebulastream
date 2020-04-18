#include "Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp"

namespace NES {

ZmqSinkDescriptor::ZmqSinkDescriptor(SchemaPtr schema, std::string host, uint16_t port)
    : SinkDescriptor(schema), host(host), port(port) {}

SinkDescriptorType ZmqSinkDescriptor::getType() {
    return ZmqSink;
}

const std::string& ZmqSinkDescriptor::getHost() const {
    return host;
}

uint16_t ZmqSinkDescriptor::getPort() const {
    return port;
}

}

