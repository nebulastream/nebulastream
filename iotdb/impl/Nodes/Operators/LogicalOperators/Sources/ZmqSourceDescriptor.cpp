#include "Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp"

namespace NES {

ZmqSourceDescriptor::ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port)
    : SourceDescriptor(schema), host(host), port(port) {}

SourceDescriptorType ZmqSourceDescriptor::getType() {
    return ZmqSinkDescriptor;
}

const std::string& ZmqSourceDescriptor::getHost() const {
    return host;
}

uint16_t ZmqSourceDescriptor::getPort() const {
    return port;
}

}


