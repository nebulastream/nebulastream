#include "Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp"

namespace NES {

ZmqSourceDescriptor::ZmqSourceDescriptor(std::string host, uint16_t port) : host(host), port(port) {}

SourceDescriptorType ZmqSourceDescriptor::getType() {
    return ZmqDescriptor;
}

const std::string& ZmqSourceDescriptor::getHost() const {
    return host;
}

uint16_t ZmqSourceDescriptor::getPort() const {
    return port;
}

}


