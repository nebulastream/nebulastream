#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>

namespace NES {

ZmqSinkDescriptor::ZmqSinkDescriptor(std::string host, uint16_t port)
    : host(host), port(port) {}

const std::string& ZmqSinkDescriptor::getHost() const {
    return host;
}

uint16_t ZmqSinkDescriptor::getPort() const {
    return port;
}
SinkDescriptorPtr ZmqSinkDescriptor::create(std::string host, uint16_t port) {
    return std::make_shared<ZmqSinkDescriptor>(ZmqSinkDescriptor(host, port));
}

}// namespace NES
