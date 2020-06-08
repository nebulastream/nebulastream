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

std::string ZmqSinkDescriptor::toString() {
    return "ZmqSinkDescriptor()";
}
bool ZmqSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<ZmqSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<ZmqSinkDescriptor>();
    return port == otherSinkDescriptor->port && host == otherSinkDescriptor->host;
}

void ZmqSinkDescriptor::setPort(uint16_t port) {
    this->port = port;
}

}// namespace NES
