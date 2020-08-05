#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>

namespace NES {

ZmqSinkDescriptor::ZmqSinkDescriptor(std::string host, uint16_t port, bool internal)
    : host(host), port(port), internal(internal) {}

const std::string& ZmqSinkDescriptor::getHost() const {
    return host;
}

uint16_t ZmqSinkDescriptor::getPort() const {
    return port;
}
SinkDescriptorPtr ZmqSinkDescriptor::create(std::string host, uint16_t port, bool internal) {
    return std::make_shared<ZmqSinkDescriptor>(ZmqSinkDescriptor(host, port, internal));
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
bool ZmqSinkDescriptor::isInternal() const {
    return internal;
}
void ZmqSinkDescriptor::setInternal(bool internal) {
    ZmqSinkDescriptor::internal = internal;
}

}// namespace NES
