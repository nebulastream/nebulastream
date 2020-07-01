#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <utility>

namespace NES {

SourceDescriptorPtr ZmqSourceDescriptor::create(SchemaPtr schema, std::string host, uint16_t port) {
    return std::make_shared<ZmqSourceDescriptor>(ZmqSourceDescriptor(std::move(schema), std::move(host), port));
}

SourceDescriptorPtr ZmqSourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string host, uint16_t port) {
    return std::make_shared<ZmqSourceDescriptor>(ZmqSourceDescriptor(std::move(schema), std::move(streamName), std::move(host), port));
}

ZmqSourceDescriptor::ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port)
    : SourceDescriptor(std::move(schema)), host(std::move(host)), port(port) {}

ZmqSourceDescriptor::ZmqSourceDescriptor(SchemaPtr schema, std::string streamName, std::string host, uint16_t port)
    : SourceDescriptor(std::move(schema), std::move(streamName)), host(std::move(host)), port(port) {}

const std::string& ZmqSourceDescriptor::getHost() const {
    return host;
}

uint16_t ZmqSourceDescriptor::getPort() const {
    return port;
}
bool ZmqSourceDescriptor::equal(SourceDescriptorPtr other) {

    if (!other->instanceOf<ZmqSourceDescriptor>())
        return false;
    auto otherZMQSource = other->as<ZmqSourceDescriptor>();
    return host == otherZMQSource->getHost() && port == otherZMQSource->getPort() && getSchema()->equals(other->getSchema());
}

std::string ZmqSourceDescriptor::toString() {
    return "ZmqSourceDescriptor()";
}

void ZmqSourceDescriptor::setPort(uint16_t port) {
    this->port = port;
}

}// namespace NES
