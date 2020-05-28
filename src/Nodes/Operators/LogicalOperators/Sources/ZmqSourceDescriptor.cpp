#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>

namespace NES {

SourceDescriptorPtr ZmqSourceDescriptor::create(SchemaPtr schema, std::string host, uint16_t port) {
    return std::make_shared<ZmqSourceDescriptor>(ZmqSourceDescriptor(schema, host, port));
}

ZmqSourceDescriptor::ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port)
    : SourceDescriptor(schema), host(host), port(port) {}

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

}// namespace NES
