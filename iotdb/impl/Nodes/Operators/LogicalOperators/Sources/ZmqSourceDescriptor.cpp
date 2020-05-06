#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>

namespace NES {

SourceDescriptorPtr ZmqSourceDescriptor::create(SchemaPtr schema, std::string host, uint16_t port) {
    return std::make_shared<ZmqSourceDescriptor>(ZmqSourceDescriptor(schema, host, port));
}

ZmqSourceDescriptor::ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port)
    : SourceDescriptor(schema), host(host), port(port) {}

SourceDescriptorType ZmqSourceDescriptor::getType() {
    return ZmqSrc;
}

const std::string& ZmqSourceDescriptor::getHost() const {
    return host;
}

uint16_t ZmqSourceDescriptor::getPort() const {
    return port;
}

}// namespace NES
