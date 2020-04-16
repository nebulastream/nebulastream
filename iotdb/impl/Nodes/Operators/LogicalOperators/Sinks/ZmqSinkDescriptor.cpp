#include "Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp"

namespace NES {

ZmqSinkDescriptor::ZmqSinkDescriptor(std::string host, uint16_t port, size_t tupleCnt)
    : host(host), port(port), tupleCnt(tupleCnt) {}

const std::string& ZmqSinkDescriptor::getHost() const {
    return host;
}

uint16_t ZmqSinkDescriptor::getPort() const {
    return port;
}

size_t ZmqSinkDescriptor::getTupleCnt() const {
    return tupleCnt;
}

}

