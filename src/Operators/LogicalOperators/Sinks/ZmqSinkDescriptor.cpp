/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>

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
