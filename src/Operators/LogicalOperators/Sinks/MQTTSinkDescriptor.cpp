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

#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>

namespace NES {

MQTTSinkDescriptor::MQTTSinkDescriptor(const std::string& host, uint16_t port, const std::string& clientId,
                                       const std::string& topic, const std::string& user)
                                        : host(host), port(port), clientId(clientId), topic(topic), user(user) {}

const std::string& MQTTSinkDescriptor::getHost() const { return host; }
uint16_t MQTTSinkDescriptor::getPort() const { return port; }
const std::string& MQTTSinkDescriptor::getClientId() const { return clientId; }
const std::string& MQTTSinkDescriptor::getTopic() const { return topic; }
const std::string& MQTTSinkDescriptor::getUser() const { return user; }

SinkDescriptorPtr MQTTSinkDescriptor::create(const std::string& host, uint16_t port, const std::string& clientId,
                                             const std::string& topic, const std::string& user) {
    return std::make_shared<MQTTSinkDescriptor>(MQTTSinkDescriptor(host, port, clientId, topic, user));
}

std::string MQTTSinkDescriptor::toString() { return "MQTTSinkDescriptor()"; }
bool MQTTSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<MQTTSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<MQTTSinkDescriptor>();
    return port == otherSinkDescriptor->port && host == otherSinkDescriptor->host &&
           clientId == otherSinkDescriptor->clientId && topic == otherSinkDescriptor->topic &&
           user == otherSinkDescriptor->user;
}

void MQTTSinkDescriptor::setPort(uint16_t newPort) { this->port = newPort; }

}// namespace NES
