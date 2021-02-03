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


#ifdef ENABLE_OPC_BUILD

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <mqtt/async_client.h>

namespace NES {

SourceDescriptorPtr MQTTSourceDescriptor::create(SchemaPtr schema, std::string serverAddress, std::string clientId,
                                                 std::string user, std::string password, std::string topic) {
    return std::make_shared<MQTTSourceDescriptor>(
        MQTTSourceDescriptor(std::move(schema), std::move(serverAddress), std::move(clientId), std::move(user), std::move(password),
                             std::move(topic)));
}

SourceDescriptorPtr MQTTSourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string serverAddress, std::string clientId,
                                                 std::string user, std::string password, std::string topic) {
    return std::make_shared<MQTTSourceDescriptor>(MQTTSourceDescriptor(std::move(schema), std::move(streamName),
                                                                      std::move(serverAddress), std::move(clientId), std::move(user), std::move(password),
                                                                      std::move(topic)));
}

MQTTSourceDescriptor::MQTTSourceDescriptor(SchemaPtr schema, std::string serverAddress, std::string clientId,
                                           std::string user, std::string password, std::string topic)
    : SourceDescriptor(std::move(schema)), serverAddress(std::move(serverAddress)), clientId(std::move(clientId)), user(std::move(user)),
      password(std::move(password)), topic(std::move(topic)) {}

MQTTSourceDescriptor::MQTTSourceDescriptor(SchemaPtr schema, std::string streamName, std::string serverAddress, std::string clientId,
                                           std::string user, std::string password, std::string topic)
    : SourceDescriptor(std::move(schema), std::move(streamName)), serverAddress(std::move(serverAddress)), clientId(std::move(clientId)), user(std::move(user)),
      password(std::move(password)), topic(std::move(topic)) {}

const std::string MQTTSourceDescriptor::getServerAddress() const { return serverAddress; }

const std::string MQTTSourceDescriptor::getClientId() const { return clientId; }

const std::string MQTTSourceDescriptor::getUser() const { return user; }

const std::string MQTTSourceDescriptor::getPassword() const { return password; }

const std::string MQTTSourceDescriptor::getTopic() const { return topic; }

bool MQTTSourceDescriptor::equal(SourceDescriptorPtr other) {

    if (!other->instanceOf<MQTTSourceDescriptor>())
        return false;
    auto otherMQTTSource = other->as<MQTTSourceDescriptor>();
    NES_DEBUG("URL= " << serverAddress << " == " << otherMQTTSource->getServerAddress());
    return serverAddress == otherMQTTSource->getServerAddress() && clientId == otherMQTTSource->getClientId()
        && user == otherMQTTSource->getUser() && password == otherMQTTSource->getPassword()
        && topic == otherMQTTSource->getTopic();
}

std::string MQTTSourceDescriptor::toString() { return "MQTTSourceDescriptor()"; }

}// namespace NES

#endif