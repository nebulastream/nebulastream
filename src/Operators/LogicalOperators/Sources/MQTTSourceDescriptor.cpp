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

#ifdef ENABLE_MQTT_BUILD

#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <mqtt/async_client.h>
#include <utility>

namespace NES {

SourceDescriptorPtr MQTTSourceDescriptor::create(SchemaPtr schema,
                                                 std::string serverAddress,
                                                 std::string clientId,
                                                 std::string user,
                                                 std::string topic,
                                                 DataType dataType,
                                                 uint32_t qos,
                                                 bool cleanSession) {
    return std::make_shared<MQTTSourceDescriptor>(MQTTSourceDescriptor(std::move(schema),
                                                                       std::move(serverAddress),
                                                                       std::move(clientId),
                                                                       std::move(user),
                                                                       std::move(topic),
                                                                       dataType,
                                                                       qos,
                                                                       cleanSession));
}

SourceDescriptorPtr MQTTSourceDescriptor::create(SchemaPtr schema,
                                                 std::string logicalStreamName,
                                                 std::string serverAddress,
                                                 std::string clientId,
                                                 std::string user,
                                                 std::string topic,
                                                 DataType dataType,
                                                 uint32_t qos,
                                                 bool cleanSession) {
    return std::make_shared<MQTTSourceDescriptor>(MQTTSourceDescriptor(std::move(schema),
                                                                       std::move(logicalStreamName),
                                                                       std::move(serverAddress),
                                                                       std::move(clientId),
                                                                       std::move(user),
                                                                       std::move(topic),
                                                                       dataType,
                                                                       qos,
                                                                       cleanSession));
}

MQTTSourceDescriptor::MQTTSourceDescriptor(SchemaPtr schema,
                                           std::string serverAddress,
                                           std::string clientId,
                                           std::string user,
                                           std::string topic,
                                           DataType dataType,
                                           uint32_t qos,
                                           bool cleanSession)
    : SourceDescriptor(std::move(schema)), serverAddress(std::move(serverAddress)), clientId(std::move(clientId)),
      user(std::move(user)), topic(std::move(topic)), dataType(dataType), qos(qos), cleanSession(cleanSession){}

MQTTSourceDescriptor::MQTTSourceDescriptor(SchemaPtr schema,
                                           std::string logicalStreamName,
                                           std::string serverAddress,
                                           std::string clientId,
                                           std::string user,
                                           std::string topic,
                                           DataType dataType,
                                           uint32_t qos,
                                           bool cleanSession)
    : SourceDescriptor(std::move(schema), std::move(logicalStreamName)), serverAddress(std::move(serverAddress)),
      clientId(std::move(clientId)), user(std::move(user)), topic(std::move(topic)), dataType(dataType), qos(qos), cleanSession(cleanSession){}

std::string MQTTSourceDescriptor::getServerAddress() const { return serverAddress; }

std::string MQTTSourceDescriptor::getClientId() const { return clientId; }

std::string MQTTSourceDescriptor::getUser() const { return user; }

std::string MQTTSourceDescriptor::getTopic() const { return topic; }

uint32_t MQTTSourceDescriptor::getQos() const { return qos; }

bool MQTTSourceDescriptor::getCleanSession() const {return cleanSession;}

std::string MQTTSourceDescriptor::toString() { return "MQTTSourceDescriptor()"; }

MQTTSourceDescriptor::DataType MQTTSourceDescriptor::getDataType() const { return dataType; }

bool MQTTSourceDescriptor::equal(SourceDescriptorPtr const& other) {

    if (!other->instanceOf<MQTTSourceDescriptor>()) {
        return false;
    }
    auto otherMQTTSource = other->as<MQTTSourceDescriptor>();
    NES_DEBUG("URL= " << serverAddress << " == " << otherMQTTSource->getServerAddress());
    return serverAddress == otherMQTTSource->getServerAddress() && clientId == otherMQTTSource->getClientId()
        && user == otherMQTTSource->getUser() && topic == otherMQTTSource->getTopic()
        && dataType == otherMQTTSource->getDataType() && qos == otherMQTTSource->getQos()
        && cleanSession == otherMQTTSource->getCleanSession();
}

}// namespace NES

#endif