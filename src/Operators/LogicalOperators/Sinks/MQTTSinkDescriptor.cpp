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

#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>

namespace NES {

MQTTSinkDescriptor::MQTTSinkDescriptor(const std::string& address, const std::string& clientId,
                                       const std::string& topic, const std::string& user, const uint32_t maxBufferedMSGs,
                                       const char timeUnit, const uint64_t msgDelay, const bool asynchronousClient)
                                        : address(address), clientId(clientId), topic(topic), user(user),
                                          maxBufferedMSGs(maxBufferedMSGs), timeUnit(timeUnit), msgDelay(msgDelay),
                                          asynchronousClient(asynchronousClient){}

const std::string& MQTTSinkDescriptor::getAddress() const { return address; }
const std::string& MQTTSinkDescriptor::getClientId() const { return clientId; }
const std::string& MQTTSinkDescriptor::getTopic() const { return topic; }
const std::string& MQTTSinkDescriptor::getUser() const { return user; }
const uint32_t MQTTSinkDescriptor::getMaxBufferedMSGs() const { return maxBufferedMSGs; }
const char MQTTSinkDescriptor::getTimeUnit() const { return timeUnit; }
const uint64_t MQTTSinkDescriptor::getMsgDelay() const { return msgDelay; }
const bool MQTTSinkDescriptor::getAsynchronousClient() const { return asynchronousClient; }

SinkDescriptorPtr MQTTSinkDescriptor::create(const std::string& address, const std::string& clientId,
                                             const std::string& topic, const std::string& user, const uint32_t maxBufferedMSGs,
                                             const char timeUnit, const uint64_t msgDelay, const bool asynchronousClient) {
    return std::make_shared<MQTTSinkDescriptor>(MQTTSinkDescriptor(address, clientId, topic, user, maxBufferedMSGs,
                                                                   timeUnit, msgDelay, asynchronousClient));
}

std::string MQTTSinkDescriptor::toString() { return "MQTTSinkDescriptor()"; }
bool MQTTSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<MQTTSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<MQTTSinkDescriptor>();
    return address == otherSinkDescriptor->address &&
           clientId == otherSinkDescriptor->clientId && topic == otherSinkDescriptor->topic &&
           user == otherSinkDescriptor->user && maxBufferedMSGs == otherSinkDescriptor->maxBufferedMSGs &&
           timeUnit == otherSinkDescriptor->timeUnit && msgDelay == otherSinkDescriptor->msgDelay &&
           asynchronousClient == otherSinkDescriptor->asynchronousClient;
}
}// namespace NES

#endif
