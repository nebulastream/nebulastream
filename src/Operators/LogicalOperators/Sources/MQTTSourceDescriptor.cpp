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

SourceDescriptorPtr
MQTTSourceDescriptor::create(SchemaPtr schema, Configurations::MQTTSourceConfigPtr sourceConfigPtr, SourceDescriptor::InputFormat inputFormat) {
    return std::make_shared<MQTTSourceDescriptor>(MQTTSourceDescriptor(std::move(schema), std::move(sourceConfigPtr), inputFormat));
}

MQTTSourceDescriptor::MQTTSourceDescriptor(SchemaPtr schema,
                                           Configurations::MQTTSourceConfigPtr sourceConfigPtr,
                                           SourceDescriptor::InputFormat inputFormat)
    : SourceDescriptor(std::move(schema)), sourceConfigPtr(std::move(sourceConfigPtr)), inputFormat(inputFormat) {}

Configurations::MQTTSourceConfigPtr MQTTSourceDescriptor::getSourceConfigPtr() const { return sourceConfigPtr; }

std::string MQTTSourceDescriptor::toString() { return "MQTTSourceDescriptor(" + sourceConfigPtr->toString() + ")"; }

SourceDescriptor::InputFormat MQTTSourceDescriptor::getInputFormat() const { return inputFormat; }

bool MQTTSourceDescriptor::equal(SourceDescriptorPtr const& other) {

    if (!other->instanceOf<MQTTSourceDescriptor>()) {
        return false;
    }
    auto otherMQTTSource = other->as<MQTTSourceDescriptor>();
    NES_DEBUG("URL= " << sourceConfigPtr->getUrl()->getValue()
                      << " == " << otherMQTTSource->getSourceConfigPtr()->getUrl()->getValue());
    return sourceConfigPtr->getUrl()->getValue() == otherMQTTSource->getSourceConfigPtr()->getUrl()->getValue()
        && sourceConfigPtr->getClientId()->getValue() == otherMQTTSource->getSourceConfigPtr()->getClientId()->getValue()
        && sourceConfigPtr->getUserName()->getValue() == otherMQTTSource->getSourceConfigPtr()->getUserName()->getValue()
        && sourceConfigPtr->getTopic()->getValue() == otherMQTTSource->getSourceConfigPtr()->getTopic()->getValue()
        && inputFormat == otherMQTTSource->getInputFormat()
        && sourceConfigPtr->getQos()->getValue() == otherMQTTSource->getSourceConfigPtr()->getQos()->getValue()
        && sourceConfigPtr->getCleanSession()->getValue() == otherMQTTSource->getSourceConfigPtr()->getCleanSession()->getValue();
}

}// namespace NES

#endif