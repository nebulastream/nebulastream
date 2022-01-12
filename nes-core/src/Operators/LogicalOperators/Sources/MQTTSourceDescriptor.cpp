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
                                                 Configurations::MQTTSourceTypeConfigPtr sourceConfig,
                                                 SourceDescriptor::InputFormat inputFormat) {
    return std::make_shared<MQTTSourceDescriptor>(MQTTSourceDescriptor(std::move(schema), std::move(sourceConfig), inputFormat));
}

MQTTSourceDescriptor::MQTTSourceDescriptor(SchemaPtr schema,
                                           Configurations::MQTTSourceTypeConfigPtr sourceConfig,
                                           SourceDescriptor::InputFormat inputFormat)
    : SourceDescriptor(std::move(schema)), sourceConfig(std::move(sourceConfig)), inputFormat(inputFormat) {}

Configurations::MQTTSourceTypeConfigPtr MQTTSourceDescriptor::getSourceConfigPtr() const { return sourceConfig; }

std::string MQTTSourceDescriptor::toString() { return "MQTTSourceDescriptor(" + sourceConfig->toString() + ")"; }

SourceDescriptor::InputFormat MQTTSourceDescriptor::getInputFormat() const { return inputFormat; }

bool MQTTSourceDescriptor::equal(SourceDescriptorPtr const& other) {

    if (!other->instanceOf<MQTTSourceDescriptor>()) {
        return false;
    }
    auto otherMQTTSource = other->as<MQTTSourceDescriptor>();
    return sourceConfig->equal(otherMQTTSource->sourceConfig);
}

}// namespace NES

#endif