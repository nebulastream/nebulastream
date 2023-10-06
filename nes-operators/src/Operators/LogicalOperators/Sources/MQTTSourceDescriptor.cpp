/*
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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <utility>

namespace NES {

SourceDescriptorPtr MQTTSourceDescriptor::create(SchemaPtr schema, MQTTSourceTypePtr sourceConfig) {
    return std::make_shared<MQTTSourceDescriptor>(MQTTSourceDescriptor(std::move(schema), std::move(sourceConfig)));
}

MQTTSourceDescriptor::MQTTSourceDescriptor(SchemaPtr schema, MQTTSourceTypePtr mqttSourceType)
    : SourceDescriptor(std::move(schema)), mqttSourceType(std::move(mqttSourceType)) {}

MQTTSourceTypePtr MQTTSourceDescriptor::getSourceConfigPtr() const { return mqttSourceType; }

std::string MQTTSourceDescriptor::toString() const { return "MQTTSourceDescriptor(" + mqttSourceType->toString() + ")"; }

bool MQTTSourceDescriptor::equal(SourceDescriptorPtr const& other) const {

    if (!other->instanceOf<MQTTSourceDescriptor>()) {
        return false;
    }
    auto otherMQTTSource = other->as<MQTTSourceDescriptor>();
    return mqttSourceType->equal(otherMQTTSource->mqttSourceType);
}

SourceDescriptorPtr MQTTSourceDescriptor::copy() {
    auto copy = MQTTSourceDescriptor::create(schema->copy(), mqttSourceType);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

}// namespace NES

#endif