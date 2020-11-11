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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <utility>

namespace NES {

KafkaSourceDescriptor::KafkaSourceDescriptor(SchemaPtr schema,
                                             std::string brokers,
                                             std::string topic,
                                             std::string groupId,
                                             bool autoCommit,
                                             uint64_t kafkaConnectTimeout, SourceId sourceId)
    : SourceDescriptor(std::move(schema), sourceId),
      brokers(std::move(brokers)),
      topic(std::move(topic)),
      groupId(std::move(groupId)),
      autoCommit(autoCommit),
      kafkaConnectTimeout(kafkaConnectTimeout) {}

KafkaSourceDescriptor::KafkaSourceDescriptor(SchemaPtr schema,
                                             std::string streamName,
                                             std::string brokers,
                                             std::string topic,
                                             std::string groupId,
                                             bool autoCommit,
                                             uint64_t kafkaConnectTimeout, SourceId sourceId)
    : SourceDescriptor(std::move(schema), std::move(streamName), sourceId),
      brokers(std::move(brokers)),
      topic(std::move(topic)),
      groupId(std::move(groupId)),
      autoCommit(autoCommit),
      kafkaConnectTimeout(kafkaConnectTimeout) {}

SourceDescriptorPtr KafkaSourceDescriptor::create(SchemaPtr schema,
                                                  std::string brokers,
                                                  std::string streamName,
                                                  std::string topic,
                                                  std::string groupId,
                                                  bool autoCommit,
                                                  uint64_t kafkaConnectTimeout, SourceId sourceId) {
    return std::make_shared<KafkaSourceDescriptor>(KafkaSourceDescriptor(std::move(schema),
                                                                         std::move(streamName),
                                                                         std::move(brokers),
                                                                         std::move(topic),
                                                                         std::move(groupId),
                                                                         autoCommit,
                                                                         kafkaConnectTimeout, sourceId));
}

SourceDescriptorPtr KafkaSourceDescriptor::create(SchemaPtr schema,
                                                  std::string brokers,
                                                  std::string topic,
                                                  std::string groupId,
                                                  bool autoCommit,
                                                  uint64_t kafkaConnectTimeout, SourceId sourceId) {
    return std::make_shared<KafkaSourceDescriptor>(KafkaSourceDescriptor(std::move(schema),
                                                                         std::move(brokers),
                                                                         std::move(topic),
                                                                         std::move(groupId),
                                                                         autoCommit,
                                                                         kafkaConnectTimeout, sourceId));
}

const std::string& KafkaSourceDescriptor::getBrokers() const {
    return brokers;
}

const std::string& KafkaSourceDescriptor::getTopic() const {
    return topic;
}

bool KafkaSourceDescriptor::isAutoCommit() const {
    return autoCommit;
}

uint64_t KafkaSourceDescriptor::getKafkaConnectTimeout() const {
    return kafkaConnectTimeout;
}

const std::string& KafkaSourceDescriptor::getGroupId() const {
    return groupId;
}
bool KafkaSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<KafkaSourceDescriptor>())
        return false;
    auto otherKafkaSource = other->as<KafkaSourceDescriptor>();
    return brokers == otherKafkaSource->getBrokers() && topic == otherKafkaSource->getTopic() && kafkaConnectTimeout == otherKafkaSource->getKafkaConnectTimeout() && groupId == otherKafkaSource->getGroupId() && getSchema()->equals(other->getSchema());
}

std::string KafkaSourceDescriptor::toString() {
    return "KafkaSourceDescriptor()";
}

}// namespace NES
