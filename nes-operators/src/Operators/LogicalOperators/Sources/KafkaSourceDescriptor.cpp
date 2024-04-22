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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <utility>

namespace NES {

KafkaSourceDescriptor::KafkaSourceDescriptor(SchemaPtr schema,
                                             std::string brokers,
                                             std::string topic,
                                             std::string groupId,
                                             bool autoCommit,
                                             uint64_t kafkaConnectTimeout,
                                             const std::string& offsetMode,
                                             const KafkaSourceTypePtr& kafkaSourceType,
                                             uint64_t numbersOfBufferToProduce,
                                             uint64_t batchSize)
    : SourceDescriptor(std::move(schema)), brokers(std::move(brokers)), topic(std::move(topic)), groupId(std::move(groupId)),
      kafkaSourceType(kafkaSourceType), autoCommit(autoCommit), kafkaConnectTimeout(kafkaConnectTimeout), offsetMode(offsetMode), numbersOfBufferToProduce(numbersOfBufferToProduce), batchSize(batchSize) {}

KafkaSourceDescriptor::KafkaSourceDescriptor(SchemaPtr schema, KafkaSourceTypePtr kafkaSourceType)
    : SourceDescriptor(std::move(schema)), kafkaSourceType(std::move(kafkaSourceType)) {}

KafkaSourceDescriptor::KafkaSourceDescriptor(SchemaPtr schema,
                                             std::string logicalSourceName,
                                             std::string brokers,
                                             std::string topic,
                                             std::string groupId,
                                             bool autoCommit,
                                             uint64_t kafkaConnectTimeout,
                                             const std::string& offsetMode,
                                             const KafkaSourceTypePtr& kafkaSourceType,
                                             uint64_t numbersOfBufferToProduce,
                                             uint64_t batchSize)
    : SourceDescriptor(std::move(schema), std::move(logicalSourceName)), brokers(std::move(brokers)), topic(std::move(topic)),
      groupId(std::move(groupId)), kafkaSourceType(kafkaSourceType), autoCommit(autoCommit),
      kafkaConnectTimeout(kafkaConnectTimeout), offsetMode(offsetMode), numbersOfBufferToProduce(numbersOfBufferToProduce),
      batchSize(batchSize) {}

SourceDescriptorPtr KafkaSourceDescriptor::create(SchemaPtr schema,
                                                  std::string brokers,
                                                  std::string logicalSourceName,
                                                  std::string topic,
                                                  std::string groupId,
                                                  bool autoCommit,
                                                  uint64_t kafkaConnectTimeout,
                                                  const std::string& offsetMode,
                                                  const KafkaSourceTypePtr& kafkaSourceType,
                                                  uint64_t numbersOfBufferToProduce,
                                                  uint64_t batchSize) {
    return std::make_shared<KafkaSourceDescriptor>(KafkaSourceDescriptor(std::move(schema),
                                                                         std::move(logicalSourceName),
                                                                         std::move(brokers),
                                                                         std::move(topic),
                                                                         std::move(groupId),
                                                                         autoCommit,
                                                                         kafkaConnectTimeout,
                                                                         offsetMode,
                                                                         kafkaSourceType,
                                                                         numbersOfBufferToProduce,
                                                                         batchSize));
}

SourceDescriptorPtr KafkaSourceDescriptor::create(SchemaPtr schema,
                                                  std::string brokers,
                                                  std::string topic,
                                                  std::string groupId,
                                                  bool autoCommit,
                                                  uint64_t kafkaConnectTimeout,
                                                  const std::string& offsetMode,
                                                  const KafkaSourceTypePtr& kafkaSourceType,
                                                  uint64_t numbersOfBufferToProduce,
                                                  uint64_t batchSize) {
    return std::make_shared<KafkaSourceDescriptor>(KafkaSourceDescriptor(std::move(schema),
                                                                         std::move(brokers),
                                                                         std::move(topic),
                                                                         std::move(groupId),
                                                                         autoCommit,
                                                                         kafkaConnectTimeout,
                                                                         offsetMode,
                                                                         kafkaSourceType,
                                                                         numbersOfBufferToProduce,
                                                                         batchSize));
}

SourceDescriptorPtr KafkaSourceDescriptor::create(SchemaPtr schema, KafkaSourceTypePtr sourceConfig) {
    return std::make_shared<KafkaSourceDescriptor>(KafkaSourceDescriptor(std::move(schema), std::move(sourceConfig)));
}

const std::string& KafkaSourceDescriptor::getBrokers() const { return brokers; }

const std::string& KafkaSourceDescriptor::getTopic() const { return topic; }

const std::string& KafkaSourceDescriptor::getOffsetMode() const { return offsetMode; }

std::uint64_t KafkaSourceDescriptor::getNumberOfToProcessBuffers() const { return numbersOfBufferToProduce; }

std::uint64_t KafkaSourceDescriptor::getBatchSize() const { return batchSize; }

bool KafkaSourceDescriptor::isAutoCommit() const { return autoCommit; }

uint64_t KafkaSourceDescriptor::getKafkaConnectTimeout() const { return kafkaConnectTimeout; }

KafkaSourceTypePtr KafkaSourceDescriptor::getSourceConfigPtr() const { return kafkaSourceType; }

const std::string& KafkaSourceDescriptor::getGroupId() const { return groupId; }

bool KafkaSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<KafkaSourceDescriptor>()) {
        return false;
    }
    auto otherKafkaSource = other->as<KafkaSourceDescriptor>();
    return brokers == otherKafkaSource->getBrokers() && topic == otherKafkaSource->getTopic()
        && kafkaConnectTimeout == otherKafkaSource->getKafkaConnectTimeout() && groupId == otherKafkaSource->getGroupId()
        && getSchema()->equals(other->getSchema()) && kafkaSourceType->equal(otherKafkaSource->getSourceConfigPtr());
}

std::string KafkaSourceDescriptor::toString() const { return "KafkaSourceDescriptor()"; }

SourceDescriptorPtr KafkaSourceDescriptor::copy() {
    auto copy = KafkaSourceDescriptor::create(schema->copy(),
                                              std::move(logicalSourceName),
                                              std::move(brokers),
                                              std::move(topic),
                                              std::move(groupId),
                                              autoCommit,
                                              kafkaConnectTimeout,
                                              offsetMode,
                                              kafkaSourceType,
                                              numbersOfBufferToProduce,
                                              batchSize);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

}// namespace NES
