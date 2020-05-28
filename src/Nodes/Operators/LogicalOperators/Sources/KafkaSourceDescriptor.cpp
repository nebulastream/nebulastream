#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>

namespace NES {

KafkaSourceDescriptor::KafkaSourceDescriptor(SchemaPtr schema,
                                             std::string brokers,
                                             std::string topic,
                                             std::string groupId,
                                             bool autoCommit,
                                             uint64_t kafkaConnectTimeout)
    : SourceDescriptor(schema),
      brokers(brokers),
      topic(topic),
      groupId(groupId),
      autoCommit(autoCommit),
      kafkaConnectTimeout(kafkaConnectTimeout) {}

SourceDescriptorPtr KafkaSourceDescriptor::create(SchemaPtr schema,
                                                  std::string brokers,
                                                  std::string topic,
                                                  std::string groupId,
                                                  bool autoCommit,
                                                  uint64_t kafkaConnectTimeout) {
    return std::make_shared<KafkaSourceDescriptor>(KafkaSourceDescriptor(schema,
                                                                         brokers,
                                                                         topic,
                                                                         groupId,
                                                                         autoCommit,
                                                                         kafkaConnectTimeout));
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
