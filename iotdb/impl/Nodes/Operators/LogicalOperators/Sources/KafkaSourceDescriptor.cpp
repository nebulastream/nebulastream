#include "Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp"

namespace NES {

KafkaSourceDescriptor::KafkaSourceDescriptor(SchemaPtr schema,
                                             std::string brokers,
                                             std::string topic,
                                             std::string groupId,
                                             bool autoCommit,
                                             cppkafka::Configuration config)
    : SourceDescriptor(schema),
      brokers(brokers),
      topic(topic),
      groupId(groupId),
      autoCommit(autoCommit),
      config(config) {}

SourceDescriptorType KafkaSourceDescriptor::getType() {
    return KafkaSinkDescriptor;
}

const std::string& KafkaSourceDescriptor::getBrokers() const {
    return brokers;
}

const std::string& KafkaSourceDescriptor::getTopic() const {
    return topic;
}

const std::string& KafkaSourceDescriptor::getGroupId() const {
    return groupId;
}

bool KafkaSourceDescriptor::isAutoCommit() const {
    return autoCommit;
}

const cppkafka::Configuration& KafkaSourceDescriptor::getConfig() const {
    return config;
}

}
