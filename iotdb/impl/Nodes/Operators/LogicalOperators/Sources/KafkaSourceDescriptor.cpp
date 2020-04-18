#include <Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>

namespace NES {

KafkaSourceDescriptor::KafkaSourceDescriptor(SchemaPtr schema,
                                             std::string brokers,
                                             std::string topic,
                                             bool autoCommit,
                                             cppkafka::Configuration config,
                                             uint64_t kafkaConnectTimeout)
    : SourceDescriptor(schema),
      brokers(brokers),
      topic(topic),
      autoCommit(autoCommit),
      config(config),
      kafkaConnectTimeout(kafkaConnectTimeout) {}

SourceDescriptorType KafkaSourceDescriptor::getType() {
    return KafkaSource;
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

const cppkafka::Configuration& KafkaSourceDescriptor::getConfig() const {
    return config;
}

uint64_t KafkaSourceDescriptor::getKafkaConnectTimeout() const {
    return kafkaConnectTimeout;
}

}
