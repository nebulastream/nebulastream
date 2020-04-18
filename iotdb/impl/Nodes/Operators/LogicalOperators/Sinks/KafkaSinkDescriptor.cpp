#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>

namespace NES {

KafkaSinkDescriptor::KafkaSinkDescriptor(SchemaPtr schema, std::string brokers,
                                         std::string topic,
                                         uint64_t kafkaProducerTimeout,
                                         cppkafka::Configuration config)
    : SinkDescriptor(schema), brokers(brokers), topic(topic), kafkaProducerTimeout(kafkaProducerTimeout), config(config) {}

SinkDescriptorType KafkaSinkDescriptor::getType() {
    return KafkaSinkDescriptorType;
}

const std::string& KafkaSinkDescriptor::getBrokers() const {
    return brokers;
}

const std::string& KafkaSinkDescriptor::getTopic() const {
    return topic;
}

const cppkafka::Configuration& KafkaSinkDescriptor::getConfig() const {
    return config;
}

const uint64_t KafkaSinkDescriptor::getKafkaProducerTimeout() const {
    return kafkaProducerTimeout;
}

}
