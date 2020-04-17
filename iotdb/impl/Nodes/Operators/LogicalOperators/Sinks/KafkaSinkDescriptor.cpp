#include "Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp"

namespace NES {

SinkDescriptorType KafkaSinkDescriptor::getType() {
    return SinkDescriptorType::KafkaDescriptor;
}

KafkaSinkDescriptor::KafkaSinkDescriptor(std::string brokers,
                                         std::string topic,
                                         int partition,
                                         cppkafka::Configuration config)
    : brokers(brokers), topic(topic), partition(partition), config(config) {}

const std::string& KafkaSinkDescriptor::getBrokers() const {
    return brokers;
}

const std::string& KafkaSinkDescriptor::getTopic() const {
    return topic;
}

int KafkaSinkDescriptor::getPartition() const {
    return partition;
}

const cppkafka::Configuration& KafkaSinkDescriptor::getConfig() const {
    return config;
}

}
