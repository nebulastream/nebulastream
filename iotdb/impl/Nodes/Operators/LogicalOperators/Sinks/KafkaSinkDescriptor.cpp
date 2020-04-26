#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>

namespace NES {

KafkaSinkDescriptor::KafkaSinkDescriptor(SchemaPtr schema, std::string topic, std::string brokers, uint64_t timeout)
    : SinkDescriptor(schema), topic(topic), brokers(brokers), timeout(timeout) {}

SinkDescriptorType KafkaSinkDescriptor::getType() {
    return KafkaSinkDescriptorType;
}

const std::string& KafkaSinkDescriptor::getTopic() const {
    return topic;
}

const std::string& KafkaSinkDescriptor::getBrokers() const {
    return brokers;
}

uint64_t KafkaSinkDescriptor::getTimeout() const {
    return timeout;
}
SinkDescriptorPtr KafkaSinkDescriptor::create(SchemaPtr schema,
                                              std::string topic,
                                              std::string brokers,
                                              uint64_t timeout) {
    return std::make_shared<KafkaSinkDescriptor>(KafkaSinkDescriptor(schema, topic, brokers, timeout));
}

}
