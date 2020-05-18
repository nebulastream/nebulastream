#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>

namespace NES {

KafkaSinkDescriptor::KafkaSinkDescriptor(std::string topic, std::string brokers, uint64_t timeout)
    : topic(topic), brokers(brokers), timeout(timeout) {}

const std::string& KafkaSinkDescriptor::getTopic() const {
    return topic;
}

const std::string& KafkaSinkDescriptor::getBrokers() const {
    return brokers;
}

uint64_t KafkaSinkDescriptor::getTimeout() const {
    return timeout;
}
SinkDescriptorPtr KafkaSinkDescriptor::create(std::string topic,
                                              std::string brokers,
                                              uint64_t timeout) {
    return std::make_shared<KafkaSinkDescriptor>(KafkaSinkDescriptor(topic, brokers, timeout));
}

}// namespace NES
