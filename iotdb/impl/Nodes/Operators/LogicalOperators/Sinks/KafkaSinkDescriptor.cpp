#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>

namespace NES {

KafkaSinkDescriptor::KafkaSinkDescriptor(SchemaPtr schema, std::string topic, cppkafka::Configuration config)
    : SinkDescriptor(schema), topic(topic), config(config) {}

SinkDescriptorType KafkaSinkDescriptor::getType() {
    return KafkaSinkDescriptorType;
}

const std::string& KafkaSinkDescriptor::getTopic() const {
    return topic;
}

const cppkafka::Configuration& KafkaSinkDescriptor::getConfig() const {
    return config;
}

}
