#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <cppkafka/configuration.h>

namespace NES {

class KafkaSourceDescriptor : public SourceDescriptor {

  public:

    KafkaSourceDescriptor(SchemaPtr schema,
                          std::string brokers,
                          std::string topic,
                          bool autoCommit,
                          cppkafka::Configuration config,
                          uint64_t kafkaConnectTimeout);

    SourceDescriptorType getType() override;

    const std::string& getBrokers() const;
    const std::string& getTopic() const;
    bool isAutoCommit() const;
    const cppkafka::Configuration& getConfig() const;
    uint64_t getKafkaConnectTimeout() const;

  private:

    std::string brokers;
    std::string topic;
    bool autoCommit;
    cppkafka::Configuration config;
    uint64_t kafkaConnectTimeout;

};

typedef std::shared_ptr<KafkaSourceDescriptor> KafkaSourceDescriptorPtr;

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_
