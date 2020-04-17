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
                          std::string groupId,
                          bool autoCommit,
                          cppkafka::Configuration config);

    SourceDescriptorType getType() override;

    const std::string& getBrokers() const;
    const std::string& getTopic() const;
    const std::string& getGroupId() const;
    bool isAutoCommit() const;
    const cppkafka::Configuration& getConfig() const;

  private:

    std::string brokers;
    std::string topic;
    std::string groupId;
    bool autoCommit;
    cppkafka::Configuration config;

};

typedef std::shared_ptr<KafkaSourceDescriptor> KafkaSourceDescriptorPtr;

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_
