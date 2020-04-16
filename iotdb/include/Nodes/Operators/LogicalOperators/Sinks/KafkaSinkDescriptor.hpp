#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <cppkafka/cppkafka.h>

namespace NES {

class KafkaSinkDescriptor: public SinkDescriptor {

  public:

    KafkaSinkDescriptor(std::string brokers, std::string topic, int partition, cppkafka::Configuration config);

    SinkDescriptorType getType() override;

    const std::string& getBrokers() const;
    const std::string& getTopic() const;
    int getPartition() const;
    const cppkafka::Configuration& getConfig() const;

  private:

    KafkaSinkDescriptor() = default;

    std::string brokers;
    std::string topic;
    int partition;
    cppkafka::Configuration config;


};

typedef std::shared_ptr<KafkaSinkDescriptor> KafkaSinkDescriptorPtr ;
}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
