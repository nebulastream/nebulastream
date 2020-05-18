#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <cppkafka/cppkafka.h>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical kafka sink
 */
class KafkaSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method to create a new Kafka sink.
     * @param topic kafka topic name
     * @param brokers kafka broker list
     * @param timeout Kafka producer timeout
     * @return descriptor for kafka sink
     */
    static SinkDescriptorPtr create(std::string topic, std::string brokers, uint64_t timeout);

    /**
     * @brief Get Kafka topic where data is to be written
     */
    const std::string& getTopic() const;

    /**
     * @brief List of comma separated kafka brokers
     */
    const std::string& getBrokers() const;

    /**
     * @brief Kafka connection Timeout
     */
    uint64_t getTimeout() const;

  private:
    explicit KafkaSinkDescriptor(std::string topic, std::string brokers, uint64_t timeout);
    std::string topic;
    std::string brokers;
    uint64_t timeout;
};

typedef std::shared_ptr<KafkaSinkDescriptor> KafkaSinkDescriptorPtr;
}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
