#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <cppkafka/cppkafka.h>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical kafka sink
 */
class KafkaSinkDescriptor: public SinkDescriptor {

  public:

    KafkaSinkDescriptor(SchemaPtr schema, std::string brokers, std::string topic, uint64_t  kafkaProducerTimeout, cppkafka::Configuration config);

    SinkDescriptorType getType() override;

    /**
     * @brief Get comma separated list of kafka brokers to connect to
     */
    const std::string& getBrokers() const;

    /**
     * @brief Get Kafka topic where data is to be written
     */
    const std::string& getTopic() const;

    /**
     * @brief get the producer timeout in milliseconds
     */
    const uint64_t getKafkaProducerTimeout() const;

    /**
     * @brief Get the kafka configuration used by kafka producer
     */
    const cppkafka::Configuration& getConfig() const;

  private:

    KafkaSinkDescriptor() = default;

    std::string brokers;
    std::string topic;
    uint64_t kafkaProducerTimeout;
    cppkafka::Configuration config;


};

typedef std::shared_ptr<KafkaSinkDescriptor> KafkaSinkDescriptorPtr ;
}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
