#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <cppkafka/configuration.h>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical kafka source
 */
class KafkaSourceDescriptor : public SourceDescriptor {

  public:

    KafkaSourceDescriptor(SchemaPtr schema,
                          std::string brokers,
                          std::string topic,
                          bool autoCommit,
                          cppkafka::Configuration config,
                          uint64_t kafkaConnectTimeout);

    SourceDescriptorType getType() override;

    /**
     * @brief Get the list of kafka brokers
     */
    const std::string& getBrokers() const;

    /**
     * @brief Get the kafka topic name
     */
    const std::string& getTopic() const;

    /**
     * @brief Is kafka topic offset to be auto committed
     */
    bool isAutoCommit() const;

    /**
     * @brief Kafka configurations for producer and consumer
     */
    const cppkafka::Configuration& getConfig() const;

    /**
     * @brief Get kafka connection timeout
     * @return
     */
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
