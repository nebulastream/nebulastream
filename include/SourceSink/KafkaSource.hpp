#ifndef KAFKASOURCE_HPP
#define KAFKASOURCE_HPP
#ifdef ENABLE_KAFKA_BUILD
#include <cstdint>
#include <memory>
#include <string>

#include <SourceSink/DataSource.hpp>
#include <cppkafka/cppkafka.h>

namespace NES {

class KafkaSource : public DataSource {

  public:
    KafkaSource(SchemaPtr schema,
                BufferManagerPtr bufferManager,
                QueryManagerPtr queryManager,
                std::string brokers,
                std::string topic,
                std::string groupId,
                bool autoCommit,
                uint64_t kafkaConsumerTimeout);

    /**
     * @brief Get source type
     */
    SourceType getType() const override;
    ~KafkaSource() override;
    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the kafka source
     * @return returns string describing the kafka source
     */
    const std::string toString() const override;

    /**
     * @brief Get kafka brokers
     */
    const std::string getBrokers() const;

    /**
     * @brief Get kafka topic
     */
    const std::string getTopic() const;

    /**
     * @brief Get kafka group id
     */
    const std::string getGroupId() const;

    /**
     * @brief If kafka offset is to be committed automatically
     */
    bool isAutoCommit() const;

    /**
     * @brief Get kafka configuration
     */
    const cppkafka::Configuration& getConfig() const;

    /**
     * @brief Get kafka connection timeout
     */
    const std::chrono::milliseconds& getKafkaConsumerTimeout() const;
    const std::unique_ptr<cppkafka::Consumer>& getConsumer() const;

  private:
    KafkaSource() = default;

    void _connect();

    std::string brokers;
    std::string topic;
    std::string groupId;
    bool autoCommit;
    cppkafka::Configuration config;
    std::chrono::milliseconds kafkaConsumerTimeout;
    std::unique_ptr<cppkafka::Consumer> consumer;
};

typedef std::shared_ptr<KafkaSource> KafkaSourcePtr;
}// namespace NES
#endif
#endif// KAFKASOURCE_HPP
