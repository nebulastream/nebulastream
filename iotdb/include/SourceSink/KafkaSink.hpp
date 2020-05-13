#ifndef KAFKASINK_HPP
#define KAFKASINK_HPP

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include <SourceSink/DataSink.hpp>
#include <cppkafka/cppkafka.h>

namespace NES {

class KafkaSink : public DataSink {
    constexpr static size_t INVALID_PARTITION_NUMBER = -1;

  public:
    KafkaSink();
    KafkaSink(SchemaPtr schema,
              const std::string& brokers,
              const std::string& topic,
              const size_t kafkaProducerTimeout = 10 * 1000);

    ~KafkaSink() override;

    /**
     * @brief Get sink type
     */
    SinkType getType() const override;
    bool writeData(TupleBuffer& input_buffer);
    void setup() override;
    void shutdown() override;

    /**
     * @brief Get broker list
     */
    const std::string getBrokers() const;

    /**
     * @brief Get kafka topic name
     */
    const std::string getTopic() const;

    /**
     * @brief Get kafka producer timeout
     */
    const uint64_t getKafkaProducerTimeout() const;
    const std::string toString() const override;

  private:
    void _connect();
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& boost::serialization::base_object<DataSink>(*this);
        ar& brokers;
        ar& topic;
        ar& partition;
    }

    std::string brokers;
    std::string topic;
    int partition;

    cppkafka::Configuration config;

    std::unique_ptr<cppkafka::Producer> producer;
    std::unique_ptr<cppkafka::MessageBuilder> msgBuilder;

    std::chrono::milliseconds kafkaProducerTimeout;
};
typedef std::shared_ptr<KafkaSink> KafkaSinkPtr;
}// namespace NES

#endif// KAFKASINK_HPP
