#ifndef KAFKASOURCE_HPP
#define KAFKASOURCE_HPP

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

    SourceType getType() const override;
    ~KafkaSource() override;
    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the kafka source
     * @return returns string describing the kafka source
     */
    const std::string toString() const override;

    const std::string& getBrokers() const;
    const std::string& getTopic() const;
    const std::string& getGroupId() const;
    bool isAutoCommit() const;
    const cppkafka::Configuration& getConfig() const;
    const std::chrono::milliseconds& getKafkaConsumerTimeout() const;
    const std::unique_ptr<cppkafka::Consumer>& getConsumer() const;

  private:
    KafkaSource() = default;

    void _connect();
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar,
                   const unsigned int) {
        ar& boost::serialization::base_object<DataSource>(*this);
        ar& brokers;
        ar& topic;
        ar& groupId;
        ar& autoCommit;
    }

    std::string brokers;
    std::string topic;
    std::string groupId;
    bool autoCommit;
    cppkafka::Configuration config;
    std::chrono::milliseconds kafkaConsumerTimeout;
    std::unique_ptr<cppkafka::Consumer> consumer;
};

typedef std::shared_ptr<KafkaSource> KafkaSourcePtr;
} // namespace NES

#endif// KAFKASOURCE_HPP
