#ifndef KAFKASOURCE_HPP
#define KAFKASOURCE_HPP

#include <cstdint>
#include <memory>
#include <string>

#include <cppkafka/cppkafka.h>
#include <SourceSink/DataSource.hpp>

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
} // namespace NES

#endif // KAFKASOURCE_HPP
