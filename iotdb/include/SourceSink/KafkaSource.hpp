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
  KafkaSource();
  KafkaSource(SchemaPtr schema,
              const std::string& brokers,
              const std::string& topic,
              const std::string& groupId="nes",
              const size_t kafkaConsumerTimeout=100);

  KafkaSource(SchemaPtr schema,
              const std::string& topic,
              const cppkafka::Configuration& config,
              const size_t kafkaConsumerTimeout=100);
    SourceType getType() const override;
    ~KafkaSource() override;

  TupleBufferPtr receiveData() override;

  /**
   * @brief override the toString method for the kafka source
   * @return returns string describing the kafka source
   */
  const std::string toString() const override;

 private:
  void _connect();
  friend class boost::serialization::access;
  template <class Archive> void serialize(Archive& ar,
                                          const unsigned int)
  {
    ar& boost::serialization::base_object<DataSource>(*this);
    ar& brokers;
    ar& topic;
    ar& groupId;
  }

  std::string brokers;
  std::string topic;
  std::string groupId;
  bool autoCommit;

  cppkafka::Configuration config;

  std::unique_ptr<cppkafka::Consumer> consumer;

  std::chrono::milliseconds kafkaConsumerTimeout;
};
} // namespace NES

#endif // KAFKASOURCE_HPP
