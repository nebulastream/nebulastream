#ifndef KAFKASINK_HPP
#define KAFKASINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <chrono>

#include <cppkafka/cppkafka.h>
#include <SourceSink/DataSink.hpp>

namespace NES {

class KafkaSink : public DataSink {
 constexpr static size_t INVALID_PARTITION_NUMBER = -1;
 public:
  KafkaSink();
  KafkaSink(SchemaPtr schema,
            const std::string& brokers,
            const std::string& topic,
            const size_t kafkaProducerTimeout=10*1000);
  KafkaSink(SchemaPtr schema,
            const std::string& topic,
            const cppkafka::Configuration& config);
  ~KafkaSink() override;
    SinkType getType() const override;
    bool writeData(TupleBufferPtr input_buffer);
  void setup() override;
  void shutdown() override;

  const std::string toString() const override;

 private:
  void _connect();
  friend class boost::serialization::access;
  template <class Archive> void serialize(Archive& ar, const unsigned int version)
  {
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
} // namespace NES

#endif // KAFKASINK_HPP
