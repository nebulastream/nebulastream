#include <sstream>
#include <string>
#include <NodeEngine/Dispatcher.hpp>
#include <SourceSink/KafkaSink.hpp>
#include <Util/Logger.hpp>

#include <chrono>
using namespace std::chrono_literals;

namespace NES {

KafkaSink::KafkaSink() {}

KafkaSink::KafkaSink(SchemaPtr schema,
                     const std::string& brokers,
                     const std::string& topic,
                     const size_t kafkaProducerTimeout)
    : DataSink(schema),
      brokers(brokers),
      topic(topic),
      kafkaProducerTimeout(std::move(std::chrono::milliseconds(kafkaProducerTimeout))) {

    config = {
        {"metadata.broker.list", brokers.c_str()}
    };

    _connect();
    NES_DEBUG("KAFKASINK  " << this << ": Init KAFKA SINK to brokers " << brokers
                            << ", topic " << topic << ", partition " << partition)
}

KafkaSink::~KafkaSink() {}

bool KafkaSink::writeData(TupleBuffer& input_buffer) {
    NES_DEBUG("KAFKASINK " << this << ": writes buffer " << input_buffer)
    try {
        cppkafka::Buffer buffer(input_buffer.getBuffer(),
                                input_buffer.getBufferSize());
        msgBuilder->payload(buffer);

        NES_DEBUG("KAFKASINK buffer to send " << buffer.get_size() << " bytes, content: " << msgBuilder->payload())
        producer->produce(*msgBuilder);
        producer->flush(kafkaProducerTimeout);
        NES_DEBUG("KAFKASINK " << this << ": send successfully")
    }
    catch (const cppkafka::HandleException& ex) {
        throw ex;
    }
    catch (...) {
        NES_ERROR("KAFKASINK Unknown error occurs")
        return false;
    }

    return true;
}

const std::string KafkaSink::toString() const {
  std::stringstream ss;
  ss << "KAFKA_SINK(";
  ss << "SCHEMA(" << schema->toString() << "), ";
  ss << "BROKER(" << brokers << "), ";
  ss << "TOPIC(" << topic << "), ";
  ss << "PARTITION(" << partition << ").";
  return ss.str();
}

void KafkaSink::setup() {
    // currently not required
}

void KafkaSink::shutdown() {
    // currently not required
}

void KafkaSink::_connect() {

    NES_DEBUG("KAFKASINK connecting...")
    producer = std::make_unique<cppkafka::Producer>(config);
    msgBuilder = std::make_unique<cppkafka::MessageBuilder>(topic);
    // FIXME: should we provide user to access partition ?
    // if (partition != INVALID_PARTITION_NUMBER) {
    // msgBuilder->partition(partition);
    // }
}

SinkType KafkaSink::getType() const {
    return KAFKA_SINK;
}

}  // namespace NES

BOOST_CLASS_EXPORT(NES::KafkaSink);
