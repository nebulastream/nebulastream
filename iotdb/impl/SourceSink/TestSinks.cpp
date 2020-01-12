#include <SourceSink/FileOutputSink.hpp>
#include <SourceSink/PrintSink.hpp>
#include <SourceSink/ZmqSink.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/KafkaSink.hpp>
#include <Util/Logger.hpp>
namespace NES {

const DataSinkPtr createTestSink() {
  IOTDB_ERROR("Called unimplemented Function");
  IOTDB_NOT_IMPLEMENTED
}


const DataSinkPtr createBinaryFileSinkWithoutSchema(const std::string& filePath) {
  return std::make_shared<FileOutputSink>(filePath);
}

const DataSinkPtr createBinaryFileSinkWithSchema(const Schema& schema,
                                                 const std::string& filePath) {
  return std::make_shared<FileOutputSink>(schema , filePath);
}

const DataSinkPtr createPrintSinkWithoutSchema(std::ostream& out) {
  return std::make_shared<PrintSink>(out);
}

const DataSinkPtr createPrintSinkWithSchema(const Schema& schema, std::ostream& out) {
  return std::make_shared<PrintSink>(schema, out);
}

const DataSinkPtr createZmqSink(const Schema& schema, const std::string& host,
                                const uint16_t port) {
  return std::make_shared<ZmqSink>(schema, host, port);
}

const DataSinkPtr createKafkaSinkWithSchema(const Schema& schema, const std::string& brokers, const std::string& topic,
                                            const size_t kafkaProducerTimeout) {
    return std::make_shared<KafkaSink>(schema, brokers, topic, kafkaProducerTimeout);
}

const DataSinkPtr createKafkaSinkWithSchema(const Schema& schema, const std::string& topic,
                                            const cppkafka::Configuration& config) {
    return std::make_shared<KafkaSink>(schema, topic, config);
}

}
