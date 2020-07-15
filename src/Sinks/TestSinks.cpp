#include <Network/NetworkSink.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/DataSink.hpp>
#include <Sinks/KafkaSink.hpp>
#include <Sinks/PrintSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sinks/ZmqSink.hpp>
#include <Util/Logger.hpp>

namespace NES {

const DataSinkPtr createTestSink() {
    NES_ERROR("Called unimplemented Function");
    NES_NOT_IMPLEMENTED();
}

const DataSinkPtr createTextFileSinkWithSchema(SchemaPtr schema,
                                                 const std::string& filePath, FileOutputMode fMode) {
    return std::make_shared<FileSink>(schema, TEXT_FORMAT, filePath, fMode);
}

const DataSinkPtr createCSVFileSinkWithSchema(SchemaPtr schema,
                                                    const std::string& filePath, FileOutputMode fMode) {
    return std::make_shared<FileSink>(schema, CSV_FORMAT, filePath, fMode);
}

const DataSinkPtr createBinaryNESFileSinkWithSchema(SchemaPtr schema,
                                              const std::string& filePath, FileOutputMode fMode) {
    return std::make_shared<FileSink>(schema, NES_FORMAT, filePath, fMode);
}

const DataSinkPtr createJSONFileSinkWithSchema(SchemaPtr schema,
                                               const std::string& filePath, FileOutputMode fMode) {
    return std::make_shared<FileSink>(schema, JSON_FORMAT, filePath, fMode);
}

const DataSinkPtr createZmqSink(SchemaPtr schema, const std::string& host,
                                const uint16_t port) {
    return std::make_shared<ZmqSink>(schema, host, port);
}

const DataSinkPtr createPrintSinkWithSchema(SchemaPtr schema, std::ostream& out) {
    return std::make_shared<PrintSink>(schema, out);
}

const DataSinkPtr createNetworkSink(SchemaPtr schema, Network::NetworkManagerPtr networkManager, Network::NodeLocation nodeLocation,
                                    Network::NesPartition nesPartition, std::chrono::seconds waitTime, uint8_t retryTimes) {
    return std::make_shared<Network::NetworkSink>(schema, networkManager, nodeLocation, nesPartition, waitTime, retryTimes);
}

#ifdef ENABLE_KAFKA_BUILD
const DataSinkPtr createKafkaSinkWithSchema(SchemaPtr schema, const std::string& brokers, const std::string& topic,
                                            const size_t kafkaProducerTimeout) {
    return std::make_shared<KafkaSink>(schema, brokers, topic, kafkaProducerTimeout);
}
#endif
}// namespace NES
