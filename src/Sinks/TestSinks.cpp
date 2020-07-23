#include <Network/NetworkSink.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Formats/TextFormat.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

const DataSinkPtr createTestSink() {
    NES_ERROR("Called unimplemented Function");
    NES_NOT_IMPLEMENTED();
}

const DataSinkPtr createTextFileSinkWithSchema(SchemaPtr schema,
                                               const std::string& filePath, BufferManagerPtr bufferManager, bool append) {
    //TODO: this is not nice and should be fixed such that we only provide the paramter once
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, bufferManager);
    return std::make_shared<FileSink>(schema, format, filePath, append);
}

const DataSinkPtr createCSVFileSinkWithSchema(SchemaPtr schema,
                                              const std::string& filePath, BufferManagerPtr bufferManager, bool append) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, bufferManager);
    return std::make_shared<FileSink>(schema, format, filePath, append);
}

const DataSinkPtr createBinaryNESFileSinkWithSchema(SchemaPtr schema,
                                                    const std::string& filePath, BufferManagerPtr bufferManager, bool append) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, bufferManager);
    return std::make_shared<FileSink>(schema, format, filePath, append);
}

const DataSinkPtr createJSONFileSinkWithSchema(SchemaPtr schema,
                                               const std::string& filePath, BufferManagerPtr bufferManager, bool append) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, bufferManager);
    return std::make_shared<FileSink>(schema, format, filePath, append);
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
