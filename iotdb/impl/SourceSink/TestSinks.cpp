#include <SourceSink/FileOutputSink.hpp>
#include <SourceSink/PrintSink.hpp>
#include <SourceSink/ZmqSink.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/KafkaSink.hpp>
#include <Util/Logger.hpp>

namespace NES {

const DataSinkPtr createTestSink() {
    NES_ERROR("Called unimplemented Function");
    NES_NOT_IMPLEMENTED
}

const DataSinkPtr createBinaryFileSinkWithSchema(SchemaPtr schema,
                                                 const std::string& filePath) {
    return std::make_shared<FileOutputSink>(schema, filePath);
}

const DataSinkPtr createBinaryFileSinkWithoutSchema(const std::string& filePath) {
    return std::make_shared<FileOutputSink>(filePath);
}

const DataSinkPtr createCSVFileSinkWithSchemaAppend(SchemaPtr schema,
                                                    const std::string& filePath) {
    return std::make_shared<FileOutputSink>(schema, filePath, CSV_TYPE, FILE_APPEND);
}

const DataSinkPtr createCSVFileSinkWithSchemaOverwrite(SchemaPtr schema,
                                                       const std::string& filePath) {
    return std::make_shared<FileOutputSink>(schema, filePath, CSV_TYPE, FILE_OVERWRITE);
}

const DataSinkPtr createPrintSinkWithoutSchema(std::ostream& out) {
    return std::make_shared<PrintSink>(out);
}

const DataSinkPtr createPrintSinkWithSchema(SchemaPtr schema, std::ostream& out) {
    return std::make_shared<PrintSink>(schema, out);
}

const DataSinkPtr createZmqSink(SchemaPtr schema, const std::string& host,
                                const uint16_t port) {
    return std::make_shared<ZmqSink>(schema, host, port);
}

const DataSinkPtr createKafkaSinkWithSchema(SchemaPtr schema, const std::string& brokers, const std::string& topic,
                                            const size_t kafkaProducerTimeout) {
    return std::make_shared<KafkaSink>(schema, brokers, topic, kafkaProducerTimeout);
}

}
