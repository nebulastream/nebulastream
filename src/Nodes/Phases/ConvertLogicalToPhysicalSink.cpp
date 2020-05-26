#include <Nodes/Operators/LogicalOperators/Sinks/CsvSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSinkPtr ConvertLogicalToPhysicalSink::createDataSink(SchemaPtr schema, SinkDescriptorPtr sinkDescriptor) {

    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating print sink");
        const PrintSinkDescriptorPtr printSinkDescriptor = sinkDescriptor->as<PrintSinkDescriptor>();
        return createPrintSinkWithSchema(schema, std::cout);
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating ZMQ sink");
        const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        return createZmqSink(schema, zmqSinkDescriptor->getHost(), zmqSinkDescriptor->getPort());
#ifdef ENABLE_KAFKA_BUILD
    } else if (sinkDescriptor->instanceOf<KafkaSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Kafka sink");
        const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();
        return createKafkaSinkWithSchema(schema, kafkaSinkDescriptor->getBrokers(),
                                         kafkaSinkDescriptor->getTopic(),
                                         kafkaSinkDescriptor->getTimeout());
#endif
    } else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Binary file sink");
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        return createBinaryFileSinkWithSchema(schema, fileSinkDescriptor->getFileName());
    } else if (sinkDescriptor->instanceOf<CsvSinkDescriptor>()) {
        auto csvSinkDescriptor = sinkDescriptor->as<CsvSinkDescriptor>();
        if (csvSinkDescriptor->getFileOutputMode() == CsvSinkDescriptor::APPEND) {
            NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink in append mode");
            return createCSVFileSinkWithSchemaAppend(schema, csvSinkDescriptor->getFileName());
        } else if (csvSinkDescriptor->getFileOutputMode() == CsvSinkDescriptor::OVERWRITE) {
            NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink in Overwrite mode");
            return createCSVFileSinkWithSchemaOverwrite(schema, csvSinkDescriptor->getFileName());
        } else {
            NES_ERROR("ConvertLogicalToPhysicalSink: Unknown File Mode");
            throw std::invalid_argument("Unknown File Mode");
        }
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSink: Unknown Sink Descriptor Type");
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

}// namespace NES
