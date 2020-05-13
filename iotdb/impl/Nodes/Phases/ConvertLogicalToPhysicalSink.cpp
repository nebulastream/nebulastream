#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSinkPtr ConvertLogicalToPhysicalSink::createDataSink(SinkDescriptorPtr sinkDescriptor) {

    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating print sink");
        const PrintSinkDescriptorPtr printSinkDescriptor = sinkDescriptor->as<PrintSinkDescriptor>();
        return createPrintSinkWithSchema(printSinkDescriptor->getSchema(), std::cout);
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating ZMQ sink");
        const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        return createZmqSink(zmqSinkDescriptor->getSchema(),
                             zmqSinkDescriptor->getHost(),
                             zmqSinkDescriptor->getPort());
    } else if (sinkDescriptor->instanceOf<KafkaSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Kafka sink");
        const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();
        return createKafkaSinkWithSchema(kafkaSinkDescriptor->getSchema(),
                                         kafkaSinkDescriptor->getBrokers(),
                                         kafkaSinkDescriptor->getTopic(),
                                         kafkaSinkDescriptor->getTimeout());
    } else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating File sink");
        const FileSinkDescriptorPtr fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        FileOutputType fileOutPutType = fileSinkDescriptor->getFileOutputType();
        switch (fileOutPutType) {
            case BINARY_TYPE: {
                NES_INFO("ConvertLogicalToPhysicalSink: Creating Binary file sink");
                return createBinaryFileSinkWithSchema(fileSinkDescriptor->getSchema(),
                                                      fileSinkDescriptor->getFileName());
            }
            case CSV_TYPE: {
                NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink");
                if (fileSinkDescriptor->getFileOutputMode() == FILE_APPEND) {
                    NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink in append mode");
                    return createCSVFileSinkWithSchemaAppend(fileSinkDescriptor->getSchema(),
                                                             fileSinkDescriptor->getFileName());
                } else if (fileSinkDescriptor->getFileOutputMode() == FILE_OVERWRITE) {
                    NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink in Overwrite mode");
                    return createCSVFileSinkWithSchemaOverwrite(fileSinkDescriptor->getSchema(),
                                                                fileSinkDescriptor->getFileName());
                } else {
                    NES_ERROR("ConvertLogicalToPhysicalSink: Unknown File Mode");
                    throw std::invalid_argument("Unknown File Mode");
                }
            }
        }
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSink: Unknown Sink Descriptor Type");
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

}// namespace NES
