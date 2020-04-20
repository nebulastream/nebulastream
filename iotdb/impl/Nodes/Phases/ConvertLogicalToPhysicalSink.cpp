#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSinkPtr ConvertLogicalToPhysicalSink::createDataSink(SinkDescriptorPtr sinkDescriptor) {

    SinkDescriptorType sinkDescriptorType = sinkDescriptor->getType();

    switch (sinkDescriptorType) {

        case PrintSinkDescriptorType: {
            NES_INFO("ConvertLogicalToPhysicalSink: Creating print sink")
            const PrintSinkDescriptorPtr printSinkDescriptor = sinkDescriptor->as<PrintSinkDescriptor>();
            return createPrintSinkWithSchema(printSinkDescriptor->getSchema(), printSinkDescriptor->getOutputStream());
        }
        case ZmqSinkDescriptorType: {
            NES_INFO("ConvertLogicalToPhysicalSink: Creating ZMQ sink")
            const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
            return createZmqSink(zmqSinkDescriptor->getSchema(),
                                 zmqSinkDescriptor->getHost(),
                                 zmqSinkDescriptor->getPort());
        }
        case KafkaSinkDescriptorType: {
            NES_INFO("ConvertLogicalToPhysicalSink: Creating Kafka sink")
            const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();
            return createKafkaSinkWithSchema(kafkaSinkDescriptor->getSchema(),
                                             kafkaSinkDescriptor->getTopic(),
                                             kafkaSinkDescriptor->getConfig());
        }
        case FileSinkDescriptorType: {
            NES_INFO("ConvertLogicalToPhysicalSink: Creating File sink")
            const FileSinkDescriptorPtr fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
            FileOutPutType fileOutPutType = fileSinkDescriptor->getFileOutPutType();
            switch (fileOutPutType) {
                case BINARY_TYPE: {
                    NES_INFO("ConvertLogicalToPhysicalSink: Creating Binary file sink")
                    return createBinaryFileSinkWithSchema(fileSinkDescriptor->getSchema(),
                                                          fileSinkDescriptor->getFileName());
                }
                case CSV_TYPE:{
                    NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink")
                    if (fileSinkDescriptor->getFileOutPutMode() == FILE_APPEND) {
                        NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink in append mode")
                        return createCSVFileSinkWithSchemaAppend(fileSinkDescriptor->getSchema(),
                                                                 fileSinkDescriptor->getFileName());
                    } else if (fileSinkDescriptor->getFileOutPutMode() == FILE_OVERWRITE) {
                        NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink in Overwrite mode")
                        return createCSVFileSinkWithSchemaOverwrite(fileSinkDescriptor->getSchema(),
                                                                    fileSinkDescriptor->getFileName());
                    } else {
                        NES_ERROR("ConvertLogicalToPhysicalSink: Unknown File Mode")
                        throw std::invalid_argument("Unknown File Mode");
                    }
                }
            }
        }
        default: {
            NES_ERROR("ConvertLogicalToPhysicalSink: Unknown Sink Descriptor Type")
            throw std::invalid_argument("Unknown Sink Descriptor Type");
        }
    }
}

}
