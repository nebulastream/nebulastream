#include <Nodes/Phases/ConvertPhysicalToLogicalSink.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <SourceSink/DataSink.hpp>
#include <Util/Logger.hpp>

namespace NES {

SinkDescriptorPtr ConvertPhysicalToLogicalSink::createSinkDescriptor(DataSinkPtr dataSink) {

    SinkType sinkType = dataSink->getType();

    switch (sinkType) {

        case PRINT_SINK: {
            NES_INFO("ConvertPhysicalToLogicalSink: Creating print sink");
            const SchemaPtr schema = dataSink->getSchema();
            return PrintSinkDescriptor::create(schema);
        }
        case ZmqSinkDescriptorType: {
            NES_INFO("ConvertPhysicalToLogicalSink: Creating ZMQ sink");
            const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
            return createZmqSink(zmqSinkDescriptor->getSchema(),
                                 zmqSinkDescriptor->getHost(),
                                 zmqSinkDescriptor->getPort());
        }
        case KafkaSinkDescriptorType: {
            NES_INFO("ConvertPhysicalToLogicalSink: Creating Kafka sink");
            const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();
            return createKafkaSinkWithSchema(kafkaSinkDescriptor->getSchema(),
                                             kafkaSinkDescriptor->getBrokers(),
                                             kafkaSinkDescriptor->getTopic(),
                                             kafkaSinkDescriptor->getTimeout());
        }
        case FileSinkDescriptorType: {
            NES_INFO("ConvertPhysicalToLogicalSink: Creating File sink");
            const FileSinkDescriptorPtr fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
            FileOutputType fileOutPutType = fileSinkDescriptor->getFileOutputType();
            switch (fileOutPutType) {
                case BINARY_TYPE: {
                    NES_INFO("ConvertPhysicalToLogicalSink: Creating Binary file sink");
                    return createBinaryFileSinkWithSchema(fileSinkDescriptor->getSchema(),
                                                          fileSinkDescriptor->getFileName());
                }
                case CSV_TYPE: {
                    NES_INFO("ConvertPhysicalToLogicalSink: Creating CSV File sink");
                    if (fileSinkDescriptor->getFileOutputMode() == FILE_APPEND) {
                        NES_INFO("ConvertLogicalToPhysicalSink: Creating CSV File sink in append mode");
                        return createCSVFileSinkWithSchemaAppend(fileSinkDescriptor->getSchema(),
                                                                 fileSinkDescriptor->getFileName());
                    } else if (fileSinkDescriptor->getFileOutputMode() == FILE_OVERWRITE) {
                        NES_INFO("ConvertPhysicalToLogicalSink: Creating CSV File sink in Overwrite mode");
                        return createCSVFileSinkWithSchemaOverwrite(fileSinkDescriptor->getSchema(),
                                                                    fileSinkDescriptor->getFileName());
                    } else {
                        NES_ERROR("ConvertPhysicalToLogicalSink: Unknown File Mode");
                        throw std::invalid_argument("Unknown File Mode");
                    }
                }
            }
        }
        default: {
            NES_ERROR("ConvertPhysicalToLogicalSink: Unknown Data Sink Type");
            throw std::invalid_argument("Unknown DataSink Type");
        }
    }
}

}// namespace NES
