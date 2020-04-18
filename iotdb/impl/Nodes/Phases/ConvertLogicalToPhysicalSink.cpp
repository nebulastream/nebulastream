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
            const PrintSinkDescriptorPtr printSinkDescriptor = sinkDescriptor->as<PrintSinkDescriptor>();
            return createPrintSinkWithSchema(printSinkDescriptor->getSchema(), printSinkDescriptor->getOutputStream());
        }
        case ZmqSinkDescriptorType: {
            const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
            return createZmqSink(zmqSinkDescriptor->getSchema(),
                                 zmqSinkDescriptor->getHost(),
                                 zmqSinkDescriptor->getPort());
        }
        case KafkaSinkDescriptorType: {
            const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();
            return createKafkaSinkWithSchema(kafkaSinkDescriptor->getSchema(),
                                             kafkaSinkDescriptor->getTopic(),
                                             kafkaSinkDescriptor->getConfig());
        }
        case FileSinkDescriptorType: {
            const FileSinkDescriptorPtr fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
            FileOutPutType fileOutPutType = fileSinkDescriptor->getFileOutPutType();
            switch (fileOutPutType) {
                case BINARY_TYPE:
                    return createBinaryFileSinkWithSchema(fileSinkDescriptor->getSchema(),
                                                          fileSinkDescriptor->getFileName());
                case CSV_TYPE:
                    if (fileSinkDescriptor->getFileOutPutMode() == FILE_APPEND) {
                        return createCSVFileSinkWithSchemaAppend(fileSinkDescriptor->getSchema(),
                                                                 fileSinkDescriptor->getFileName());
                    } else if (fileSinkDescriptor->getFileOutPutMode() == FILE_OVERWRITE) {
                        return createCSVFileSinkWithSchemaOverwrite(fileSinkDescriptor->getSchema(),
                                                                    fileSinkDescriptor->getFileName());
                    } else {
                        NES_ERROR("Unknown File Mode")
                        throw std::invalid_argument("Unknown File Mode");
                    }
            }
        }
        default: {
            NES_ERROR("Unknown Sink Descriptor Type")
            throw std::invalid_argument("Unknown Sink Descriptor Type");
        }
    }
}

}
