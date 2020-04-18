#include "Nodes/Phases/ConvertLogicalToPhysicalSink.hpp"
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

        case PrintSink: {
            const PrintSinkDescriptorPtr
                printSinkDescriptor = std::dynamic_pointer_cast<PrintSinkDescriptor>(sinkDescriptor);
            return createPrintSinkWithSchema(printSinkDescriptor->getSchema(), printSinkDescriptor->getOutputStream());
        }
        case ZmqSink: {
            const ZmqSinkDescriptorPtr zmqSinkDescriptor = std::dynamic_pointer_cast<ZmqSinkDescriptor>(sinkDescriptor);
            return createZmqSink(zmqSinkDescriptor->getSchema(),
                                 zmqSinkDescriptor->getHost(),
                                 zmqSinkDescriptor->getPort());
        }
        case KafkaSink: {
            const KafkaSinkDescriptorPtr
                kafkaSinkDescriptor = std::dynamic_pointer_cast<KafkaSinkDescriptor>(sinkDescriptor);
            return createKafkaSinkWithSchema(kafkaSinkDescriptor->getSchema(),
                                             kafkaSinkDescriptor->getTopic(),
                                             kafkaSinkDescriptor->getConfig());
        }
        case FileSink: {
            const FileSinkDescriptorPtr
                fileSinkDescriptor = std::dynamic_pointer_cast<FileSinkDescriptor>(sinkDescriptor);
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
