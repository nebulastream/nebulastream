#include <Nodes/Operators/LogicalOperators/Sinks/CsvSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSink.hpp>
#include <Sinks/DataSink.hpp>
#include <Sinks/FileOutputSink.hpp>
#include <Sinks/KafkaSink.hpp>
#include <Sinks/ZmqSink.hpp>
#include <Util/Logger.hpp>

namespace NES {

SinkDescriptorPtr ConvertPhysicalToLogicalSink::createSinkDescriptor(DataSinkPtr dataSink) {

    SinkType sinkType = dataSink->getType();

    switch (sinkType) {

        case PRINT_SINK: {
            NES_INFO("ConvertPhysicalToLogicalSink: Creating print sink");
            return PrintSinkDescriptor::create();
        }
        case ZMQ_SINK: {
            NES_INFO("ConvertPhysicalToLogicalSink: Creating ZMQ sink");
            ZmqSinkPtr zmqSink = std::dynamic_pointer_cast<ZmqSink>(dataSink);
            return ZmqSinkDescriptor::create(zmqSink->getHost(), zmqSink->getPort());
        }
#ifdef ENABLE_KAFKA_BUILD
        case KAFKA_SINK: {
            NES_INFO("ConvertPhysicalToLogicalSink: Creating Kafka sink");
            KafkaSinkPtr kafkaSink = std::dynamic_pointer_cast<KafkaSink>(dataSink);
            return KafkaSinkDescriptor::create(kafkaSink->getTopic(),
                                               kafkaSink->getBrokers(),
                                               kafkaSink->getKafkaProducerTimeout());
        }
#endif
        case FILE_SINK: {

            FileOutputSinkPtr fileOutputSink = std::dynamic_pointer_cast<FileOutputSink>(dataSink);
            if (fileOutputSink->getOutputType() == FileOutputSink::BINARY_TYPE) {
                NES_INFO("ConvertPhysicalToLogicalSink: Creating File sink");
                return FileSinkDescriptor::create(fileOutputSink->getFilePath());
            } else {
                NES_INFO("ConvertPhysicalToLogicalSink: Creating CSV sink");
                auto outputMode = fileOutputSink->getOutputMode() == FileOutputSink::FILE_APPEND
                    ? CsvSinkDescriptor::APPEND
                    : CsvSinkDescriptor::OVERWRITE;
                return CsvSinkDescriptor::create(fileOutputSink->getFilePath(), outputMode);
            }
        }
        default: {
            NES_ERROR("ConvertPhysicalToLogicalSink: Unknown Data Sink Type");
            throw std::invalid_argument("Unknown DataSink Type");
        }
    }
}

}// namespace NES
