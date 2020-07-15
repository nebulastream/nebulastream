#include <Nodes/Operators/LogicalOperators/Sinks/CsvSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSink.hpp>
#include <Sinks/DataSink.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/KafkaSink.hpp>
#include <Sinks/ZmqSink.hpp>
#include <Util/Logger.hpp>

namespace NES {

SinkDescriptorPtr ConvertPhysicalToLogicalSink::createSinkDescriptor(DataSinkPtr dataSink) {

    SinkMedium sinkType = dataSink->getType();

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
            FileSinkPtr fileSink = std::dynamic_pointer_cast<FileSink>(dataSink);
            NES_INFO("ConvertPhysicalToLogicalSink: Creating File sink with outputMode " << fileSink->getFileOutputModeAsString()
                                                                                         << " format " << fileSink->getFormatAsString());
            return FileSinkDescriptor::create(fileSink->getFilePath(), fileSink->getOutputMode(), fileSink->getSinkFormat());
        }
        default: {
            NES_ERROR("ConvertPhysicalToLogicalSink: Unknown Data Sink Type");
            throw std::invalid_argument("Unknown DataSink Type");
        }
    }
}

}// namespace NES
