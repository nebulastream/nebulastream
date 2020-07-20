#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSink.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Util/Logger.hpp>

namespace NES {

SinkDescriptorPtr ConvertPhysicalToLogicalSink::createSinkDescriptor(DataSinkPtr dataSink) {

    std::string sinkType = dataSink->toString();

    if (sinkType == "PRINT_SINK") {
        NES_INFO("ConvertPhysicalToLogicalSink: Creating print sink");
        return PrintSinkDescriptor::create();
    } else if (sinkType == "ZMQ_SINK") {
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
        else if (sinkType == "ZMQ_SINK") {
            FileSinkPtr fileSink = std::dynamic_pointer_cast<FileSink>(dataSink);
            NES_INFO("ConvertPhysicalToLogicalSink: Creating File sink with outputMode " << fileSink->getAppendAsBool()
                                                                                         << " format " << fileSink->getSinkFormat());
            return FileSinkDescriptor::create(fileSink->getFilePath(), fileSink->getSinkFormat(), fileSink->getAppendAsString());
        }
        else {
            NES_ERROR("ConvertPhysicalToLogicalSink: Unknown Data Sink Type");
            throw std::invalid_argument("Unknown SinkMedium Type");
        }
}// namespace NES
}// namespace NES
