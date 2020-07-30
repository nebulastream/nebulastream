#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSinkPtr ConvertLogicalToPhysicalSink::createDataSink(SchemaPtr schema, BufferManagerPtr bufferManager, SinkDescriptorPtr sinkDescriptor) {
    //TODO: this needs to be changed as in ConvertLogicalToPhysicalSource
    Network::NetworkManagerPtr networkManager;

    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        NES_DEBUG("ConvertLogicalToPhysicalSink: Creating print sink" << schema->toString());
        const PrintSinkDescriptorPtr printSinkDescriptor = sinkDescriptor->as<PrintSinkDescriptor>();
        return createTextPrintSink(schema, bufferManager, std::cout);
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating ZMQ sink");
        const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        return createBinaryZmqSink(schema, bufferManager, zmqSinkDescriptor->getHost(), zmqSinkDescriptor->getPort(), true);
#ifdef ENABLE_KAFKA_BUILD
    } else if (sinkDescriptor->instanceOf<KafkaSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Kafka sink");
        const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();
        return createKafkaSinkWithSchema(schema, kafkaSinkDescriptor->getBrokers(),
                                         kafkaSinkDescriptor->getTopic(),
                                         kafkaSinkDescriptor->getTimeout());
#endif
    } else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Binary file sink for format=" << fileSinkDescriptor->getSinkFormatAsString());
        if (fileSinkDescriptor->getSinkFormatAsString() == "CSV_FORMAT") {
            return createCSVFileSink(schema, bufferManager, fileSinkDescriptor->getFileName(), fileSinkDescriptor->getAppend());
        } else if (fileSinkDescriptor->getSinkFormatAsString() == "NES_FORMAT") {
            return createBinaryNESFileSink(schema, bufferManager, fileSinkDescriptor->getFileName(), fileSinkDescriptor->getAppend());
        } else if (fileSinkDescriptor->getSinkFormatAsString() == "TEXT_FORMAT") {
            return createTextFileSink(schema, bufferManager, fileSinkDescriptor->getFileName(), fileSinkDescriptor->getAppend());
        } else {
            NES_ERROR("createDataSink: unsupported format");
            throw std::invalid_argument("Unknown File format");
        }
    } else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating network sink");
        auto networkSinkDescriptor = sinkDescriptor->as<Network::NetworkSinkDescriptor>();
        return createNetworkSink(schema, networkManager, networkSinkDescriptor->getNodeLocation(),
                                 networkSinkDescriptor->getNesPartition(), bufferManager, networkSinkDescriptor->getWaitTime(),
                                 networkSinkDescriptor->getRetryTimes());
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSink: Unknown Sink Descriptor Type");
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

}// namespace NES
