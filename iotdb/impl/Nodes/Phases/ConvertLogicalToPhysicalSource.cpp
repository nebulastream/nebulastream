#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSource.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSourcePtr ConvertLogicalToPhysicalSource::createDataSource(SourceDescriptorPtr sourceDescriptor) {

    SourceDescriptorType sourceDescriptorType = sourceDescriptor->getType();
    //TODO this has be be fixed
    BufferManagerPtr bufferManager;
    QueryManagerPtr queryManager;
    switch (sourceDescriptorType) {

        case ZmqSrc: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating ZMQ source");
            const ZmqSourceDescriptorPtr zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
            return createZmqSource(zmqSourceDescriptor->getSchema(), bufferManager, queryManager,
                                   zmqSourceDescriptor->getHost(), zmqSourceDescriptor->getPort());
        }
        case DefaultSrc: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating Default source");
            const DefaultSourceDescriptorPtr defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
            return createDefaultDataSourceWithSchemaForVarBuffers(
                defaultSourceDescriptor->getSchema(), bufferManager, queryManager,
                defaultSourceDescriptor->getNumbersOfBufferToProduce(), defaultSourceDescriptor->getFrequency());
        }
        case BinarySrc: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating Binary File source");
            const BinarySourceDescriptorPtr binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
            return createBinaryFileSource(binarySourceDescriptor->getSchema(), bufferManager, queryManager,
                                          binarySourceDescriptor->getFilePath());
        }
        case CsvSrc: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating CSV file source");
            const CsvSourceDescriptorPtr csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
            return createCSVFileSource(csvSourceDescriptor->getSchema(), bufferManager, queryManager,
                                       csvSourceDescriptor->getFilePath(), csvSourceDescriptor->getDelimiter(),
                                       csvSourceDescriptor->getNumBuffersToProcess(), csvSourceDescriptor->getFrequency());
        }
        case KafkaSrc: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating Kafka source");
            const KafkaSourceDescriptorPtr kafkaSourceDescriptor = sourceDescriptor->as<KafkaSourceDescriptor>();
            return createKafkaSource(kafkaSourceDescriptor->getSchema(), bufferManager, queryManager,
                                     kafkaSourceDescriptor->getBrokers(), kafkaSourceDescriptor->getTopic(),
                                     kafkaSourceDescriptor->getGroupId(), kafkaSourceDescriptor->isAutoCommit(),
                                     kafkaSourceDescriptor->getKafkaConnectTimeout());
        }
        case SenseSrc: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating sense source");
            const SenseSourceDescriptorPtr senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
            return createSenseSource(senseSourceDescriptor->getSchema(), bufferManager, queryManager,
                                     senseSourceDescriptor->getUdfs());
        }
        default: {
            NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type");
            throw std::invalid_argument("Unknown Source Descriptor Type");
        }
    }
}

}// namespace NES
