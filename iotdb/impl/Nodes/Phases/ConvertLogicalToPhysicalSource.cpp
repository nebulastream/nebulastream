#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSource.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSourcePtr ConvertLogicalToPhysicalSource::createDataSource(SourceDescriptorPtr sourceDescriptor) {

    SourceDescriptorType sourceDescriptorType = sourceDescriptor->getType();

    switch (sourceDescriptorType) {

        case ZmqSource: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating ZMQ source")
            const ZmqSourceDescriptorPtr zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
            return createZmqSource(zmqSourceDescriptor->getSchema(),
                                   zmqSourceDescriptor->getHost(),
                                   zmqSourceDescriptor->getPort());
        }
        case DefaultSource: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating Default source")
            const DefaultSourceDescriptorPtr defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
            return createDefaultDataSourceWithSchemaForVarBuffers(defaultSourceDescriptor->getSchema(),
                                                                  defaultSourceDescriptor->getNumbersOfBufferToProduce(),
                                                                  defaultSourceDescriptor->getFrequency());
        }
        case BinarySource: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating Binary File source")
            const BinarySourceDescriptorPtr binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
            return createBinaryFileSource(binarySourceDescriptor->getSchema(), binarySourceDescriptor->getFilePath());
        }
        case CsvSource: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating CSV file source")
            const CsvSourceDescriptorPtr csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
            return createCSVFileSource(csvSourceDescriptor->getSchema(),
                                       csvSourceDescriptor->getFilePath(),
                                       csvSourceDescriptor->getDelimiter(),
                                       csvSourceDescriptor->getNumBuffersToProcess(),
                                       csvSourceDescriptor->getFrequency());
        }
        case KafkaSource: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating Kafka source")
            const KafkaSourceDescriptorPtr kafkaSourceDescriptor = sourceDescriptor->as<KafkaSourceDescriptor>();
            return createKafkaSource(kafkaSourceDescriptor->getSchema(),
                                     kafkaSourceDescriptor->getBrokers(),
                                     kafkaSourceDescriptor->getTopic(),
                                     kafkaSourceDescriptor->isAutoCommit(),
                                     kafkaSourceDescriptor->getConfig(),
                                     kafkaSourceDescriptor->getKafkaConnectTimeout());
        }
        case SenseSource: {
            NES_INFO("ConvertLogicalToPhysicalSource: Creating sense source")
            const SenseSourceDescriptorPtr senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
            return createSenseSource(senseSourceDescriptor->getSchema(), senseSourceDescriptor->getUdfs());
        }
        default: {
            NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type")
            throw std::invalid_argument("Unknown Source Descriptor Type");
        }
    }
}

}
