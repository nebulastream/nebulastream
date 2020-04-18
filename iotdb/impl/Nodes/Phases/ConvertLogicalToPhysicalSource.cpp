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

DataSourcePtr ConvertLogicalToPhysicalSource::getDataSource(SourceDescriptorPtr sourceDescriptor) {

    SourceDescriptorType sourceDescriptorType = sourceDescriptor->getType();

    switch (sourceDescriptorType) {

        case ZmqSource: {
            const ZmqSourceDescriptorPtr
                zmqSourceDescriptor = std::dynamic_pointer_cast<ZmqSourceDescriptor>(sourceDescriptor);
            return createZmqSource(zmqSourceDescriptor->getSchema(),
                                   zmqSourceDescriptor->getHost(),
                                   zmqSourceDescriptor->getPort());
        }
        case DefaultSource: {
            const DefaultSourceDescriptorPtr
                defaultSourceDescriptor = std::dynamic_pointer_cast<DefaultSourceDescriptor>(sourceDescriptor);
            return createDefaultDataSourceWithSchemaForVarBuffers(defaultSourceDescriptor->getSchema(),
                                                                  defaultSourceDescriptor->getNumbersOfBufferToProduce(),
                                                                  defaultSourceDescriptor->getFrequency());
        }
        case BinarySource: {
            const BinarySourceDescriptorPtr
                binarySourceDescriptor = std::dynamic_pointer_cast<BinarySourceDescriptor>(sourceDescriptor);
            return createBinaryFileSource(binarySourceDescriptor->getSchema(), binarySourceDescriptor->getFilePath());
        }
        case CsvSource: {
            const CsvSourceDescriptorPtr
                csvSourceDescriptor = std::dynamic_pointer_cast<CsvSourceDescriptor>(sourceDescriptor);
            return createCSVFileSource(csvSourceDescriptor->getSchema(),
                                       csvSourceDescriptor->getFilePath(),
                                       csvSourceDescriptor->getDelimiter(),
                                       csvSourceDescriptor->getNumBuffersToProcess(),
                                       csvSourceDescriptor->getFrequency());
        }
        case KafkaSource: {
            const KafkaSourceDescriptorPtr
                kafkaSourceDescriptor = std::dynamic_pointer_cast<KafkaSourceDescriptor>(sourceDescriptor);
            return createKafkaSource(kafkaSourceDescriptor->getSchema(),
                                     kafkaSourceDescriptor->getBrokers(),
                                     kafkaSourceDescriptor->getTopic(),
                                     kafkaSourceDescriptor->isAutoCommit(),
                                     kafkaSourceDescriptor->getConfig(),
                                     kafkaSourceDescriptor->getKafkaConnectTimeout());
        }
        case SenseSource: {
            const SenseSourceDescriptorPtr
                senseSourceDescriptor = std::dynamic_pointer_cast<SenseSourceDescriptor>(sourceDescriptor);
            return createSenseSource(senseSourceDescriptor->getSchema(), senseSourceDescriptor->getUdfs());
        }
        default: {
            NES_ERROR("Unknown Source Descriptor Type")
            throw std::invalid_argument("Unknown Source Descriptor Type");
        }
    }
}

}
